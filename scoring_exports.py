"""Helpers for exporting environment and factor score reports to XLSX."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from data import DataLoader
from env import EnvironmentDetector, MacroAlignmentSummary
from factor import FactorEngine
from strategy_config import StrategyParameters, load_strategy_parameters


ENV_SUMMARY_INDICATOR_LABELS = {
    "csi300": "沪深300指数",
    "csi300_amount": "沪深300成交额",
    "bond_index": "债券指数",
    "treasury_10y": "10年期国债收益率",
    "credit_spread": "信用利差",
    "cb_equal_weight": "可转债等权指数",
}

FACTOR_EXCLUDE_REASON_LABELS = {
    "missing_required_fields": "核心因子字段不足",
    "recently_listed": "上市观察期不足",
    "remain_size_below_min": "余额低于阈值",
    "amount_below_min": "成交额低于阈值",
    "call_announced": "已公告强赎",
    "put_triggered": "已触发回售",
    "not_tradable": "当日不可交易",
    "missing_daily_history": "缺少日线历史",
}


@dataclass(frozen=True)
class EnvironmentScoreReport:
    scores: pd.DataFrame
    summary: MacroAlignmentSummary
    requested_start: pd.Timestamp
    requested_end: pd.Timestamp
    history_start_requested: pd.Timestamp
    history_start_used: pd.Timestamp
    fetch_policy: str = "缓存优先，完整性优先"
    refresh_requested: bool = False
    data_quality_status: str = "正常"
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class FactorScoreReport:
    scores: pd.DataFrame
    diagnostics: pd.DataFrame
    requested_start: pd.Timestamp
    requested_end: pd.Timestamp
    history_start_requested: pd.Timestamp
    history_start_used: pd.Timestamp
    codes: list[str]
    fetch_policy: str = "缓存优先，完整性优先"
    refresh_requested: bool = False
    data_quality_status: str = "正常"
    notes: tuple[str, ...] = ()


def build_environment_score_report(
    start_date: object,
    end_date: object,
    loader: DataLoader | None = None,
    detector: EnvironmentDetector | None = None,
    refresh: bool = False,
    config: StrategyParameters | None = None,
    config_path: str | Path | None = None,
) -> EnvironmentScoreReport:
    """Build a trading-day environment score report for one date window."""

    config = config or load_strategy_parameters(config_path)
    loader = loader or DataLoader(config=config)
    detector = detector or EnvironmentDetector(params=config.env)

    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    if start_ts > end_ts:
        raise ValueError("start_date must be <= end_date")

    history_start = start_ts - pd.Timedelta(
        days=config.exports.env_history_buffer_calendar_days
    )
    if not refresh:
        local_history_start = _local_env_history_start(loader, config)
        if local_history_start is not None and local_history_start <= start_ts:
            history_start = max(history_start, local_history_start)
    trading_calendar = loader.get_trading_calendar(
        history_start,
        end_ts,
        exchange=config.data.calendar_exchange,
        refresh=refresh,
    )
    macro_daily = loader.get_macro_daily(
        list(detector.required_indicators),
        history_start,
        end_ts,
    )
    scores, summary = detector.compute_aligned(macro_daily, trading_calendar)
    window_scores = scores.loc[
        scores["trade_date"].between(start_ts, end_ts)
    ].copy()
    if window_scores.empty:
        raise ValueError("Requested window has no environment scores to export.")

    history_start_used = _max_timestamp(
        _safe_min_timestamp(trading_calendar, "calendar_date"),
        _safe_min_timestamp(macro_daily, "trade_date"),
    )
    expected_trade_days = trading_calendar.loc[
        (trading_calendar["is_open"].astype("Int64") == 1)
        & trading_calendar["calendar_date"].between(start_ts, end_ts),
        "calendar_date",
    ]
    notes = _build_history_notes(
        history_start_requested=history_start,
        history_start_used=history_start_used,
        context="环境打分",
    ) + _build_window_coverage_notes(
        expected_dates=expected_trade_days,
        actual_dates=window_scores["trade_date"],
        context="环境打分",
    )
    data_quality_status = "警告" if notes else "正常"
    if data_quality_status == "警告":
        notes = notes + (_data_quality_warning_note("环境打分"),)
    return EnvironmentScoreReport(
        scores=window_scores.reset_index(drop=True),
        summary=summary,
        requested_start=start_ts,
        requested_end=end_ts,
        history_start_requested=history_start,
        history_start_used=history_start_used,
        refresh_requested=bool(refresh),
        data_quality_status=data_quality_status,
        notes=notes,
    )


def build_factor_score_report(
    start_date: object,
    end_date: object,
    codes: str | Iterable[str],
    loader: DataLoader | None = None,
    engine: FactorEngine | None = None,
    refresh: bool = False,
    config: StrategyParameters | None = None,
    config_path: str | Path | None = None,
) -> FactorScoreReport:
    """Build a daily factor score report for one or more convertible bonds."""

    config = config or load_strategy_parameters(config_path)
    loader = loader or DataLoader(config=config)
    engine = engine or FactorEngine(params=config.factor)

    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    if start_ts > end_ts:
        raise ValueError("start_date must be <= end_date")

    normalized_codes = normalize_cb_codes(codes)
    if not normalized_codes:
        raise ValueError("At least one convertible bond code is required.")
    if len(normalized_codes) > config.exports.factor_max_codes_per_run:
        raise ValueError(
            f"Too many codes for one run: {len(normalized_codes)} > "
            f"{config.exports.factor_max_codes_per_run}"
        )

    history_start = start_ts - pd.Timedelta(
        days=_recommended_factor_history_buffer_calendar_days(config)
    )
    if not refresh:
        local_history_start = _local_factor_history_start(loader, normalized_codes)
        if local_history_start is not None and local_history_start <= start_ts:
            history_start = max(history_start, local_history_start)
    cb_daily = loader.get_cb_daily_cross_section(
        history_start,
        end_ts,
        refresh=refresh,
    )
    cb_basic = loader.get_cb_basic()
    cb_rate = None
    if not cb_daily.empty and cb_daily["ytm"].isna().any() and hasattr(loader, "get_cb_rate"):
        universe_codes = (
            cb_daily.loc[cb_daily["trade_date"].between(start_ts, end_ts), "cb_code"]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .tolist()
        )
        if universe_codes:
            cb_rate = loader.get_cb_rate(universe_codes, refresh=refresh)
    cb_call = loader.get_cb_call(
        history_start,
        end_ts,
        refresh=refresh,
    )
    trading_calendar = loader.get_trading_calendar(
        start_ts,
        end_ts,
        exchange=config.data.calendar_exchange,
        refresh=refresh,
    )
    trade_days = (
        trading_calendar.loc[trading_calendar["is_open"].astype("Int64") == 1, "calendar_date"]
        .dropna()
        .sort_values()
        .drop_duplicates()
        .tolist()
    )
    if not trade_days:
        raise ValueError("Requested window has no open trading days.")

    diagnostics = engine.compute_panel_with_diagnostics(
        trade_days=trade_days,
        cb_daily=cb_daily,
        cb_basic=cb_basic,
        cb_call=cb_call,
        cb_rate=cb_rate,
        requested_codes=normalized_codes,
    )
    diagnostics = engine.append_weighted_total_score(
        diagnostics,
        factor_weights=config.model.base_weights,
    )
    scores = diagnostics.loc[
        :,
        [
            "trade_date",
            "cb_code",
            "baseline_total_score",
            "value_score",
            "carry_score",
            "structure_score",
            "trend_score",
            "stability_score",
            "eligible",
            "exclude_reason",
            "close",
            "premium_rate",
            "ytm",
            "remain_size",
            "amount_mean_20",
        ],
    ].copy()
    history_start_used = _safe_min_timestamp(cb_daily, "trade_date")
    history_notes = _build_history_notes(
        history_start_requested=history_start,
        history_start_used=history_start_used,
        context="因子打分",
    )
    diagnostic_notes = _build_factor_diagnostic_notes(diagnostics, normalized_codes)
    data_quality_status = (
        "警告"
        if history_notes or _has_factor_data_completeness_issue(diagnostics)
        else "正常"
    )
    notes = history_notes + diagnostic_notes
    if data_quality_status == "警告":
        notes = notes + (_data_quality_warning_note("因子打分"),)
    return FactorScoreReport(
        scores=scores.reset_index(drop=True),
        diagnostics=diagnostics.reset_index(drop=True),
        requested_start=start_ts,
        requested_end=end_ts,
        history_start_requested=history_start,
        history_start_used=history_start_used,
        codes=normalized_codes,
        refresh_requested=bool(refresh),
        data_quality_status=data_quality_status,
        notes=notes,
    )


def write_environment_score_xlsx(
    report: EnvironmentScoreReport,
    output_path: str | Path | None = None,
    config: StrategyParameters | None = None,
    config_path: str | Path | None = None,
) -> Path:
    """Write one environment score report to XLSX and return the path."""

    config = config or load_strategy_parameters(config_path)
    path = Path(output_path) if output_path is not None else build_output_path(
        prefix=config.exports.env_filename_prefix,
        requested_start=report.requested_start,
        requested_end=report.requested_end,
        config=config,
    )
    path.parent.mkdir(parents=True, exist_ok=True)

    summary_rows = [
        _build_environment_summary_row("report_type", "environment_scores"),
        _build_environment_summary_row("config_path", str(config.path)),
        _build_environment_summary_row("fetch_policy", report.fetch_policy),
        _build_environment_summary_row(
            "refresh_requested",
            "是" if report.refresh_requested else "否",
        ),
        _build_environment_summary_row("data_quality_status", report.data_quality_status),
        _build_environment_summary_row(
            "requested_start",
            report.requested_start.strftime("%Y-%m-%d"),
        ),
        _build_environment_summary_row(
            "requested_end",
            report.requested_end.strftime("%Y-%m-%d"),
        ),
        _build_environment_summary_row(
            "history_start_requested",
            report.history_start_requested.strftime("%Y-%m-%d"),
        ),
        _build_environment_summary_row(
            "history_start_used",
            report.history_start_used.strftime("%Y-%m-%d"),
        ),
        _build_environment_summary_row(
            "total_calendar_days",
            report.summary.total_calendar_days,
        ),
        _build_environment_summary_row("kept_days", report.summary.kept_days),
        _build_environment_summary_row("dropped_days", report.summary.dropped_days),
    ]
    summary_rows.extend(
        _build_environment_summary_row(f"filled_days::{indicator}", value)
        for indicator, value in report.summary.filled_days_by_indicator.items()
    )
    summary_rows.extend(
        _build_environment_summary_row(f"invalid_days::{indicator}", value)
        for indicator, value in report.summary.invalid_days_by_indicator.items()
    )
    summary_rows.extend(
        _build_environment_summary_row(f"note::{index + 1}", note)
        for index, note in enumerate(report.notes)
    )
    summary = pd.DataFrame(summary_rows)

    with pd.ExcelWriter(path, engine=config.exports.excel_engine) as writer:
        report.scores.to_excel(
            writer,
            sheet_name=config.exports.env_sheet_name,
            index=False,
        )
        summary.to_excel(
            writer,
            sheet_name=config.exports.summary_sheet_name,
            index=False,
        )
    return path


def write_factor_score_xlsx(
    report: FactorScoreReport,
    output_path: str | Path | None = None,
    config: StrategyParameters | None = None,
    config_path: str | Path | None = None,
) -> Path:
    """Write one factor score report to XLSX and return the path."""

    config = config or load_strategy_parameters(config_path)
    path = Path(output_path) if output_path is not None else build_output_path(
        prefix=config.exports.factor_filename_prefix,
        requested_start=report.requested_start,
        requested_end=report.requested_end,
        config=config,
    )
    path.parent.mkdir(parents=True, exist_ok=True)

    summary_rows = [
        _build_factor_summary_row("report_type", "factor_scores"),
        _build_factor_summary_row("config_path", str(config.path)),
        _build_factor_summary_row("fetch_policy", report.fetch_policy),
        _build_factor_summary_row(
            "refresh_requested",
            "是" if report.refresh_requested else "否",
        ),
        _build_factor_summary_row("data_quality_status", report.data_quality_status),
        _build_factor_summary_row(
            "requested_start",
            report.requested_start.strftime("%Y-%m-%d"),
        ),
        _build_factor_summary_row(
            "requested_end",
            report.requested_end.strftime("%Y-%m-%d"),
        ),
        _build_factor_summary_row(
            "history_start_requested",
            report.history_start_requested.strftime("%Y-%m-%d"),
        ),
        _build_factor_summary_row(
            "history_start_used",
            report.history_start_used.strftime("%Y-%m-%d"),
        ),
        _build_factor_summary_row("codes", ",".join(report.codes)),
        _build_factor_summary_row("code_count", len(report.codes)),
        _build_factor_summary_row(
            "factor_max_codes_per_run",
            config.exports.factor_max_codes_per_run,
        ),
    ]
    summary_rows.extend(
        _build_factor_summary_row(f"note::{index + 1}", note)
        for index, note in enumerate(report.notes)
    )
    summary = pd.DataFrame(summary_rows)
    sheet_name_map = _build_factor_code_sheet_name_map(
        codes=report.codes,
        reserved_names=(
            config.exports.diagnostics_sheet_name,
            config.exports.summary_sheet_name,
        ),
    )

    with pd.ExcelWriter(path, engine=config.exports.excel_engine) as writer:
        for code in report.codes:
            code_scores = report.scores.loc[report.scores["cb_code"] == code].copy()
            if code_scores.empty:
                code_scores = pd.DataFrame(columns=report.scores.columns)
            code_scores.to_excel(
                writer,
                sheet_name=sheet_name_map[code],
                index=False,
            )
        report.diagnostics.to_excel(
            writer,
            sheet_name=config.exports.diagnostics_sheet_name,
            index=False,
        )
        summary.to_excel(
            writer,
            sheet_name=config.exports.summary_sheet_name,
            index=False,
        )
    return path


def build_output_path(
    prefix: str,
    requested_start: pd.Timestamp,
    requested_end: pd.Timestamp,
    config: StrategyParameters,
    now: datetime | None = None,
) -> Path:
    """Build a default XLSX output path under the configured export directory."""

    project_root = config.path.parent
    output_dir = project_root / config.exports.output_dir
    date_token_format = config.exports.date_token_format
    timestamp_format = config.exports.timestamp_format
    timestamp = (now or datetime.now()).strftime(timestamp_format)
    filename = (
        f"{prefix}_"
        f"{requested_start.strftime(date_token_format)}_"
        f"{requested_end.strftime(date_token_format)}_"
        f"{timestamp}.xlsx"
    )
    return output_dir / filename


def normalize_cb_codes(codes: str | Iterable[str]) -> list[str]:
    """Normalize one or more convertible-bond codes into a de-duplicated list."""

    if isinstance(codes, str):
        raw_codes = re.split(r"[\s,，;；]+", codes.strip())
    else:
        raw_codes = []
        for item in codes:
            raw_codes.extend(re.split(r"[\s,，;；]+", str(item).strip()))
    normalized = []
    seen: set[str] = set()
    for raw in raw_codes:
        code = _normalize_single_cb_code(raw)
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


def _normalize_single_cb_code(raw_code: object) -> str:
    code = str(raw_code).strip().upper()
    if not code:
        return ""
    if re.fullmatch(r"\d{6}", code):
        # A-share convertible bonds usually use 11xxxx on SSE and 12xxxx on SZSE.
        if code.startswith("11"):
            return f"{code}.SH"
        if code.startswith("12"):
            return f"{code}.SZ"
    return code


def _build_factor_code_sheet_name_map(
    codes: list[str],
    reserved_names: tuple[str, ...] = (),
) -> dict[str, str]:
    taken = {name.casefold() for name in reserved_names}
    mapping: dict[str, str] = {}
    for code in codes:
        mapping[code] = _make_unique_excel_sheet_name(
            preferred_name=code,
            taken_names=taken,
        )
        taken.add(mapping[code].casefold())
    return mapping


def _make_unique_excel_sheet_name(
    preferred_name: str,
    taken_names: set[str],
) -> str:
    sanitized = re.sub(r"[:\\/?*\[\]]", "_", str(preferred_name).strip())
    sanitized = sanitized.strip("'")
    if not sanitized:
        sanitized = "code"
    sanitized = sanitized[:31]

    if sanitized.casefold() not in taken_names:
        return sanitized

    counter = 2
    while True:
        suffix = f"_{counter}"
        candidate = f"{sanitized[: max(0, 31 - len(suffix))]}{suffix}"
        if candidate.casefold() not in taken_names:
            return candidate
        counter += 1


def _safe_min_timestamp(frame: pd.DataFrame, column: str) -> pd.Timestamp:
    if frame.empty or column not in frame.columns:
        return pd.Timestamp("1970-01-01")
    values = pd.to_datetime(frame[column], errors="coerce").dropna()
    if values.empty:
        return pd.Timestamp("1970-01-01")
    return pd.Timestamp(values.min()).normalize()


def _max_timestamp(*values: pd.Timestamp) -> pd.Timestamp:
    cleaned = [pd.Timestamp(value).normalize() for value in values if value is not None]
    if not cleaned:
        return pd.Timestamp("1970-01-01")
    return max(cleaned)


def _recommended_factor_history_buffer_calendar_days(
    config: StrategyParameters,
) -> int:
    required_trading_days = max(
        int(config.factor.momentum_window) + 1,
        int(config.factor.volatility_window) + 1,
        int(config.factor.amount_mean_window) + 1,
        int(config.factor.min_listing_days) + 1,
    )
    derived_calendar_days = max(
        60,
        int(math.ceil(required_trading_days * 2.0)),
    )
    return min(
        int(config.exports.factor_history_buffer_calendar_days),
        derived_calendar_days,
    )


def _build_history_notes(
    history_start_requested: pd.Timestamp,
    history_start_used: pd.Timestamp,
    context: str,
) -> tuple[str, ...]:
    if history_start_used <= history_start_requested:
        return ()
    return (
        f"{context}预热窗口未完全覆盖，当前报告基于本地可用缓存或当前可得历史数据生成。",
        f"预热起点请求为 {history_start_requested.strftime('%Y-%m-%d')}，实际使用起点为 {history_start_used.strftime('%Y-%m-%d')}。",
    )


def _build_window_coverage_notes(
    expected_dates: pd.Series | list[object],
    actual_dates: pd.Series,
    context: str,
) -> tuple[str, ...]:
    expected = pd.to_datetime(expected_dates, errors="coerce")
    expected = pd.Series(expected).dropna().sort_values()
    actual = pd.to_datetime(actual_dates, errors="coerce").dropna().sort_values()
    if expected.empty or actual.empty:
        return ()

    notes: list[str] = []
    expected_start = pd.Timestamp(expected.iloc[0]).normalize()
    expected_end = pd.Timestamp(expected.iloc[-1]).normalize()
    actual_start = pd.Timestamp(actual.iloc[0]).normalize()
    actual_end = pd.Timestamp(actual.iloc[-1]).normalize()

    if actual_start > expected_start:
        notes.append(
            f"{context}请求窗口内首个应输出交易日为 {expected_start.strftime('%Y-%m-%d')}，但当前报告首个可导出交易日为 {actual_start.strftime('%Y-%m-%d')}。"
        )
    if actual_end < expected_end:
        notes.append(
            f"{context}请求窗口内最后一个应输出交易日为 {expected_end.strftime('%Y-%m-%d')}，但当前报告最后一个可导出交易日为 {actual_end.strftime('%Y-%m-%d')}。"
        )
    if notes:
        notes.append("这通常表示某个必需指标在部分日期缺少本地缓存，或刷新补数未成功。")
    return tuple(notes)


def _build_environment_summary_row(item: str, value: object) -> dict[str, object]:
    return {
        "item": item,
        "value": value,
        "comment": _environment_summary_comment(item),
    }


def _environment_summary_comment(item: str) -> str:
    fixed_comments = {
        "report_type": "报表类型标识，用于区分当前 XLSX 是环境打分导出结果。",
        "config_path": "本次运行实际使用的策略参数文件绝对路径。",
        "fetch_policy": "当前取数策略规则：优先复用缓存，但一旦发现数据覆盖不足，就优先补齐数据完整性。",
        "refresh_requested": "本次是否启用了强制刷新模式；启用后会尽量重拉本次所需远端数据。",
        "data_quality_status": "本次结果的数据质量状态；若为“警告”，表示当前计算结果可能受数据不完整影响。",
        "requested_start": "用户本次输入的开始日期。",
        "requested_end": "用户本次输入的结束日期。",
        "history_start_requested": "按预热窗口规则向前回溯后，请求参与计算的预热起点。",
        "history_start_used": "本次运行实际可用并参与环境计算的历史起点。",
        "total_calendar_days": "参与本次环境对齐与计算的交易日历总天数，包含预热区间。",
        "kept_days": "完成环境对齐后，所有必需指标都有效并被保留的交易日数量。",
        "dropped_days": "完成环境对齐后，因必需指标缺失或超过填充上限而被剔除的交易日数量。",
    }
    if item in fixed_comments:
        return fixed_comments[item]

    if item.startswith("filled_days::"):
        indicator = item.split("::", 1)[1]
        label = ENV_SUMMARY_INDICATOR_LABELS.get(indicator, indicator)
        return f"{label}在环境对齐时通过前值填充补齐的交易日数量。"

    if item.startswith("invalid_days::"):
        indicator = item.split("::", 1)[1]
        label = ENV_SUMMARY_INDICATOR_LABELS.get(indicator, indicator)
        return f"{label}在环境对齐时因缺失或超过填充上限而无效的交易日数量。"

    if item.startswith("note::"):
        return "本次导出附带的补充提示、边界说明或异常说明。"

    return "本次环境打分导出中的运行摘要项。"


def _build_factor_summary_row(item: str, value: object) -> dict[str, object]:
    return {
        "item": item,
        "value": value,
        "comment": _factor_summary_comment(item),
    }


def _factor_summary_comment(item: str) -> str:
    fixed_comments = {
        "report_type": "报表类型标识，用于区分当前 XLSX 是因子打分导出结果。",
        "config_path": "本次运行实际使用的策略参数文件绝对路径。",
        "fetch_policy": "当前取数策略规则：优先复用缓存，但一旦发现数据覆盖不足，就优先补齐数据完整性。",
        "refresh_requested": "本次是否启用了强制刷新模式；启用后会尽量重拉本次所需远端数据。",
        "data_quality_status": "本次结果的数据质量状态；若为“警告”，表示当前计算结果可能受数据不完整影响。",
        "requested_start": "用户本次输入的开始日期。",
        "requested_end": "用户本次输入的结束日期。",
        "history_start_requested": "按预热窗口规则向前回溯后，请求参与计算的预热起点。",
        "history_start_used": "本次运行实际取得并参与因子计算的历史起点。",
        "codes": "本次请求导出的可转债代码列表，多个代码用逗号分隔。",
        "code_count": "本次导出的可转债代码数量。",
        "factor_max_codes_per_run": "当前配置允许单次因子打分导出的最大代码数量。",
    }
    if item in fixed_comments:
        return fixed_comments[item]

    if item.startswith("note::"):
        return "本次导出附带的补充提示、边界说明或异常说明。"

    return "本次因子打分导出中的运行摘要项。"


def _build_factor_diagnostic_notes(
    diagnostics: pd.DataFrame,
    codes: list[str],
) -> tuple[str, ...]:
    if diagnostics.empty or "cb_code" not in diagnostics.columns:
        return ()

    notes: list[str] = []
    for code in codes:
        code_frame = diagnostics.loc[diagnostics["cb_code"] == code].copy()
        if code_frame.empty:
            notes.append(f"{code} 在请求窗口内没有生成任何因子诊断记录。")
            continue

        eligible_count = int(code_frame["eligible"].fillna(False).astype(bool).sum())
        if eligible_count > 0:
            continue

        reasons = _summarize_factor_exclude_reasons(code_frame)
        if reasons:
            notes.append(f"{code} 在请求窗口内未产生可用因子分数，主要原因：{reasons}。")
        else:
            notes.append(f"{code} 在请求窗口内未产生可用因子分数。")
    return tuple(notes)


def _summarize_factor_exclude_reasons(frame: pd.DataFrame) -> str:
    if "exclude_reason" not in frame.columns:
        return ""

    counts: dict[str, int] = {}
    for raw in frame["exclude_reason"].fillna("").astype(str):
        for reason in [part.strip() for part in raw.split(",") if part.strip()]:
            counts[reason] = counts.get(reason, 0) + 1
    if not counts:
        return ""

    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    parts = [
        f"{FACTOR_EXCLUDE_REASON_LABELS.get(reason, reason)}({count}天)"
        for reason, count in ranked[:3]
    ]
    return "、".join(parts)


def _has_factor_data_completeness_issue(diagnostics: pd.DataFrame) -> bool:
    if diagnostics.empty or "exclude_reason" not in diagnostics.columns:
        return False

    data_issue_reasons = {"missing_required_fields", "missing_daily_history"}
    for raw in diagnostics["exclude_reason"].fillna("").astype(str):
        parts = {part.strip() for part in raw.split(",") if part.strip()}
        if parts & data_issue_reasons:
            return True
    return False


def _data_quality_warning_note(context: str) -> str:
    return (
        f"{context}当前存在数据完整性风险，计算结果可能偏离真实可投资信号，"
        "请勿直接据此做投资判断。"
    )


def _local_env_history_start(
    loader: DataLoader,
    config: StrategyParameters,
) -> pd.Timestamp | None:
    if not hasattr(loader, "cache_store") or not hasattr(loader, "source_name"):
        return None
    frames = [
        loader.cache_store.load_calendar(loader.source_name, config.data.calendar_exchange),
        loader.cache_store.load_time_series(loader.source_name, "index_daily", "000300.SH"),
        loader.cache_store.load_time_series(loader.source_name, "index_daily", "H11001.CSI"),
        loader.cache_store.load_time_series(
            loader.source_name,
            "yield_curve",
            f"{config.data.treasury_curve_code}__{config.data.treasury_curve_type}__{config.data.treasury_curve_term:g}",
        ),
        loader.cache_store.load_time_series(loader.source_name, "cb_equal_weight", "ALL"),
        loader.cache_store.load_reference_frame("macro", "credit_spread"),
    ]
    if any(frame is None or frame.empty for frame in frames):
        return None
    candidates = [
        _safe_min_timestamp(frames[0], "calendar_date"),
        _safe_min_timestamp(frames[1], "trade_date"),
        _safe_min_timestamp(frames[2], "trade_date"),
        _safe_min_timestamp(frames[3], "trade_date"),
        _safe_min_timestamp(frames[4], "trade_date"),
        _safe_min_timestamp(frames[5], "trade_date"),
    ]
    return max(candidates)


def _local_factor_history_start(
    loader: DataLoader,
    codes: list[str],
) -> pd.Timestamp | None:
    if not hasattr(loader, "cache_store") or not hasattr(loader, "source_name"):
        return None
    cross_section_dir = (
        loader.cache_store.base_dir
        / loader.source_name
        / "time_series"
        / "cb_daily_cross_section"
    )
    if cross_section_dir.exists():
        trade_days = [
            pd.Timestamp(path.stem).normalize()
            for path in cross_section_dir.glob("*.csv")
            if path.stem.isdigit() and len(path.stem) == 8
        ]
        if trade_days:
            return min(trade_days)
    if not codes:
        return None
    frames = [
        loader.cache_store.load_time_series(loader.source_name, "cb_daily", code)
        for code in codes
    ]
    available_frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not available_frames:
        return None
    starts = [_safe_min_timestamp(frame, "trade_date") for frame in available_frames]
    return min(starts)
