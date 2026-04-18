"""Helpers for exporting environment and factor score reports to XLSX."""

from __future__ import annotations

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


@dataclass(frozen=True)
class EnvironmentScoreReport:
    scores: pd.DataFrame
    summary: MacroAlignmentSummary
    requested_start: pd.Timestamp
    requested_end: pd.Timestamp
    history_start_requested: pd.Timestamp
    history_start_used: pd.Timestamp
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
    notes = _build_history_notes(
        history_start_requested=history_start,
        history_start_used=history_start_used,
        context="环境打分",
    )
    return EnvironmentScoreReport(
        scores=window_scores.reset_index(drop=True),
        summary=summary,
        requested_start=start_ts,
        requested_end=end_ts,
        history_start_requested=history_start,
        history_start_used=history_start_used,
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
        days=config.exports.factor_history_buffer_calendar_days
    )
    if not refresh:
        local_history_start = _local_factor_history_start(loader, normalized_codes)
        if local_history_start is not None and local_history_start <= start_ts:
            history_start = max(history_start, local_history_start)
    cb_daily = loader.get_cb_daily(
        normalized_codes,
        history_start,
        end_ts,
        refresh=refresh,
        enrich=True,
    )
    cb_basic = loader.get_cb_basic()
    cb_call = loader.get_cb_call(
        history_start,
        end_ts,
        codes=normalized_codes,
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

    diagnostics_frames: list[pd.DataFrame] = []
    for trade_day in trade_days:
        daily = engine.compute_with_diagnostics(
            as_of_date=trade_day,
            cb_daily=cb_daily,
            cb_basic=cb_basic,
            cb_call=cb_call,
            requested_codes=normalized_codes,
        )
        diagnostics_frames.append(
            engine.append_weighted_total_score(
                daily,
                factor_weights=config.model.base_weights,
            )
        )

    diagnostics = (
        pd.concat(diagnostics_frames, ignore_index=True)
        if diagnostics_frames
        else engine._empty_result(include_diagnostics=True)  # noqa: SLF001
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
    notes = _build_history_notes(
        history_start_requested=history_start,
        history_start_used=history_start_used,
        context="因子打分",
    )
    return FactorScoreReport(
        scores=scores.reset_index(drop=True),
        diagnostics=diagnostics.reset_index(drop=True),
        requested_start=start_ts,
        requested_end=end_ts,
        history_start_requested=history_start,
        history_start_used=history_start_used,
        codes=normalized_codes,
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
        {"item": "report_type", "value": "environment_scores"},
        {"item": "config_path", "value": str(config.path)},
        {"item": "requested_start", "value": report.requested_start.strftime("%Y-%m-%d")},
        {"item": "requested_end", "value": report.requested_end.strftime("%Y-%m-%d")},
        {
            "item": "history_start_requested",
            "value": report.history_start_requested.strftime("%Y-%m-%d"),
        },
        {
            "item": "history_start_used",
            "value": report.history_start_used.strftime("%Y-%m-%d"),
        },
        {"item": "total_calendar_days", "value": report.summary.total_calendar_days},
        {"item": "kept_days", "value": report.summary.kept_days},
        {"item": "dropped_days", "value": report.summary.dropped_days},
    ]
    summary_rows.extend(
        {
            "item": f"filled_days::{indicator}",
            "value": value,
        }
        for indicator, value in report.summary.filled_days_by_indicator.items()
    )
    summary_rows.extend(
        {
            "item": f"invalid_days::{indicator}",
            "value": value,
        }
        for indicator, value in report.summary.invalid_days_by_indicator.items()
    )
    summary_rows.extend(
        {"item": f"note::{index + 1}", "value": note}
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
        {"item": "report_type", "value": "factor_scores"},
        {"item": "config_path", "value": str(config.path)},
        {"item": "requested_start", "value": report.requested_start.strftime("%Y-%m-%d")},
        {"item": "requested_end", "value": report.requested_end.strftime("%Y-%m-%d")},
        {
            "item": "history_start_requested",
            "value": report.history_start_requested.strftime("%Y-%m-%d"),
        },
        {
            "item": "history_start_used",
            "value": report.history_start_used.strftime("%Y-%m-%d"),
        },
        {"item": "codes", "value": ",".join(report.codes)},
        {"item": "code_count", "value": len(report.codes)},
        {
            "item": "factor_max_codes_per_run",
            "value": config.exports.factor_max_codes_per_run,
        },
    ]
    summary_rows.extend(
        {"item": f"note::{index + 1}", "value": note}
        for index, note in enumerate(report.notes)
    )
    summary = pd.DataFrame(summary_rows)

    with pd.ExcelWriter(path, engine=config.exports.excel_engine) as writer:
        report.scores.to_excel(
            writer,
            sheet_name=config.exports.factor_sheet_name,
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
        code = raw.strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


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
