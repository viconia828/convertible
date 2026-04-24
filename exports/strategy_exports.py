"""Helpers for exporting one-date strategy observation reports to XLSX."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from config.strategy_config import StrategyParameters, load_strategy_parameters
from shared.cache_diagnostics import build_cache_diagnostics
from shared.reporting_semantics import yes_no_label
from strategy import StrategyDecision, StrategyService, normalize_requested_codes


STRATEGY_OBSERVATION_FILENAME_PREFIX = "策略观察"
STRATEGY_ENVIRONMENT_SHEET_NAME = "environment"
STRATEGY_FACTOR_WEIGHTS_SHEET_NAME = "factor_weights"
STRATEGY_PORTFOLIO_SHEET_NAME = "portfolio"
STRATEGY_WATCHLIST_SHEET_NAME = "watchlist"
STRATEGY_CANDIDATE_SHEET_NAME = "candidate_scores"
STRATEGY_FACTOR_DIAGNOSTICS_SHEET_NAME = "factor_diagnostics"
STRATEGY_CACHE_DIAGNOSTICS_SHEET_NAME = "cache_diagnostics"


@dataclass(frozen=True)
class StrategyObservationReport:
    trade_date: pd.Timestamp
    requested_codes: list[str]
    environment: pd.DataFrame
    factor_weights: pd.DataFrame
    portfolio: pd.DataFrame
    watchlist: pd.DataFrame
    candidate_scores: pd.DataFrame
    factor_diagnostics: pd.DataFrame
    cache_diagnostics: pd.DataFrame
    history_start_requested: pd.Timestamp
    history_start_used: pd.Timestamp
    first_fully_ready_trade_date: pd.Timestamp | None = None
    fetch_policy: str = ""
    refresh_requested: bool = False
    runtime_snapshot_reused: bool = False
    data_quality_status: str = ""
    eligible_count: int = 0
    selected_count: int = 0
    cash_weight: float = 0.0
    notes: tuple[str, ...] = ()


def build_strategy_observation_report(
    trade_date: object,
    requested_codes: str | Iterable[str] | None = None,
    service: StrategyService | None = None,
    refresh: bool = False,
    config: StrategyParameters | None = None,
    config_path: str | Path | None = None,
) -> StrategyObservationReport:
    """Build one-date strategy observation report data for XLSX export."""

    config = config or load_strategy_parameters(config_path)
    service = service or StrategyService(config=config)
    normalized_codes = list(normalize_requested_codes(requested_codes))
    decision = service.run_for_date(
        trade_date=trade_date,
        requested_codes=normalized_codes,
        refresh=refresh,
    )
    diagnostics = decision.diagnostics
    cache_payload = diagnostics.cache_diagnostics or build_cache_diagnostics(
        runtime_snapshot_reused=diagnostics.runtime_snapshot_reused
    )
    return StrategyObservationReport(
        trade_date=pd.Timestamp(decision.trade_date).normalize(),
        requested_codes=list(diagnostics.requested_codes),
        environment=_build_environment_frame(decision),
        factor_weights=_build_factor_weights_frame(decision),
        portfolio=decision.selected_portfolio.reset_index(drop=True).copy(),
        watchlist=_build_watchlist_frame(decision),
        candidate_scores=decision.total_scores.reset_index(drop=True).copy(),
        factor_diagnostics=decision.factor_diagnostics.reset_index(drop=True).copy(),
        cache_diagnostics=_build_cache_diagnostics_frame(cache_payload),
        history_start_requested=diagnostics.history_start_requested,
        history_start_used=diagnostics.history_start_used,
        first_fully_ready_trade_date=diagnostics.first_fully_ready_trade_date,
        fetch_policy=diagnostics.fetch_policy,
        refresh_requested=bool(diagnostics.refresh_requested),
        runtime_snapshot_reused=bool(diagnostics.runtime_snapshot_reused),
        data_quality_status=str(diagnostics.data_quality_status),
        eligible_count=int(decision.eligible_count),
        selected_count=int(len(decision.selected_portfolio)),
        cash_weight=float(decision.cash_weight),
        notes=_collect_observation_notes(decision),
    )


def write_strategy_observation_xlsx(
    report: StrategyObservationReport,
    output_path: str | Path | None = None,
    config: StrategyParameters | None = None,
    config_path: str | Path | None = None,
) -> Path:
    """Write one-date strategy observation report to XLSX and return the path."""

    config = config or load_strategy_parameters(config_path)
    path = (
        Path(output_path)
        if output_path is not None
        else build_strategy_output_path(
            trade_date=report.trade_date,
            config=config,
        )
    )
    path.parent.mkdir(parents=True, exist_ok=True)

    summary = pd.DataFrame(_build_strategy_summary_rows(report, config))

    with pd.ExcelWriter(path, engine=config.exports.excel_engine) as writer:
        report.environment.to_excel(
            writer,
            sheet_name=STRATEGY_ENVIRONMENT_SHEET_NAME,
            index=False,
        )
        report.factor_weights.to_excel(
            writer,
            sheet_name=STRATEGY_FACTOR_WEIGHTS_SHEET_NAME,
            index=False,
        )
        report.portfolio.to_excel(
            writer,
            sheet_name=STRATEGY_PORTFOLIO_SHEET_NAME,
            index=False,
        )
        report.watchlist.to_excel(
            writer,
            sheet_name=STRATEGY_WATCHLIST_SHEET_NAME,
            index=False,
        )
        report.candidate_scores.to_excel(
            writer,
            sheet_name=STRATEGY_CANDIDATE_SHEET_NAME,
            index=False,
        )
        report.factor_diagnostics.to_excel(
            writer,
            sheet_name=STRATEGY_FACTOR_DIAGNOSTICS_SHEET_NAME,
            index=False,
        )
        report.cache_diagnostics.to_excel(
            writer,
            sheet_name=STRATEGY_CACHE_DIAGNOSTICS_SHEET_NAME,
            index=False,
        )
        summary.to_excel(
            writer,
            sheet_name=config.exports.summary_sheet_name,
            index=False,
        )
    return path


def build_strategy_output_path(
    trade_date: pd.Timestamp,
    config: StrategyParameters,
    now: datetime | None = None,
) -> Path:
    """Build the default XLSX output path for one-date strategy observation."""

    project_root = config.path.parent
    output_dir = project_root / config.exports.output_dir
    date_token = pd.Timestamp(trade_date).strftime(config.exports.date_token_format)
    timestamp = (now or datetime.now()).strftime(config.exports.timestamp_format)
    filename = (
        f"{STRATEGY_OBSERVATION_FILENAME_PREFIX}_"
        f"{date_token}_{date_token}_{timestamp}.xlsx"
    )
    return output_dir / filename


def _build_environment_frame(decision: StrategyDecision) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": pd.Timestamp(decision.trade_date).normalize(),
                **{key: float(value) for key, value in decision.environment.items()},
            }
        ]
    )


def _build_factor_weights_frame(decision: StrategyDecision) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": pd.Timestamp(decision.trade_date).normalize(),
                **{key: float(value) for key, value in decision.factor_weights.items()},
            }
        ]
    )


def _build_watchlist_frame(decision: StrategyDecision) -> pd.DataFrame:
    columns = ["代码", "观察状态", "可建仓", "候选排序", "组合排序", "目标权重", "总分", "剔除原因"]
    requested_codes = tuple(decision.diagnostics.requested_codes)
    if not requested_codes:
        return pd.DataFrame(columns=columns)

    watchlist = pd.DataFrame({"cb_code": list(requested_codes)})
    total_scores = decision.total_scores.copy()
    required_score_columns = ("cb_code", "trade_date", "eligible", "exclude_reason", "total_score")
    if total_scores.empty:
        total_scores = pd.DataFrame(columns=required_score_columns)
    else:
        total_scores = total_scores.loc[
            :,
            [column for column in required_score_columns if column in total_scores.columns],
        ].copy()
    for column in required_score_columns:
        if column not in total_scores.columns:
            total_scores[column] = pd.NA

    selected = decision.selected_portfolio.copy()
    required_selected_columns = ("cb_code", "rank", "target_weight")
    if selected.empty:
        selected = pd.DataFrame(columns=required_selected_columns)
    else:
        selected = selected.loc[
            :,
            [column for column in required_selected_columns if column in selected.columns],
        ].copy()
    for column in required_selected_columns:
        if column not in selected.columns:
            selected[column] = pd.NA
    selected = selected.rename(
        columns={
            "rank": "portfolio_rank",
            "target_weight": "portfolio_weight",
        }
    )

    eligible_scores = total_scores.loc[
        total_scores["eligible"].fillna(False).astype(bool) & total_scores["total_score"].notna(),
        ["cb_code"],
    ].copy()
    eligible_scores["eligible_rank"] = range(1, len(eligible_scores) + 1)

    watchlist = watchlist.merge(total_scores, on="cb_code", how="left")
    watchlist = watchlist.merge(eligible_scores, on="cb_code", how="left")
    watchlist = watchlist.merge(selected, on="cb_code", how="left")
    watchlist["status"] = watchlist.apply(_resolve_watchlist_status, axis=1)
    watchlist["eligible_display"] = watchlist["eligible"].map(_format_nullable_bool)
    watchlist["eligible_rank"] = watchlist["eligible_rank"].map(_format_rank)
    watchlist["portfolio_rank"] = watchlist["portfolio_rank"].map(_format_rank)
    watchlist["portfolio_weight"] = watchlist["portfolio_weight"].map(_format_percent)
    watchlist["total_score"] = watchlist["total_score"].map(_format_score)
    watchlist["exclude_reason"] = watchlist["exclude_reason"].fillna("")
    display = watchlist.rename(
        columns={
            "cb_code": "代码",
            "status": "观察状态",
            "eligible_display": "可建仓",
            "eligible_rank": "候选排序",
            "portfolio_rank": "组合排序",
            "portfolio_weight": "目标权重",
            "total_score": "总分",
            "exclude_reason": "剔除原因",
        }
    )
    return display.loc[:, columns].reset_index(drop=True)


def _build_cache_diagnostics_frame(
    diagnostics: dict[str, object],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    rows.append(
        _build_cache_summary_row(
            "runtime_snapshot_reused",
            yes_no_label(bool(diagnostics.get("runtime_snapshot_reused", False))),
        )
    )

    summary = diagnostics.get("summary")
    if isinstance(summary, dict):
        for key, value in summary.items():
            rows.append(_build_cache_summary_row(f"summary::{key}", value))

    layers = diagnostics.get("layers")
    if isinstance(layers, dict):
        for layer_name, payload in layers.items():
            if not isinstance(payload, dict):
                continue
            for key, value in payload.items():
                rows.append(
                    _build_cache_summary_row(
                        f"layer::{layer_name}::{key}",
                        value,
                    )
                )

    top_stages = diagnostics.get("top_stages")
    if isinstance(top_stages, list):
        for index, payload in enumerate(top_stages, start=1):
            if not isinstance(payload, dict):
                continue
            stage_name = str(payload.get("name", ""))
            elapsed_ms = int(payload.get("elapsed_ms", 0))
            calls = int(payload.get("calls", 0))
            rows.append(
                _build_cache_summary_row(
                    f"top_stage::{index}",
                    f"{stage_name} | {elapsed_ms}ms | {calls}次",
                )
            )

    return pd.DataFrame(rows)


def _build_strategy_summary_rows(
    report: StrategyObservationReport,
    config: StrategyParameters,
) -> list[dict[str, object]]:
    rows = [
        _build_strategy_summary_row("report_type", "strategy_observation"),
        _build_strategy_summary_row("config_path", str(config.path)),
        _build_strategy_summary_row("fetch_policy", report.fetch_policy),
        _build_strategy_summary_row(
            "refresh_requested",
            yes_no_label(report.refresh_requested),
        ),
        _build_strategy_summary_row("data_quality_status", report.data_quality_status),
        _build_strategy_summary_row(
            "trade_date",
            report.trade_date.strftime("%Y-%m-%d"),
        ),
        _build_strategy_summary_row(
            "history_start_requested",
            report.history_start_requested.strftime("%Y-%m-%d"),
        ),
        _build_strategy_summary_row(
            "history_start_used",
            report.history_start_used.strftime("%Y-%m-%d"),
        ),
        _build_strategy_summary_row(
            "first_fully_ready_trade_date",
            (
                report.first_fully_ready_trade_date.strftime("%Y-%m-%d")
                if report.first_fully_ready_trade_date is not None
                else ""
            ),
        ),
        _build_strategy_summary_row("requested_codes", ",".join(report.requested_codes)),
        _build_strategy_summary_row("requested_code_count", len(report.requested_codes)),
        _build_strategy_summary_row("eligible_count", report.eligible_count),
        _build_strategy_summary_row("selected_count", report.selected_count),
        _build_strategy_summary_row("cash_weight", report.cash_weight),
        _build_strategy_summary_row(
            "runtime_snapshot_reused",
            yes_no_label(report.runtime_snapshot_reused),
        ),
        _build_strategy_summary_row(
            "environment_sheet_role",
            "单日环境分数观察 sheet。",
        ),
        _build_strategy_summary_row(
            "factor_weights_sheet_role",
            "单日因子权重观察 sheet。",
        ),
        _build_strategy_summary_row(
            "portfolio_sheet_role",
            "策略当日目标组合观察 sheet。",
        ),
        _build_strategy_summary_row(
            "watchlist_sheet_role",
            "按用户指定代码聚焦查看当日策略状态的观察 sheet。",
        ),
        _build_strategy_summary_row(
            "candidate_scores_sheet_role",
            "全市场候选总分、eligible 与 exclude_reason 观察 sheet。",
        ),
        _build_strategy_summary_row(
            "factor_diagnostics_sheet_role",
            "单日因子 raw 值、分数和诊断列观察 sheet。",
        ),
        _build_strategy_summary_row(
            "cache_diagnostics_sheet_role",
            "单次运行缓存诊断摘要 sheet。",
        ),
        _build_strategy_summary_row(
            "environment_sheet_columns",
            ",".join(report.environment.columns),
        ),
        _build_strategy_summary_row(
            "factor_weights_sheet_columns",
            ",".join(report.factor_weights.columns),
        ),
        _build_strategy_summary_row(
            "portfolio_sheet_columns",
            ",".join(report.portfolio.columns),
        ),
        _build_strategy_summary_row(
            "watchlist_sheet_columns",
            ",".join(report.watchlist.columns),
        ),
        _build_strategy_summary_row(
            "candidate_scores_sheet_columns",
            ",".join(report.candidate_scores.columns),
        ),
        _build_strategy_summary_row(
            "factor_diagnostics_sheet_columns",
            ",".join(report.factor_diagnostics.columns),
        ),
        _build_strategy_summary_row(
            "cache_diagnostics_sheet_columns",
            ",".join(report.cache_diagnostics.columns),
        ),
    ]
    rows.extend(
        _build_strategy_summary_row(f"note::{index + 1}", note)
        for index, note in enumerate(report.notes)
    )
    return rows


def _collect_observation_notes(decision: StrategyDecision) -> tuple[str, ...]:
    raw_notes = list(decision.diagnostics.data_quality_hints) + list(decision.diagnostics.notes)
    return tuple(dict.fromkeys(raw_notes))


def _resolve_watchlist_status(row: pd.Series) -> str:
    if pd.isna(row.get("trade_date")):
        return "当日无横截面记录"
    if pd.notna(row.get("portfolio_rank")):
        return "已入选目标组合"
    eligible = row.get("eligible")
    if pd.notna(eligible) and bool(eligible):
        return "可建仓未入选"
    return "不可交易/已剔除"


def _format_nullable_bool(value: object) -> str:
    if pd.isna(value):
        return "未知"
    return yes_no_label(bool(value))


def _format_rank(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(int(value))


def _format_percent(value: object) -> str:
    if pd.isna(value):
        return ""
    return f"{float(value):.2%}"


def _format_score(value: object) -> str:
    if pd.isna(value):
        return ""
    return f"{float(value):.4f}"


def _build_strategy_summary_row(item: str, value: object) -> dict[str, object]:
    return {
        "item": item,
        "value": value,
        "comment": _strategy_summary_comment(item),
    }


def _strategy_summary_comment(item: str) -> str:
    fixed_comments = {
        "report_type": "报表类型标识，用于区分当前 XLSX 是 strategy 单交易日观察结果。",
        "config_path": "本次运行实际使用的策略参数文件绝对路径。",
        "fetch_policy": "当前取数策略规则：优先复用缓存，但一旦发现数据覆盖不足，就优先补齐数据完整性。",
        "refresh_requested": "本次是否启用了强制刷新模式；启用后会尽量重拉本次所需远端数据。",
        "data_quality_status": "本次结果的数据质量状态；若为“警告”，表示当前计算结果可能受数据不完整影响。",
        "trade_date": "本次 strategy 观察导出的目标交易日。",
        "history_start_requested": "按 snapshot 历史窗口规则向前回溯后，请求参与计算的历史起点。",
        "history_start_used": "本次实际取得并参与单日 strategy 计算的历史起点。",
        "first_fully_ready_trade_date": "环境 fully-ready 首日；若 trade_date 早于该日期，则结果可能仍受环境预热影响。",
        "requested_codes": "本次输入的观察名单代码列表；多个代码用逗号分隔。",
        "requested_code_count": "本次观察名单代码数量。",
        "eligible_count": "当日全市场中可建仓的候选数量。",
        "selected_count": "当日目标组合的持仓数量。",
        "cash_weight": "当日组合剩余现金权重。",
        "runtime_snapshot_reused": "本次运行是否直接命中同实例 runtime snapshot reuse。",
        "environment_sheet_role": "说明 environment sheet 承担单日环境状态观察职责。",
        "factor_weights_sheet_role": "说明 factor_weights sheet 承担单日权重观察职责。",
        "portfolio_sheet_role": "说明 portfolio sheet 承担最终目标组合观察职责。",
        "watchlist_sheet_role": "说明 watchlist sheet 承担用户指定观察名单的聚焦观察职责。",
        "candidate_scores_sheet_role": "说明 candidate_scores sheet 承担全市场候选排序观察职责。",
        "factor_diagnostics_sheet_role": "说明 factor_diagnostics sheet 承担单日因子排查职责。",
        "cache_diagnostics_sheet_role": "说明 cache_diagnostics sheet 承担单次运行缓存行为观察职责。",
        "environment_sheet_columns": "environment sheet 的固定列集合。",
        "factor_weights_sheet_columns": "factor_weights sheet 的固定列集合。",
        "portfolio_sheet_columns": "portfolio sheet 的固定列集合。",
        "watchlist_sheet_columns": "watchlist sheet 的固定列集合。",
        "candidate_scores_sheet_columns": "candidate_scores sheet 的固定列集合。",
        "factor_diagnostics_sheet_columns": "factor_diagnostics sheet 的固定列集合。",
        "cache_diagnostics_sheet_columns": "cache_diagnostics sheet 的固定列集合。",
    }
    if item in fixed_comments:
        return fixed_comments[item]
    if item.startswith("note::"):
        return "本次导出附带的补充提示、边界说明或异常说明。"
    return "本次 strategy 单交易日观察导出中的运行摘要项。"


def _build_cache_summary_row(item: str, value: object) -> dict[str, object]:
    return {
        "item": item,
        "value": value,
        "comment": _cache_summary_comment(item),
    }


def _cache_summary_comment(item: str) -> str:
    if item == "runtime_snapshot_reused":
        return "本次运行是否直接命中同实例 runtime snapshot reuse。"
    if item.startswith("summary::"):
        suffix = item.split("::", 1)[1]
        labels = {
            "cache_hits": "统一缓存层命中次数。",
            "cache_misses": "统一缓存层 miss 次数。",
            "cache_partial_hits": "统一缓存层 partial hit 次数。",
            "cache_refresh_bypass": "统一缓存层 refresh bypass 次数。",
            "file_scans": "本次运行的文件扫描次数。",
            "remote_fills": "本次运行的远端补数次数。",
            "writebacks": "本次运行的缓存回写次数。",
        }
        return labels.get(suffix, "统一缓存层摘要项。")
    if item.startswith("layer::request_panel_memory::"):
        return "request panel runtime memory cache 的命中/失配/保存统计。"
    if item.startswith("layer::aggregate_memory::"):
        return "aggregate runtime memory cache 的命中/失配统计。"
    if item.startswith("layer::aggregate_metadata_memory::"):
        return "aggregate metadata runtime memory cache 的命中/失配统计。"
    if item.startswith("top_stage::"):
        return "本次运行耗时最高的阶段摘要。"
    return "本次 strategy 观察导出中的缓存诊断项。"
