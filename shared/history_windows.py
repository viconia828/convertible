"""Shared helpers for resolving history windows across exports and strategy."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import TYPE_CHECKING, Sequence

import pandas as pd

from config.strategy_config import StrategyParameters
from env.environment_detector import EnvironmentDetector

if TYPE_CHECKING:
    from data.data_loader import DataLoader


@dataclass(frozen=True)
class EnvironmentExportWindowResolution:
    """Resolved environment export window semantics for one requested date range."""

    warmup_first_ready_date: pd.Timestamp
    trend_first_ready_date: pd.Timestamp | None
    effective_start: pd.Timestamp
    warmup_trade_days_excluded: int = 0
    notes: tuple[str, ...] = ()


def recommended_factor_history_buffer_calendar_days(
    config: StrategyParameters,
) -> int:
    """Resolve the dynamic factor history window capped by the export config."""

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


def recommended_environment_history_buffer_calendar_days(
    config: StrategyParameters,
) -> int:
    """Resolve the dynamic environment history window capped by the export config."""

    required_observations = EnvironmentDetector(
        params=config.env
    ).recommended_history_buffer_observation_count()
    derived_calendar_days = max(
        60,
        int(math.ceil(required_observations * 2.0)),
    )
    return min(
        int(config.exports.env_history_buffer_calendar_days),
        derived_calendar_days,
    )


def recommended_strategy_snapshot_history_buffer_calendar_days(
    config: StrategyParameters,
) -> int:
    """Resolve the strategy snapshot buffer from dynamic needs plus optional widen."""

    required_history_buffer_days = max(
        recommended_environment_history_buffer_calendar_days(config),
        recommended_factor_history_buffer_calendar_days(config),
    )
    configured_buffer_days = max(0, int(config.strategy.history_buffer_calendar_days))
    if configured_buffer_days <= 0:
        return required_history_buffer_days
    return max(required_history_buffer_days, configured_buffer_days)


def inspect_local_env_history_start(
    loader: DataLoader,
    config: StrategyParameters,
) -> pd.Timestamp | None:
    """Inspect the earliest locally ready history start for environment inputs."""

    cache_service = getattr(loader, "cache_service", None)
    if cache_service is None:
        return None
    return cache_service.inspect_local_env_history_start(
        calendar_exchange=config.data.calendar_exchange,
        treasury_curve_code=config.data.treasury_curve_code,
        treasury_curve_type=config.data.treasury_curve_type,
        treasury_curve_term=config.data.treasury_curve_term,
    )


def inspect_local_factor_history_start(
    loader: DataLoader,
    codes: Sequence[str],
) -> pd.Timestamp | None:
    """Inspect the earliest locally ready history start for factor inputs."""

    cache_service = getattr(loader, "cache_service", None)
    if cache_service is None:
        return None
    return cache_service.inspect_local_factor_history_start(list(codes))


def resolve_environment_report_history_start(
    loader: DataLoader,
    requested_start: object,
    config: StrategyParameters,
    refresh: bool,
) -> pd.Timestamp:
    """Resolve the environment report history start after local-cache compaction."""

    requested_ts = pd.Timestamp(requested_start).normalize()
    history_start = requested_ts - pd.Timedelta(
        days=recommended_environment_history_buffer_calendar_days(config)
    )
    if refresh:
        return history_start.normalize()

    local_history_start = inspect_local_env_history_start(loader, config)
    if local_history_start is not None and local_history_start <= requested_ts:
        history_start = max(history_start, pd.Timestamp(local_history_start).normalize())
    return history_start.normalize()


def resolve_factor_report_history_start(
    loader: DataLoader,
    requested_start: object,
    config: StrategyParameters,
    codes: Sequence[str],
    refresh: bool,
) -> pd.Timestamp:
    """Resolve the factor report history start after local-cache compaction."""

    requested_ts = pd.Timestamp(requested_start).normalize()
    history_start = requested_ts - pd.Timedelta(
        days=recommended_factor_history_buffer_calendar_days(config)
    )
    if refresh:
        return history_start.normalize()

    local_history_start = inspect_local_factor_history_start(loader, codes)
    if local_history_start is not None and local_history_start <= requested_ts:
        history_start = max(history_start, pd.Timestamp(local_history_start).normalize())
    return history_start.normalize()


def resolve_strategy_snapshot_history_start(
    trade_date: object,
    config: StrategyParameters,
) -> pd.Timestamp:
    """Resolve the widest shared history window needed by one strategy snapshot."""

    trade_ts = pd.Timestamp(trade_date).normalize()
    history_buffer_days = recommended_strategy_snapshot_history_buffer_calendar_days(
        config
    )
    return (trade_ts - pd.Timedelta(days=history_buffer_days)).normalize()


def resolve_environment_warmup_history_start(
    loader: DataLoader,
    requested_start: object,
    config: StrategyParameters,
    warmup_observation_count: int,
    refresh: bool,
) -> pd.Timestamp:
    """Resolve the earliest prior trade day needed to satisfy environment warm-up."""

    requested_ts = pd.Timestamp(requested_start).normalize()
    required_prior_trade_days = max(0, int(warmup_observation_count))
    if required_prior_trade_days <= 0:
        return requested_ts

    probe_calendar_days = max(90, required_prior_trade_days * 6)
    probe_start = requested_ts - pd.Timedelta(days=probe_calendar_days)
    probe_calendar = loader.get_trading_calendar(
        probe_start,
        requested_ts,
        exchange=config.data.calendar_exchange,
        refresh=refresh,
    )
    calendar_dates = pd.to_datetime(
        probe_calendar.get("calendar_date"),
        errors="coerce",
    )
    open_mask = (
        probe_calendar.get("is_open", pd.Series(dtype="Int64")).astype("Int64").eq(1)
        & calendar_dates.lt(requested_ts)
    )
    open_days = (
        pd.Series(calendar_dates.loc[open_mask])
        .dropna()
        .sort_values()
        .reset_index(drop=True)
    )
    if len(open_days) >= required_prior_trade_days:
        return pd.Timestamp(open_days.iloc[-required_prior_trade_days]).normalize()
    if not open_days.empty:
        return pd.Timestamp(open_days.iloc[0]).normalize()
    return requested_ts


def resolve_environment_export_window(
    score_dates: pd.Series | list[object],
    readiness: pd.DataFrame,
    requested_trade_days: pd.Series | list[object],
    requested_start: object,
    requested_end: object,
    warmup_observation_count: int,
    trend_ready_column: str = "trend_ready",
) -> EnvironmentExportWindowResolution:
    """Resolve shared environment export-ready window semantics."""

    requested_start_ts = pd.Timestamp(requested_start).normalize()
    requested_end_ts = pd.Timestamp(requested_end).normalize()
    warmup_first_ready_date = resolve_environment_export_first_ready_date(
        score_dates=score_dates,
        requested_start=requested_start_ts,
        requested_end=requested_end_ts,
        warmup_observation_count=warmup_observation_count,
    )
    trend_first_ready_date = first_ready_trade_date(readiness, trend_ready_column)
    effective_start = max(requested_start_ts, warmup_first_ready_date)
    warmup_trade_days_excluded = count_trade_days_in_range(
        requested_trade_days,
        requested_start_ts,
        effective_start,
    )
    notes = build_environment_warmup_notes(
        requested_start=requested_start_ts,
        requested_end=requested_end_ts,
        first_ready_trade_date=warmup_first_ready_date,
        warmup_trade_days_excluded=warmup_trade_days_excluded,
    )
    return EnvironmentExportWindowResolution(
        warmup_first_ready_date=warmup_first_ready_date,
        trend_first_ready_date=trend_first_ready_date,
        effective_start=effective_start,
        warmup_trade_days_excluded=warmup_trade_days_excluded,
        notes=notes,
    )


def resolve_environment_export_first_ready_date(
    score_dates: pd.Series | list[object],
    requested_start: object,
    requested_end: object,
    warmup_observation_count: int,
) -> pd.Timestamp:
    """Resolve the first export-ready trade date under environment warm-up rules."""

    requested_start_ts = pd.Timestamp(requested_start).normalize()
    requested_end_ts = pd.Timestamp(requested_end).normalize()
    observation_dates = pd.to_datetime(score_dates, errors="coerce")
    observation_dates = (
        pd.Series(observation_dates)
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    if observation_dates.empty:
        raise ValueError("Requested window has no environment scores to export.")

    requested_dates = observation_dates.loc[
        observation_dates.between(requested_start_ts, requested_end_ts)
    ].reset_index(drop=True)
    if requested_dates.empty:
        raise ValueError("Requested window has no environment scores to export.")

    required_prior_observations = max(0, int(warmup_observation_count))
    available_prior_observations = int((observation_dates < requested_start_ts).sum())
    shortage = max(0, required_prior_observations - available_prior_observations)
    if shortage <= 0:
        return pd.Timestamp(requested_dates.iloc[0]).normalize()

    requested_onward = observation_dates.loc[
        observation_dates.ge(requested_start_ts)
    ].reset_index(drop=True)
    first_ready_candidate = (
        pd.Timestamp(requested_onward.iloc[shortage]).normalize()
        if len(requested_onward) > shortage
        else None
    )
    if first_ready_candidate is None or first_ready_candidate > requested_end_ts:
        first_ready_label = (
            first_ready_candidate.strftime("%Y-%m-%d")
            if first_ready_candidate is not None
            else "unknown"
        )
        raise ValueError(
            "Requested window remains entirely inside the environment warm-up interval; "
            f"first export-ready trade date is {first_ready_label}."
        )
    return first_ready_candidate


def first_ready_trade_date(
    readiness: pd.DataFrame,
    column: str,
) -> pd.Timestamp | None:
    """Resolve the first trade date where one readiness column is true."""

    if readiness.empty or column not in readiness.columns:
        return None
    ready_dates = pd.to_datetime(
        readiness.loc[readiness[column].fillna(False).astype(bool), "trade_date"],
        errors="coerce",
    ).dropna()
    if ready_dates.empty:
        return None
    return pd.Timestamp(ready_dates.iloc[0]).normalize()


def build_environment_warmup_notes(
    requested_start: object,
    requested_end: object,
    first_ready_trade_date: pd.Timestamp | None,
    warmup_trade_days_excluded: int,
) -> tuple[str, ...]:
    """Build notes describing how environment warm-up affected export output."""

    requested_start_ts = pd.Timestamp(requested_start).normalize()
    requested_end_ts = pd.Timestamp(requested_end).normalize()
    if first_ready_trade_date is None:
        return ()
    first_ready = pd.Timestamp(first_ready_trade_date).normalize()
    if first_ready <= requested_start_ts or warmup_trade_days_excluded <= 0:
        return ()
    warmup_end = min(requested_end_ts, first_ready - pd.Timedelta(days=1))
    return (
        "环境打分已自动识别预热区间，预热期默认值不会纳入正式导出结果。",
        (
            f"已跳过 {warmup_trade_days_excluded} 个请求窗口内交易日，"
            f"预热区间截至 {warmup_end.strftime('%Y-%m-%d')}，"
            f"首个正式环境得分交易日为 {first_ready.strftime('%Y-%m-%d')}。"
        ),
    )


def count_trade_days_in_range(
    trade_days: pd.Series | list[object],
    start_ts: object,
    end_exclusive_ts: object,
) -> int:
    """Count trade days in [start_ts, end_exclusive_ts)."""

    start_ts = pd.Timestamp(start_ts).normalize()
    end_exclusive_ts = pd.Timestamp(end_exclusive_ts).normalize()
    if start_ts >= end_exclusive_ts:
        return 0
    days = pd.to_datetime(trade_days, errors="coerce")
    days = pd.Series(days).dropna()
    if days.empty:
        return 0
    return int(days.between(start_ts, end_exclusive_ts - pd.Timedelta(days=1)).sum())


def safe_min_timestamp(frame: pd.DataFrame, column: str) -> pd.Timestamp | None:
    """Return the normalized minimum timestamp in one column, if present."""

    if frame.empty or column not in frame.columns:
        return None
    values = pd.to_datetime(frame[column], errors="coerce").dropna()
    if values.empty:
        return None
    return pd.Timestamp(values.min()).normalize()


def max_available_history_start(*timestamps: pd.Timestamp | None) -> pd.Timestamp:
    """Return the latest available timestamp among the provided history starts."""

    present = [pd.Timestamp(value).normalize() for value in timestamps if value is not None]
    if not present:
        raise ValueError("Unable to resolve history start from empty inputs.")
    return max(present)


def build_history_notes(
    history_start_requested: pd.Timestamp,
    history_start_used: pd.Timestamp,
    context: str,
    trade_days: pd.Series | list[object] | None = None,
) -> tuple[str, ...]:
    """Build a consistent note when the effective history starts later than requested."""

    requested_ts = pd.Timestamp(history_start_requested).normalize()
    used_ts = pd.Timestamp(history_start_used).normalize()
    if used_ts <= requested_ts:
        return ()
    if trade_days is not None:
        skipped_trade_days = count_trade_days_in_range(
            trade_days=trade_days,
            start_ts=requested_ts,
            end_exclusive_ts=used_ts,
        )
        if skipped_trade_days <= 0:
            return ()
    return (
        f"{context}预热窗口未完全覆盖，当前结果基于本地可用缓存或当前可得历史数据生成。",
        f"预热起点请求为 {requested_ts.strftime('%Y-%m-%d')}，实际使用起点为 {used_ts.strftime('%Y-%m-%d')}。",
    )
