"""Shared helpers for resolving history windows across exports and strategy."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Sequence

import pandas as pd

from strategy_config import StrategyParameters

if TYPE_CHECKING:
    from data.data_loader import DataLoader


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
        days=int(config.exports.env_history_buffer_calendar_days)
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
    history_buffer_days = max(
        int(config.strategy.history_buffer_calendar_days),
        int(config.exports.env_history_buffer_calendar_days),
        recommended_factor_history_buffer_calendar_days(config),
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
) -> tuple[str, ...]:
    """Build a consistent note when the effective history starts later than requested."""

    requested_ts = pd.Timestamp(history_start_requested).normalize()
    used_ts = pd.Timestamp(history_start_used).normalize()
    if used_ts <= requested_ts:
        return ()
    return (
        f"{context}预热窗口未完全覆盖，当前结果基于本地可用缓存或当前可得历史数据生成。",
        f"预热起点请求为 {requested_ts.strftime('%Y-%m-%d')}，实际使用起点为 {used_ts.strftime('%Y-%m-%d')}。",
    )
