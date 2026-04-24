"""Snapshot objects consumed by the strategy module."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass(frozen=True)
class StrategyHistoryWindow:
    """Resolved history window used to build one strategy snapshot."""

    trade_date: pd.Timestamp
    requested_start: pd.Timestamp
    used_start: pd.Timestamp


@dataclass(frozen=True)
class StrategySnapshot:
    """One-date snapshot with all inputs required by the strategy engine."""

    trade_date: pd.Timestamp
    history_window: StrategyHistoryWindow
    trading_calendar: pd.DataFrame
    macro_daily: pd.DataFrame
    cb_daily: pd.DataFrame
    cb_basic: pd.DataFrame
    cb_call: pd.DataFrame
    cb_rate: pd.DataFrame | None = None
    refresh_requested: bool = False
    requested_codes: tuple[str, ...] = ()
    data_quality_hints: tuple[str, ...] = ()
    runtime_snapshot_reused: bool = False
    cache_diagnostics: dict[str, object] = field(default_factory=dict)
