"""Structured results produced by the strategy module."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from env.macro_alignment import MacroAlignmentSummary
from shared.reporting_semantics import DATA_QUALITY_STATUS_OK, DEFAULT_FETCH_POLICY


@dataclass(frozen=True)
class StrategyDiagnostics:
    """Compact diagnostics attached to one strategy decision."""

    history_start_requested: pd.Timestamp
    history_start_used: pd.Timestamp
    fetch_policy: str = DEFAULT_FETCH_POLICY
    refresh_requested: bool = False
    runtime_snapshot_reused: bool = False
    requested_codes: tuple[str, ...] = ()
    data_quality_status: str = DATA_QUALITY_STATUS_OK
    data_quality_hints: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    alignment_summary: MacroAlignmentSummary | None = None
    first_fully_ready_trade_date: pd.Timestamp | None = None
    cache_diagnostics: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyDecision:
    """One-date strategy output."""

    trade_date: pd.Timestamp
    environment: dict[str, float]
    factor_weights: dict[str, float]
    factor_diagnostics: pd.DataFrame
    total_scores: pd.DataFrame
    selected_portfolio: pd.DataFrame
    eligible_count: int
    cash_weight: float
    diagnostics: StrategyDiagnostics
