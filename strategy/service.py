"""Snapshot orchestration for the strategy module."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pandas as pd

from data.data_loader import DataLoader
from env import EnvironmentDetector
from factor import FactorEngine
from history_windows import (
    build_history_notes,
    max_available_history_start,
    resolve_strategy_snapshot_history_start,
    safe_min_timestamp,
)
from strategy_config import StrategyParameters, load_strategy_parameters

from .engine import StrategyEngine
from .snapshot import StrategyHistoryWindow, StrategySnapshot


class StrategyService:
    """Coordinate data loading and snapshot construction for one strategy date."""

    def __init__(
        self,
        loader: DataLoader | None = None,
        engine: StrategyEngine | None = None,
        detector: EnvironmentDetector | None = None,
        factor_engine: FactorEngine | None = None,
        config: StrategyParameters | None = None,
        config_path: str | Path | None = None,
    ) -> None:
        self.config = config or load_strategy_parameters(config_path)
        self.loader = loader or DataLoader(config=self.config)
        self.detector = detector or EnvironmentDetector(params=self.config.env)
        self.factor_engine = factor_engine or FactorEngine(params=self.config.factor)
        self.engine = engine or StrategyEngine(
            detector=self.detector,
            factor_engine=self.factor_engine,
            config=self.config,
        )

    def build_snapshot(
        self,
        trade_date: object,
        requested_codes: Sequence[str] | None = None,
        refresh: bool = False,
    ) -> StrategySnapshot:
        """Load all inputs needed to run the strategy on one open trade date."""

        trade_ts = pd.Timestamp(trade_date).normalize()
        requested_history_start = resolve_strategy_snapshot_history_start(
            trade_date=trade_ts,
            config=self.config,
        )
        ensure_credit_spread_coverage = getattr(
            self.loader,
            "ensure_credit_spread_reference_coverage",
            None,
        )
        if callable(ensure_credit_spread_coverage):
            ensure_credit_spread_coverage(
                requested_history_start,
                trade_ts,
                refresh=refresh,
            )

        trading_calendar = self.loader.get_trading_calendar(
            requested_history_start,
            trade_ts,
            exchange=self.config.data.calendar_exchange,
            refresh=refresh,
        )
        open_days = pd.to_datetime(
            trading_calendar.loc[
                trading_calendar["is_open"].astype("Int64") == 1,
                "calendar_date",
            ],
            errors="coerce",
        ).dropna()
        if not open_days.eq(trade_ts).any():
            raise ValueError(
                f"trade_date {trade_ts.strftime('%Y-%m-%d')} is not an open trading day."
            )

        macro_daily = self.loader.get_macro_daily(
            list(self.detector.required_indicators),
            requested_history_start,
            trade_ts,
        )
        cb_daily = self.loader.get_cb_daily_cross_section(
            requested_history_start,
            trade_ts,
            refresh=refresh,
            columns=list(self.factor_engine.HISTORY_COLUMNS),
            aggregate_profile="factor_history_v1",
        )
        snapshot_daily = cb_daily.loc[cb_daily["trade_date"].eq(trade_ts)].copy()
        if snapshot_daily.empty:
            raise ValueError(
                f"No cb_daily cross-section rows are available on trade_date {trade_ts.strftime('%Y-%m-%d')}."
            )
        cb_basic = self.loader.get_cb_basic()
        cb_call = self.loader.get_cb_call(
            requested_history_start,
            trade_ts,
            refresh=refresh,
        )
        cb_rate = None
        missing_ytm_codes = (
            snapshot_daily.loc[snapshot_daily["ytm"].isna(), "cb_code"]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .tolist()
        )
        if missing_ytm_codes:
            cb_rate = self.loader.get_cb_rate(missing_ytm_codes, refresh=refresh)

        history_start_used = max_available_history_start(
            safe_min_timestamp(trading_calendar, "calendar_date"),
            safe_min_timestamp(macro_daily, "trade_date"),
            safe_min_timestamp(cb_daily, "trade_date"),
        )
        history_hints = build_history_notes(
            history_start_requested=requested_history_start,
            history_start_used=history_start_used,
            context="策略 snapshot",
        )
        normalized_codes = _normalize_requested_codes(requested_codes)
        return StrategySnapshot(
            trade_date=trade_ts,
            history_window=StrategyHistoryWindow(
                trade_date=trade_ts,
                requested_start=requested_history_start,
                used_start=history_start_used,
            ),
            trading_calendar=trading_calendar.reset_index(drop=True),
            macro_daily=macro_daily.reset_index(drop=True),
            cb_daily=cb_daily.reset_index(drop=True),
            cb_basic=cb_basic.reset_index(drop=True),
            cb_call=cb_call.reset_index(drop=True),
            cb_rate=None if cb_rate is None else cb_rate.reset_index(drop=True),
            refresh_requested=bool(refresh),
            requested_codes=normalized_codes,
            data_quality_hints=history_hints,
        )

    def run_for_date(
        self,
        trade_date: object,
        requested_codes: Sequence[str] | None = None,
        refresh: bool = False,
    ):
        """Build one snapshot and run the strategy engine on top of it."""

        snapshot = self.build_snapshot(
            trade_date=trade_date,
            requested_codes=requested_codes,
            refresh=refresh,
        )
        return self.engine.run(snapshot)


def _normalize_requested_codes(
    codes: Sequence[str] | None,
) -> tuple[str, ...]:
    if not codes:
        return ()
    normalized: list[str] = []
    seen: set[str] = set()
    for item in codes:
        code = str(item).strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return tuple(normalized)
