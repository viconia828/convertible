"""Snapshot orchestration for the strategy module."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import replace
import re
from pathlib import Path
from typing import Sequence

import pandas as pd

from config.strategy_config import StrategyParameters, load_strategy_parameters
from data.data_loader import DataLoader
from env import EnvironmentDetector
from factor import FactorEngine
from shared.cache_diagnostics import build_cache_diagnostics, diff_cache_stats
from shared.history_windows import (
    build_history_notes,
    max_available_history_start,
    resolve_strategy_snapshot_history_start,
    safe_min_timestamp,
)

from .engine import StrategyEngine
from .snapshot import StrategyHistoryWindow, StrategySnapshot


class StrategyService:
    """Coordinate data loading and snapshot construction for one strategy date."""

    DEFAULT_SNAPSHOT_MEMORY_ITEMS = 4

    def __init__(
        self,
        loader: DataLoader | None = None,
        engine: StrategyEngine | None = None,
        detector: EnvironmentDetector | None = None,
        factor_engine: FactorEngine | None = None,
        config: StrategyParameters | None = None,
        config_path: str | Path | None = None,
        snapshot_memory_items: int = DEFAULT_SNAPSHOT_MEMORY_ITEMS,
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
        self._snapshot_memory_items = max(int(snapshot_memory_items), 0)
        self._snapshot_memory: OrderedDict[
            tuple[str, str],
            tuple[object, StrategySnapshot],
        ] = (
            OrderedDict()
        )

    def build_snapshot(
        self,
        trade_date: object,
        requested_codes: Sequence[str] | str | None = None,
        refresh: bool = False,
    ) -> StrategySnapshot:
        """Load all inputs needed to run the strategy on one open trade date."""

        trade_ts = pd.Timestamp(trade_date).normalize()
        normalized_codes = normalize_requested_codes(requested_codes)
        cache_stats_before = self._cache_stats_snapshot()
        requested_history_start = resolve_strategy_snapshot_history_start(
            trade_date=trade_ts,
            config=self.config,
        )
        memory_key = self._snapshot_memory_key(
            trade_date=trade_ts,
            requested_history_start=requested_history_start,
        )
        dependency_revision = self._runtime_dependency_revision()
        if not refresh:
            cached_snapshot = self._load_runtime_snapshot(
                memory_key,
                dependency_revision=dependency_revision,
            )
            if cached_snapshot is not None:
                return replace(
                    cached_snapshot,
                    requested_codes=normalized_codes,
                    runtime_snapshot_reused=True,
                    cache_diagnostics=build_cache_diagnostics(
                        runtime_snapshot_reused=True
                    ),
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
        snapshot_codes = (
            snapshot_daily["cb_code"].dropna().astype(str).drop_duplicates().tolist()
        )
        cb_call_history_start = self._resolve_cb_call_history_start(
            requested_history_start=requested_history_start,
            trade_date=trade_ts,
            snapshot_codes=snapshot_codes,
            cb_basic=cb_basic,
        )
        cb_call = self.loader.get_cb_call(
            cb_call_history_start,
            trade_ts,
            codes=snapshot_codes,
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
            trade_days=open_days,
        )
        cacheable_snapshot = StrategySnapshot(
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
            refresh_requested=False,
            requested_codes=(),
            data_quality_hints=history_hints,
            runtime_snapshot_reused=False,
            cache_diagnostics={},
        )
        self._save_runtime_snapshot(
            memory_key,
            dependency_revision=self._runtime_dependency_revision(),
            snapshot=cacheable_snapshot,
        )
        cache_stats_delta = diff_cache_stats(
            cache_stats_before,
            self._cache_stats_snapshot(),
        )
        return replace(
            cacheable_snapshot,
            refresh_requested=bool(refresh),
            requested_codes=normalized_codes,
            cache_diagnostics=build_cache_diagnostics(
                cache_stats_delta,
                runtime_snapshot_reused=False,
            ),
        )

    def run_for_date(
        self,
        trade_date: object,
        requested_codes: Sequence[str] | str | None = None,
        refresh: bool = False,
    ):
        """Build one snapshot and run the strategy engine on top of it."""

        snapshot = self.build_snapshot(
            trade_date=trade_date,
            requested_codes=requested_codes,
            refresh=refresh,
        )
        return self.engine.run(snapshot)

    @staticmethod
    def _snapshot_memory_key(
        trade_date: pd.Timestamp,
        requested_history_start: pd.Timestamp,
    ) -> tuple[str, str]:
        return (
            pd.Timestamp(trade_date).strftime("%Y-%m-%d"),
            pd.Timestamp(requested_history_start).strftime("%Y-%m-%d"),
        )

    def _load_runtime_snapshot(
        self,
        memory_key: tuple[str, str],
        dependency_revision: object,
    ) -> StrategySnapshot | None:
        cached_entry = self._snapshot_memory.get(memory_key)
        if cached_entry is None:
            return None
        cached_revision, cached_snapshot = cached_entry
        if cached_revision != dependency_revision:
            self._snapshot_memory.pop(memory_key, None)
            return None
        self._snapshot_memory.move_to_end(memory_key)
        return cached_snapshot

    def _save_runtime_snapshot(
        self,
        memory_key: tuple[str, str],
        dependency_revision: object,
        snapshot: StrategySnapshot,
    ) -> None:
        if self._snapshot_memory_items <= 0:
            return
        self._snapshot_memory[memory_key] = (dependency_revision, snapshot)
        self._snapshot_memory.move_to_end(memory_key)
        while len(self._snapshot_memory) > self._snapshot_memory_items:
            self._snapshot_memory.popitem(last=False)

    def _runtime_dependency_revision(self) -> object:
        revision = getattr(self.loader, "runtime_dependency_revision", None)
        if callable(revision):
            try:
                return revision()
            except Exception:  # noqa: BLE001
                return ()
        cache_service = getattr(self.loader, "cache_service", None)
        generation = getattr(cache_service, "runtime_content_generation", None)
        if callable(generation):
            try:
                return int(generation())
            except Exception:  # noqa: BLE001
                return 0
        return ()

    @staticmethod
    def _resolve_cb_call_history_start(
        requested_history_start: pd.Timestamp,
        trade_date: pd.Timestamp,
        snapshot_codes: Sequence[str],
        cb_basic: pd.DataFrame,
    ) -> pd.Timestamp:
        history_start = pd.Timestamp(requested_history_start).normalize()
        if not snapshot_codes or cb_basic.empty or "list_date" not in cb_basic.columns:
            return history_start

        list_dates = pd.to_datetime(
            cb_basic.loc[cb_basic["cb_code"].isin(snapshot_codes), "list_date"],
            errors="coerce",
        ).dropna()
        list_dates = list_dates.loc[list_dates.le(pd.Timestamp(trade_date).normalize())]
        if list_dates.empty:
            return history_start

        earliest_list_date = pd.Timestamp(list_dates.min()).normalize()
        return min(history_start, earliest_list_date)

    def _cache_stats_snapshot(self) -> dict[str, int]:
        cache_service = getattr(self.loader, "cache_service", None)
        snapshot = getattr(cache_service, "stats_snapshot", None)
        if not callable(snapshot):
            return {}
        raw = snapshot()
        if not isinstance(raw, dict):
            return {}
        stats: dict[str, int] = {}
        for key, value in raw.items():
            try:
                stats[str(key)] = int(value)
            except Exception:  # noqa: BLE001
                continue
        return stats


def normalize_requested_codes(
    codes: Sequence[str] | str | None,
) -> tuple[str, ...]:
    if not codes:
        return ()
    if isinstance(codes, str):
        raw_items = [codes]
    else:
        raw_items = [str(item) for item in codes]
    normalized: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        for raw_code in re.split(r"[\s,，;；]+", item.strip()):
            code = _normalize_single_requested_code(raw_code)
            if not code or code in seen:
                continue
            seen.add(code)
            normalized.append(code)
    return tuple(normalized)


def _normalize_single_requested_code(raw_code: object) -> str:
    code = str(raw_code).strip().upper()
    if not code:
        return ""
    if re.fullmatch(r"\d{6}", code):
        if code.startswith("11"):
            return f"{code}.SH"
        if code.startswith("12"):
            return f"{code}.SZ"
    return code
