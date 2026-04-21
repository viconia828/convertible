from __future__ import annotations

import unittest

import pandas as pd

from history_windows import (
    build_history_notes,
    recommended_factor_history_buffer_calendar_days,
    resolve_environment_report_history_start,
    resolve_environment_warmup_history_start,
    resolve_factor_report_history_start,
    resolve_strategy_snapshot_history_start,
)
from strategy_config import load_strategy_parameters


class FakeCacheService:
    def __init__(
        self,
        env_history_start: pd.Timestamp | None = None,
        factor_history_start: pd.Timestamp | None = None,
    ) -> None:
        self.env_history_start = env_history_start
        self.factor_history_start = factor_history_start

    def inspect_local_env_history_start(self, **kwargs) -> pd.Timestamp | None:
        return self.env_history_start

    def inspect_local_factor_history_start(self, codes) -> pd.Timestamp | None:
        return self.factor_history_start


class FakeLoader:
    def __init__(
        self,
        cache_service: FakeCacheService | None = None,
        trading_calendar: pd.DataFrame | None = None,
    ) -> None:
        self.cache_service = cache_service
        self._trading_calendar = (
            trading_calendar.copy()
            if trading_calendar is not None
            else pd.DataFrame(columns=["calendar_date", "is_open"])
        )

    def get_trading_calendar(
        self,
        start_date,
        end_date,
        exchange=None,
        refresh: bool = False,
    ) -> pd.DataFrame:
        start_ts = pd.Timestamp(start_date).normalize()
        end_ts = pd.Timestamp(end_date).normalize()
        return self._trading_calendar.loc[
            self._trading_calendar["calendar_date"].between(start_ts, end_ts)
        ].reset_index(drop=True)


class HistoryWindowsTests(unittest.TestCase):
    def test_recommended_factor_history_buffer_calendar_days_respects_dynamic_window_and_cap(
        self,
    ) -> None:
        default_config = load_strategy_parameters()
        self.assertLess(
            recommended_factor_history_buffer_calendar_days(default_config),
            default_config.exports.factor_history_buffer_calendar_days,
        )

        capped_config = load_strategy_parameters(
            overrides={"factor": {"min_listing_days": 800}}
        )
        self.assertEqual(
            recommended_factor_history_buffer_calendar_days(capped_config),
            capped_config.exports.factor_history_buffer_calendar_days,
        )

    def test_resolve_environment_report_history_start_compacts_to_local_cache_start(self) -> None:
        config = load_strategy_parameters()
        loader = FakeLoader(
            cache_service=FakeCacheService(
                env_history_start=pd.Timestamp("2026-01-06"),
            )
        )

        history_start = resolve_environment_report_history_start(
            loader=loader,
            requested_start="2026-04-20",
            config=config,
            refresh=False,
        )

        self.assertEqual(history_start, pd.Timestamp("2026-01-06"))

    def test_resolve_factor_report_history_start_compacts_to_local_cache_start(self) -> None:
        config = load_strategy_parameters()
        loader = FakeLoader(
            cache_service=FakeCacheService(
                factor_history_start=pd.Timestamp("2026-03-02"),
            )
        )

        history_start = resolve_factor_report_history_start(
            loader=loader,
            requested_start="2026-04-20",
            config=config,
            codes=["110001.SH"],
            refresh=False,
        )

        self.assertEqual(history_start, pd.Timestamp("2026-03-02"))

    def test_resolve_environment_warmup_history_start_uses_prior_trade_days(self) -> None:
        config = load_strategy_parameters()
        dates = pd.date_range("2026-03-16", "2026-04-21", freq="B")
        trading_calendar = pd.DataFrame(
            {
                "calendar_date": dates,
                "is_open": 1,
            }
        )
        loader = FakeLoader(trading_calendar=trading_calendar)
        requested_start = pd.Timestamp("2026-04-21")
        expected = pd.Timestamp(dates[dates < requested_start][-5]).normalize()

        history_start = resolve_environment_warmup_history_start(
            loader=loader,
            requested_start=requested_start,
            config=config,
            warmup_observation_count=5,
            refresh=False,
        )

        self.assertEqual(history_start, expected)

    def test_resolve_strategy_snapshot_history_start_uses_widest_buffer(self) -> None:
        config = load_strategy_parameters(
            overrides={
                "strategy": {"history_buffer_calendar_days": 60},
                "exports": {
                    "env_history_buffer_calendar_days": 120,
                    "factor_history_buffer_calendar_days": 90,
                },
                "factor": {"min_listing_days": 100},
            }
        )

        history_start = resolve_strategy_snapshot_history_start(
            trade_date="2026-04-20",
            config=config,
        )

        self.assertEqual(history_start, pd.Timestamp("2025-12-21"))

    def test_build_history_notes_only_when_used_start_later_than_requested(self) -> None:
        self.assertEqual(
            build_history_notes(
                history_start_requested=pd.Timestamp("2026-01-01"),
                history_start_used=pd.Timestamp("2025-12-01"),
                context="因子打分",
            ),
            (),
        )

        notes = build_history_notes(
            history_start_requested=pd.Timestamp("2026-01-01"),
            history_start_used=pd.Timestamp("2026-01-10"),
            context="因子打分",
        )
        self.assertEqual(len(notes), 2)
        self.assertIn("因子打分预热窗口未完全覆盖", notes[0])
        self.assertIn("2026-01-10", notes[1])


if __name__ == "__main__":
    unittest.main()
