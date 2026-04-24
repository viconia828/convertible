from __future__ import annotations

import unittest

import pandas as pd

from config.strategy_config import load_strategy_parameters
from shared.history_windows import (
    build_environment_warmup_notes,
    build_history_notes,
    count_trade_days_in_range,
    first_ready_trade_date,
    recommended_environment_history_buffer_calendar_days,
    recommended_factor_history_buffer_calendar_days,
    recommended_strategy_snapshot_history_buffer_calendar_days,
    resolve_environment_export_first_ready_date,
    resolve_environment_export_window,
    resolve_environment_report_history_start,
    resolve_environment_warmup_history_start,
    resolve_factor_report_history_start,
    resolve_strategy_snapshot_history_start,
)


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

    def test_recommended_environment_history_buffer_calendar_days_respects_dynamic_window_and_cap(
        self,
    ) -> None:
        default_config = load_strategy_parameters()
        self.assertLess(
            recommended_environment_history_buffer_calendar_days(default_config),
            default_config.exports.env_history_buffer_calendar_days,
        )

        capped_config = load_strategy_parameters(
            overrides={
                "exports": {"env_history_buffer_calendar_days": 100},
                "env": {
                    "percentile_window": 400,
                    "percentile_min_periods": 400,
                    "equity": {"momentum_window": 200},
                    "bond": {"yield_change_window": 200},
                },
            }
        )
        self.assertEqual(
            recommended_environment_history_buffer_calendar_days(capped_config),
            capped_config.exports.env_history_buffer_calendar_days,
        )

    def test_recommended_strategy_snapshot_history_buffer_calendar_days_supports_auto_mode(
        self,
    ) -> None:
        config = load_strategy_parameters(
            overrides={"strategy": {"history_buffer_calendar_days": 0}}
        )

        self.assertEqual(
            recommended_strategy_snapshot_history_buffer_calendar_days(config),
            max(
                recommended_environment_history_buffer_calendar_days(config),
                recommended_factor_history_buffer_calendar_days(config),
            ),
        )

    def test_recommended_strategy_snapshot_history_buffer_calendar_days_does_not_shrink_below_dynamic_requirement(
        self,
    ) -> None:
        config = load_strategy_parameters(
            overrides={"strategy": {"history_buffer_calendar_days": 120}}
        )

        self.assertEqual(
            recommended_strategy_snapshot_history_buffer_calendar_days(config),
            max(
                120,
                recommended_environment_history_buffer_calendar_days(config),
                recommended_factor_history_buffer_calendar_days(config),
            ),
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

    def test_resolve_environment_report_history_start_uses_dynamic_window_by_default(self) -> None:
        config = load_strategy_parameters()
        loader = FakeLoader(cache_service=None)
        requested_start = pd.Timestamp("2026-04-20")

        history_start = resolve_environment_report_history_start(
            loader=loader,
            requested_start=requested_start,
            config=config,
            refresh=False,
        )

        self.assertEqual(
            history_start,
            requested_start
            - pd.Timedelta(
                days=recommended_environment_history_buffer_calendar_days(config)
            ),
        )

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

    def test_resolve_strategy_snapshot_history_start_supports_auto_mode(self) -> None:
        config = load_strategy_parameters(
            overrides={"strategy": {"history_buffer_calendar_days": 0}}
        )

        history_start = resolve_strategy_snapshot_history_start(
            trade_date="2026-04-20",
            config=config,
        )

        self.assertEqual(history_start, pd.Timestamp("2025-11-11"))

    def test_resolve_environment_export_first_ready_date_skips_requested_warmup_days(self) -> None:
        dates = pd.date_range("2025-01-01", periods=20, freq="B")

        first_ready = resolve_environment_export_first_ready_date(
            score_dates=pd.Series(dates),
            requested_start=dates[3],
            requested_end=dates[10],
            warmup_observation_count=5,
        )

        self.assertEqual(first_ready, pd.Timestamp(dates[5]))

    def test_resolve_environment_export_window_collects_warmup_and_trend_semantics(self) -> None:
        dates = pd.date_range("2025-01-01", periods=20, freq="B")
        readiness = pd.DataFrame(
            {
                "trade_date": dates,
                "trend_ready": [False] * 6 + [True] * 14,
            }
        )
        requested_trade_days = pd.Series(dates[3:11])

        resolution = resolve_environment_export_window(
            score_dates=pd.Series(dates),
            readiness=readiness,
            requested_trade_days=requested_trade_days,
            requested_start=dates[3],
            requested_end=dates[10],
            warmup_observation_count=5,
        )

        self.assertEqual(resolution.warmup_first_ready_date, pd.Timestamp(dates[5]))
        self.assertEqual(resolution.trend_first_ready_date, pd.Timestamp(dates[6]))
        self.assertEqual(resolution.effective_start, pd.Timestamp(dates[5]))
        self.assertEqual(resolution.warmup_trade_days_excluded, 2)
        self.assertTrue(any("预热区间" in note for note in resolution.notes))

    def test_first_ready_trade_date_returns_none_when_column_missing(self) -> None:
        self.assertIsNone(first_ready_trade_date(pd.DataFrame(), "trend_ready"))

    def test_build_environment_warmup_notes_only_when_requested_window_is_trimmed(self) -> None:
        self.assertEqual(
            build_environment_warmup_notes(
                requested_start="2025-01-06",
                requested_end="2025-01-10",
                first_ready_trade_date=pd.Timestamp("2025-01-06"),
                warmup_trade_days_excluded=0,
            ),
            (),
        )

        notes = build_environment_warmup_notes(
            requested_start="2025-01-06",
            requested_end="2025-01-10",
            first_ready_trade_date=pd.Timestamp("2025-01-08"),
            warmup_trade_days_excluded=2,
        )
        self.assertEqual(len(notes), 2)
        self.assertIn("已跳过 2 个请求窗口内交易日", notes[1])

    def test_count_trade_days_in_range_is_end_exclusive(self) -> None:
        dates = pd.Series(pd.date_range("2025-01-06", periods=5, freq="B"))

        count = count_trade_days_in_range(
            trade_days=dates,
            start_ts=dates.iloc[0],
            end_exclusive_ts=dates.iloc[2],
        )

        self.assertEqual(count, 2)

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

    def test_build_history_notes_ignores_weekend_only_shift_when_no_trade_days_are_skipped(self) -> None:
        trade_days = pd.Series(pd.to_datetime(["2024-10-21", "2024-10-22"]))

        notes = build_history_notes(
            history_start_requested=pd.Timestamp("2024-10-19"),
            history_start_used=pd.Timestamp("2024-10-21"),
            context="策略 snapshot",
            trade_days=trade_days,
        )

        self.assertEqual(notes, ())

    def test_build_history_notes_still_warns_when_open_days_are_skipped(self) -> None:
        trade_days = pd.Series(pd.to_datetime(["2024-10-18", "2024-10-21", "2024-10-22"]))

        notes = build_history_notes(
            history_start_requested=pd.Timestamp("2024-10-18"),
            history_start_used=pd.Timestamp("2024-10-21"),
            context="策略 snapshot",
            trade_days=trade_days,
        )

        self.assertEqual(len(notes), 2)
        self.assertIn("策略 snapshot预热窗口未完全覆盖", notes[0])
        self.assertIn("2024-10-21", notes[1])


if __name__ == "__main__":
    unittest.main()
