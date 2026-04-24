from __future__ import annotations

import unittest

import pandas as pd

from config.strategy_config import load_strategy_parameters
from strategy.service import StrategyService
from strategy.snapshot import StrategySnapshot


class FakeCacheService:
    def __init__(self) -> None:
        self._stats: dict[str, int] = {}

    def bump(self, key: str, value: int = 1) -> None:
        self._stats[key] = int(self._stats.get(key, 0)) + int(value)

    def stats_snapshot(self) -> dict[str, int]:
        return dict(self._stats)


class FakeLoader:
    def __init__(self) -> None:
        self.cache_service = FakeCacheService()
        self._runtime_dependency_revision = 0
        self.cb_rate_calls: list[list[str]] = []
        self.cb_call_requests: list[tuple[pd.Timestamp, pd.Timestamp, tuple[str, ...], bool]] = []
        self.credit_spread_calls: list[tuple[pd.Timestamp, pd.Timestamp, bool]] = []
        self.trading_calendar_calls = 0
        self.macro_daily_calls = 0
        self.cb_daily_cross_section_calls = 0
        self.cb_basic_calls = 0
        self.cb_call_calls = 0
        self.calendar = pd.DataFrame(
            [
                {"calendar_date": pd.Timestamp("2024-10-18"), "is_open": 1},
                {"calendar_date": pd.Timestamp("2025-10-01"), "is_open": 1},
                {"calendar_date": pd.Timestamp("2026-04-20"), "is_open": 1},
            ]
        )
        self.macro = pd.DataFrame(
            [
                {
                    "indicator_code": "csi300",
                    "trade_date": pd.Timestamp("2025-10-01"),
                    "value": 1.0,
                    "source_table": "local_reference",
                },
                {
                    "indicator_code": "credit_spread",
                    "trade_date": pd.Timestamp("2025-10-01"),
                    "value": 1.0,
                    "source_table": "local_reference",
                },
            ]
        )
        self.cb_daily = pd.DataFrame(
            [
                {
                    "cb_code": "110001.SH",
                    "trade_date": pd.Timestamp("2025-10-01"),
                    "close": 101.0,
                    "amount": 300.0,
                    "premium_rate": 20.0,
                    "ytm": 1.2,
                    "convert_value": 85.0,
                    "is_tradable": True,
                },
                {
                    "cb_code": "110002.SH",
                    "trade_date": pd.Timestamp("2025-10-01"),
                    "close": 102.0,
                    "amount": 280.0,
                    "premium_rate": 18.0,
                    "ytm": None,
                    "convert_value": 86.0,
                    "is_tradable": True,
                },
                {
                    "cb_code": "110001.SH",
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "close": 103.0,
                    "amount": 320.0,
                    "premium_rate": 19.0,
                    "ytm": 1.3,
                    "convert_value": 88.0,
                    "is_tradable": True,
                },
                {
                    "cb_code": "110002.SH",
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "close": 104.0,
                    "amount": 310.0,
                    "premium_rate": 17.0,
                    "ytm": None,
                    "convert_value": 89.0,
                    "is_tradable": True,
                },
            ]
        )
        self.cb_basic = pd.DataFrame(
            [
                {"cb_code": "110001.SH", "remain_size": 8.0},
                {"cb_code": "110002.SH", "remain_size": 7.0},
            ]
        )
        self.cb_call = pd.DataFrame(
            columns=["cb_code", "call_type", "call_status", "announcement_date", "call_date"]
        )
        self.cb_rate = pd.DataFrame(
            [
                {"cb_code": "110002.SH", "coupon_year": 1, "rate_start_date": pd.Timestamp("2024-01-01"), "rate_end_date": pd.Timestamp("2027-01-01"), "coupon_rate": 0.5},
            ]
        )

    def ensure_credit_spread_reference_coverage(self, start_date, end_date, refresh=False):
        self.cache_service.bump("cache_file_scan_calls")
        self.credit_spread_calls.append(
            (pd.Timestamp(start_date).normalize(), pd.Timestamp(end_date).normalize(), bool(refresh))
        )
        return self.macro

    def get_trading_calendar(self, start_date, end_date, exchange=None, refresh=False):
        self.cache_service.bump("cache_resolution_hit_calls")
        self.trading_calendar_calls += 1
        return self.calendar.copy()

    def get_macro_daily(self, indicators, start_date, end_date):
        self.cache_service.bump("cache_resolution_partial_hit_calls")
        self.macro_daily_calls += 1
        rows = []
        for indicator in indicators:
            matched = self.macro.loc[self.macro["indicator_code"].eq(indicator)].copy()
            if matched.empty:
                matched = pd.DataFrame(
                    [
                        {
                            "indicator_code": indicator,
                            "trade_date": pd.Timestamp("2025-10-01"),
                            "value": 1.0,
                            "source_table": "local_reference",
                        }
                    ]
                )
            rows.append(matched)
        return pd.concat(rows, ignore_index=True)

    def get_cb_daily_cross_section(
        self,
        start_date,
        end_date,
        refresh=False,
        columns=None,
        aggregate_profile=None,
    ):
        self.cache_service.bump("panel_memory_hit_calls")
        self.cache_service.bump("panel_memory_save_calls")
        self.cache_service.bump("aggregate_memory_hit_calls")
        self.cache_service.bump(
            "stage_calls::cb_daily_cross_section::request_panel_lookup"
        )
        self.cache_service.bump(
            "stage_elapsed_ms::cb_daily_cross_section::request_panel_lookup",
            12,
        )
        self.cb_daily_cross_section_calls += 1
        frame = self.cb_daily.copy()
        if columns is not None:
            frame = frame.loc[:, list(columns)].copy()
        return frame

    def get_cb_basic(self):
        self.cache_service.bump("cache_resolution_hit_calls")
        self.cb_basic_calls += 1
        return self.cb_basic.copy()

    def get_cb_call(self, start_date, end_date, codes=None, refresh=False):
        self.cache_service.bump("cache_resolution_miss_calls")
        self.cb_call_calls += 1
        code_list = tuple(str(code) for code in (codes or ()))
        self.cb_call_requests.append(
            (
                pd.Timestamp(start_date).normalize(),
                pd.Timestamp(end_date).normalize(),
                code_list,
                bool(refresh),
            )
        )
        frame = self.cb_call.copy()
        if not frame.empty:
            frame = frame.loc[
                frame["announcement_date"].between(
                    pd.Timestamp(start_date).normalize(),
                    pd.Timestamp(end_date).normalize(),
                )
            ].copy()
            if code_list:
                frame = frame.loc[frame["cb_code"].isin(code_list)].copy()
        return frame

    def get_cb_rate(self, codes, refresh=False):
        self.cache_service.bump("remote_fill_calls")
        code_list = list(codes)
        self.cb_rate_calls.append(code_list)
        return self.cb_rate.loc[self.cb_rate["cb_code"].isin(code_list)].copy()

    def runtime_dependency_revision(self) -> int:
        return int(self._runtime_dependency_revision)

    def bump_runtime_dependency_revision(self, value: int = 1) -> None:
        self._runtime_dependency_revision += int(value)


class WeekendAlignedLoader(FakeLoader):
    def __init__(self) -> None:
        super().__init__()
        self.calendar = pd.DataFrame(
            [
                {"calendar_date": pd.Timestamp("2024-10-19"), "is_open": 0},
                {"calendar_date": pd.Timestamp("2024-10-20"), "is_open": 0},
                {"calendar_date": pd.Timestamp("2024-10-21"), "is_open": 1},
                {"calendar_date": pd.Timestamp("2026-04-22"), "is_open": 1},
            ]
        )
        self.macro = pd.DataFrame(
            [
                {
                    "indicator_code": indicator,
                    "trade_date": pd.Timestamp("2024-10-21"),
                    "value": 1.0,
                    "source_table": "local_reference",
                }
                for indicator in (
                    "csi300",
                    "csi300_amount",
                    "bond_index",
                    "treasury_10y",
                    "credit_spread",
                    "cb_equal_weight",
                )
            ]
        )
        self.cb_daily = pd.DataFrame(
            [
                {
                    "cb_code": "110001.SH",
                    "trade_date": pd.Timestamp("2024-10-21"),
                    "close": 101.0,
                    "amount": 300.0,
                    "premium_rate": 20.0,
                    "ytm": 1.2,
                    "convert_value": 85.0,
                    "is_tradable": True,
                },
                {
                    "cb_code": "110001.SH",
                    "trade_date": pd.Timestamp("2026-04-22"),
                    "close": 103.0,
                    "amount": 320.0,
                    "premium_rate": 19.0,
                    "ytm": 1.3,
                    "convert_value": 88.0,
                    "is_tradable": True,
                },
            ]
        )
        self.cb_basic = pd.DataFrame([{"cb_code": "110001.SH", "remain_size": 8.0}])
        self.cb_rate = pd.DataFrame(
            columns=[
                "cb_code",
                "coupon_year",
                "rate_start_date",
                "rate_end_date",
                "coupon_rate",
            ]
        )

    def get_macro_daily(self, indicators, start_date, end_date):
        self.cache_service.bump("cache_resolution_partial_hit_calls")
        self.macro_daily_calls += 1
        return self.macro.loc[self.macro["indicator_code"].isin(indicators)].copy()


class StrategyServiceTests(unittest.TestCase):
    def test_build_snapshot_collects_required_frames(self) -> None:
        service = StrategyService(loader=FakeLoader())

        snapshot = service.build_snapshot(
            trade_date="2026-04-20",
            requested_codes=["110001", "110001.SH", "110002.SH", "110001"],
        )

        self.assertIsInstance(snapshot, StrategySnapshot)
        self.assertEqual(snapshot.trade_date, pd.Timestamp("2026-04-20"))
        self.assertEqual(snapshot.requested_codes, ("110001.SH", "110002.SH"))
        self.assertEqual(set(snapshot.cb_daily["cb_code"]), {"110001.SH", "110002.SH"})
        self.assertEqual(snapshot.history_window.requested_start, pd.Timestamp("2024-10-17"))
        self.assertEqual(snapshot.history_window.used_start, pd.Timestamp("2025-10-01"))
        self.assertTrue(snapshot.data_quality_hints)

    def test_build_snapshot_normalizes_requested_codes_from_raw_string(self) -> None:
        service = StrategyService(loader=FakeLoader())

        snapshot = service.build_snapshot(
            trade_date="2026-04-20",
            requested_codes="110001, 110002.SH 128123",
        )

        self.assertEqual(
            snapshot.requested_codes,
            ("110001.SH", "110002.SH", "128123.SZ"),
        )

    def test_build_snapshot_prefetches_cb_rate_only_for_missing_ytm_codes(self) -> None:
        loader = FakeLoader()
        service = StrategyService(loader=loader)

        service.build_snapshot(trade_date="2026-04-20")

        self.assertEqual(loader.cb_rate_calls, [["110002.SH"]])
        self.assertEqual(len(loader.credit_spread_calls), 1)
        self.assertEqual(
            loader.cb_call_requests,
            [
                (
                    pd.Timestamp("2024-10-17"),
                    pd.Timestamp("2026-04-20"),
                    ("110001.SH", "110002.SH"),
                    False,
                )
            ],
        )

    def test_build_snapshot_widens_cb_call_history_to_snapshot_list_dates(self) -> None:
        loader = FakeLoader()
        loader.cb_basic = pd.DataFrame(
            [
                {
                    "cb_code": "110001.SH",
                    "remain_size": 8.0,
                    "list_date": pd.Timestamp("2025-01-03"),
                },
                {
                    "cb_code": "110002.SH",
                    "remain_size": 7.0,
                    "list_date": pd.Timestamp("2025-02-05"),
                },
            ]
        )
        service = StrategyService(
            loader=loader,
            config=load_strategy_parameters(
                overrides={"strategy": {"history_buffer_calendar_days": 0}}
            ),
        )

        service.build_snapshot(trade_date="2026-04-20")

        self.assertEqual(
            loader.cb_call_requests,
            [
                (
                    pd.Timestamp("2025-01-03"),
                    pd.Timestamp("2026-04-20"),
                    ("110001.SH", "110002.SH"),
                    False,
                )
            ],
        )

    def test_build_snapshot_ignores_weekend_only_history_shift_in_quality_hints(self) -> None:
        service = StrategyService(loader=WeekendAlignedLoader())

        snapshot = service.build_snapshot(trade_date="2026-04-22")

        self.assertEqual(snapshot.history_window.requested_start, pd.Timestamp("2024-10-19"))
        self.assertEqual(snapshot.history_window.used_start, pd.Timestamp("2024-10-21"))
        self.assertEqual(snapshot.data_quality_hints, ())

    def test_build_snapshot_runtime_reuses_same_trade_date_without_reloading(self) -> None:
        loader = FakeLoader()
        service = StrategyService(loader=loader)

        first = service.build_snapshot(
            trade_date="2026-04-20",
            requested_codes="110001",
        )
        first_counts = (
            len(loader.credit_spread_calls),
            loader.trading_calendar_calls,
            loader.macro_daily_calls,
            loader.cb_daily_cross_section_calls,
            loader.cb_basic_calls,
            loader.cb_call_calls,
            len(loader.cb_rate_calls),
        )

        second = service.build_snapshot(
            trade_date="2026-04-20",
            requested_codes="110002",
        )

        self.assertFalse(first.runtime_snapshot_reused)
        self.assertTrue(second.runtime_snapshot_reused)
        self.assertEqual(
            int(first.cache_diagnostics["layers"]["request_panel_memory"]["hits"]),
            1,
        )
        self.assertEqual(
            int(second.cache_diagnostics["summary"]["remote_fills"]),
            0,
        )
        self.assertEqual(first.requested_codes, ("110001.SH",))
        self.assertEqual(second.requested_codes, ("110002.SH",))
        self.assertEqual(
            first_counts,
            (
                len(loader.credit_spread_calls),
                loader.trading_calendar_calls,
                loader.macro_daily_calls,
                loader.cb_daily_cross_section_calls,
                loader.cb_basic_calls,
                loader.cb_call_calls,
                len(loader.cb_rate_calls),
            ),
        )

    def test_build_snapshot_refresh_bypasses_runtime_reuse_but_rewarms_it(self) -> None:
        loader = FakeLoader()
        service = StrategyService(loader=loader)

        service.build_snapshot(trade_date="2026-04-20")
        counts_after_first = (
            len(loader.credit_spread_calls),
            loader.trading_calendar_calls,
            loader.macro_daily_calls,
            loader.cb_daily_cross_section_calls,
            loader.cb_basic_calls,
            loader.cb_call_calls,
            len(loader.cb_rate_calls),
        )

        refreshed = service.build_snapshot(
            trade_date="2026-04-20",
            refresh=True,
        )
        counts_after_refresh = (
            len(loader.credit_spread_calls),
            loader.trading_calendar_calls,
            loader.macro_daily_calls,
            loader.cb_daily_cross_section_calls,
            loader.cb_basic_calls,
            loader.cb_call_calls,
            len(loader.cb_rate_calls),
        )
        reused = service.build_snapshot(trade_date="2026-04-20")

        self.assertFalse(refreshed.runtime_snapshot_reused)
        self.assertTrue(refreshed.refresh_requested)
        self.assertGreater(counts_after_refresh[0], counts_after_first[0])
        self.assertGreater(counts_after_refresh[1], counts_after_first[1])
        self.assertGreater(counts_after_refresh[2], counts_after_first[2])
        self.assertGreater(counts_after_refresh[3], counts_after_first[3])
        self.assertGreater(counts_after_refresh[4], counts_after_first[4])
        self.assertGreater(counts_after_refresh[5], counts_after_first[5])
        self.assertGreater(counts_after_refresh[6], counts_after_first[6])
        self.assertTrue(reused.runtime_snapshot_reused)
        self.assertEqual(
            int(reused.cache_diagnostics["summary"]["cache_hits"]),
            0,
        )

    def test_build_snapshot_runtime_reuse_is_invalidated_when_loader_revision_changes(
        self,
    ) -> None:
        loader = FakeLoader()
        service = StrategyService(loader=loader)

        service.build_snapshot(trade_date="2026-04-20")
        counts_after_first = (
            len(loader.credit_spread_calls),
            loader.trading_calendar_calls,
            loader.macro_daily_calls,
            loader.cb_daily_cross_section_calls,
            loader.cb_basic_calls,
            loader.cb_call_calls,
            len(loader.cb_rate_calls),
        )
        loader.bump_runtime_dependency_revision()

        rebuilt = service.build_snapshot(trade_date="2026-04-20")

        self.assertFalse(rebuilt.runtime_snapshot_reused)
        self.assertGreater(len(loader.credit_spread_calls), counts_after_first[0])
        self.assertGreater(loader.trading_calendar_calls, counts_after_first[1])
        self.assertGreater(loader.macro_daily_calls, counts_after_first[2])
        self.assertGreater(loader.cb_daily_cross_section_calls, counts_after_first[3])
        self.assertGreater(loader.cb_basic_calls, counts_after_first[4])
        self.assertGreater(loader.cb_call_calls, counts_after_first[5])
        self.assertGreater(len(loader.cb_rate_calls), counts_after_first[6])

    def test_build_snapshot_rejects_non_open_trade_date(self) -> None:
        service = StrategyService(loader=FakeLoader())

        with self.assertRaises(ValueError):
            service.build_snapshot(trade_date="2026-04-19")


if __name__ == "__main__":
    unittest.main()
