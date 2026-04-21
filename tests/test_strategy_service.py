from __future__ import annotations

import unittest

import pandas as pd

from strategy.service import StrategyService
from strategy.snapshot import StrategySnapshot


class FakeCacheService:
    def __init__(self) -> None:
        self.panel_hits = 0


class FakeLoader:
    def __init__(self) -> None:
        self.cache_service = FakeCacheService()
        self.cb_rate_calls: list[list[str]] = []
        self.credit_spread_calls: list[tuple[pd.Timestamp, pd.Timestamp, bool]] = []
        self.calendar = pd.DataFrame(
            [
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
        self.credit_spread_calls.append(
            (pd.Timestamp(start_date).normalize(), pd.Timestamp(end_date).normalize(), bool(refresh))
        )
        return self.macro

    def get_trading_calendar(self, start_date, end_date, exchange=None, refresh=False):
        return self.calendar.copy()

    def get_macro_daily(self, indicators, start_date, end_date):
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
        frame = self.cb_daily.copy()
        if columns is not None:
            frame = frame.loc[:, list(columns)].copy()
        return frame

    def get_cb_basic(self):
        return self.cb_basic.copy()

    def get_cb_call(self, start_date, end_date, refresh=False):
        return self.cb_call.copy()

    def get_cb_rate(self, codes, refresh=False):
        code_list = list(codes)
        self.cb_rate_calls.append(code_list)
        return self.cb_rate.loc[self.cb_rate["cb_code"].isin(code_list)].copy()


class StrategyServiceTests(unittest.TestCase):
    def test_build_snapshot_collects_required_frames(self) -> None:
        service = StrategyService(loader=FakeLoader())

        snapshot = service.build_snapshot(
            trade_date="2026-04-20",
            requested_codes=["110001.SH", "110001.SH", "110002.SH"],
        )

        self.assertIsInstance(snapshot, StrategySnapshot)
        self.assertEqual(snapshot.trade_date, pd.Timestamp("2026-04-20"))
        self.assertEqual(snapshot.requested_codes, ("110001.SH", "110002.SH"))
        self.assertEqual(set(snapshot.cb_daily["cb_code"]), {"110001.SH", "110002.SH"})
        self.assertEqual(snapshot.history_window.requested_start, pd.Timestamp("2024-10-17"))
        self.assertEqual(snapshot.history_window.used_start, pd.Timestamp("2025-10-01"))
        self.assertTrue(snapshot.data_quality_hints)

    def test_build_snapshot_prefetches_cb_rate_only_for_missing_ytm_codes(self) -> None:
        loader = FakeLoader()
        service = StrategyService(loader=loader)

        service.build_snapshot(trade_date="2026-04-20")

        self.assertEqual(loader.cb_rate_calls, [["110002.SH"]])
        self.assertEqual(len(loader.credit_spread_calls), 1)

    def test_build_snapshot_rejects_non_open_trade_date(self) -> None:
        service = StrategyService(loader=FakeLoader())

        with self.assertRaises(ValueError):
            service.build_snapshot(trade_date="2026-04-19")


if __name__ == "__main__":
    unittest.main()
