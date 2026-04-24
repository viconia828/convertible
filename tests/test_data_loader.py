from __future__ import annotations

import shutil
import unittest
from pathlib import Path

import pandas as pd

from data.data_loader import DataLoader
from data.schema import DataSchema

TMP_ROOT = Path(__file__).resolve().parent / "_tmp"
TMP_ROOT.mkdir(parents=True, exist_ok=True)


def make_case_dir(case_name: str) -> Path:
    case_dir = TMP_ROOT / case_name
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True, exist_ok=True)
    return case_dir


class FakeTushareClient:
    def __init__(self, tables: dict[str, pd.DataFrame]) -> None:
        self.tables = tables
        self.calls: list[tuple[str, dict[str, object] | None, str | None]] = []

    def query(
        self,
        api_name: str,
        params: dict[str, object] | None = None,
        fields: str | None = None,
    ) -> pd.DataFrame:
        self.calls.append((api_name, params, fields))
        frame = self.tables[api_name].copy()

        if params and "ts_code" in params and "ts_code" in frame.columns:
            frame = frame.loc[frame["ts_code"] == params["ts_code"]]
        if params and "exchange" in params and "exchange" in frame.columns:
            frame = frame.loc[frame["exchange"] == params["exchange"]]
        if params and "curve_type" in params and "curve_type" in frame.columns:
            frame = frame.loc[frame["curve_type"].astype(str) == str(params["curve_type"])]
        if params and "curve_term" in params and "curve_term" in frame.columns:
            frame = frame.loc[frame["curve_term"].astype(float) == float(params["curve_term"])]
        if params and "trade_date" in params and "trade_date" in frame.columns:
            frame = frame.loc[
                pd.to_datetime(frame["trade_date"]) == pd.Timestamp(params["trade_date"])
            ]
        if params and "start_date" in params:
            trade_column = (
                "trade_date"
                if "trade_date" in frame.columns
                else "ann_date"
                if "ann_date" in frame.columns
                else "cal_date"
            )
            frame = frame.loc[
                pd.to_datetime(frame[trade_column]) >= pd.Timestamp(params["start_date"])
            ]
        if params and "end_date" in params:
            trade_column = (
                "trade_date"
                if "trade_date" in frame.columns
                else "ann_date"
                if "ann_date" in frame.columns
                else "cal_date"
            )
            frame = frame.loc[
                pd.to_datetime(frame[trade_column]) <= pd.Timestamp(params["end_date"])
            ]
        return frame.reset_index(drop=True)


class DataLoaderTests(unittest.TestCase):
    def test_cb_basic_is_split_and_reused_from_cache(self) -> None:
        client = FakeTushareClient(
            {
                "cb_basic": pd.DataFrame(
                    [
                        {
                            "ts_code": "110001.SH",
                            "bond_full_name": "测试转债全称",
                            "bond_short_name": "测试转债",
                            "stk_code": "600001.SH",
                            "stk_short_name": "测试正股",
                            "maturity": 6.0,
                            "par": 100.0,
                            "issue_price": 100.0,
                            "issue_size": 10.0,
                            "remain_size": 8.5,
                            "value_date": "20240101",
                            "maturity_date": "20300101",
                            "rate_type": "固定",
                            "coupon_rate": 0.5,
                            "add_rate": 0.0,
                            "pay_per_year": 1,
                            "list_date": "20240201",
                            "delist_date": None,
                            "exchange": "SH",
                            "conv_start_date": "20240801",
                            "conv_end_date": "20291231",
                            "conv_stop_date": None,
                            "first_conv_price": 12.34,
                            "conv_price": 11.11,
                            "rate_clause": "demo",
                        }
                    ]
                )
            }
        )

        case_dir = make_case_dir("cb_basic_cache")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        first = loader.get_cb_basic()
        second = loader.get_cb_basic()

        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertEqual(len(client.calls), 1)
        self.assertTrue(
            (case_dir / "cache" / "tushare" / "static" / "cb_basic_fixed.csv").exists()
        )
        self.assertTrue(
            (case_dir / "cache" / "tushare" / "static" / "cb_basic_mutable.csv").exists()
        )

    def test_cb_daily_uses_per_code_cache(self) -> None:
        client = FakeTushareClient(
            {
                "cb_daily": pd.DataFrame(
                    [
                        {
                            "ts_code": "110001.SH",
                            "trade_date": "20260401",
                            "pre_close": 100.0,
                            "open": 101.0,
                            "high": 102.0,
                            "low": 99.0,
                            "close": 101.0,
                            "change": 1.0,
                            "pct_chg": 1.0,
                            "vol": 10.0,
                            "amount": 20.0,
                        },
                        {
                            "ts_code": "110001.SH",
                            "trade_date": "20260402",
                            "pre_close": 101.0,
                            "open": 101.0,
                            "high": 101.0,
                            "low": 100.0,
                            "close": 100.0,
                            "change": -1.0,
                            "pct_chg": -0.99,
                            "vol": 12.0,
                            "amount": 22.0,
                        },
                    ]
                )
            }
        )

        case_dir = make_case_dir("cb_daily_cache")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        first = loader.get_cb_daily("110001.SH", "2026-04-01", "2026-04-02")
        second = loader.get_cb_daily("110001.SH", "2026-04-01", "2026-04-02")

        self.assertEqual(len(first), 2)
        self.assertEqual(len(second), 2)
        self.assertEqual(len(client.calls), 1)
        self.assertTrue(first["is_tradable"].all())

    def test_cb_daily_only_fetches_missing_codes(self) -> None:
        client = FakeTushareClient(
            {
                "cb_daily": pd.DataFrame(
                    [
                        {
                            "ts_code": "110001.SH",
                            "trade_date": "20260401",
                            "pre_close": 100.0,
                            "open": 101.0,
                            "high": 102.0,
                            "low": 99.0,
                            "close": 101.0,
                            "change": 1.0,
                            "pct_chg": 1.0,
                            "vol": 10.0,
                            "amount": 20.0,
                        },
                        {
                            "ts_code": "110001.SH",
                            "trade_date": "20260402",
                            "pre_close": 101.0,
                            "open": 101.0,
                            "high": 101.0,
                            "low": 100.0,
                            "close": 100.0,
                            "change": -1.0,
                            "pct_chg": -0.99,
                            "vol": 12.0,
                            "amount": 22.0,
                        },
                        {
                            "ts_code": "110002.SH",
                            "trade_date": "20260401",
                            "pre_close": 100.0,
                            "open": 100.0,
                            "high": 101.0,
                            "low": 99.0,
                            "close": 100.0,
                            "change": 0.0,
                            "pct_chg": 0.0,
                            "vol": 11.0,
                            "amount": 21.0,
                        },
                        {
                            "ts_code": "110002.SH",
                            "trade_date": "20260402",
                            "pre_close": 100.0,
                            "open": 102.0,
                            "high": 102.0,
                            "low": 100.0,
                            "close": 102.0,
                            "change": 2.0,
                            "pct_chg": 2.0,
                            "vol": 13.0,
                            "amount": 23.0,
                        },
                    ]
                )
            }
        )

        case_dir = make_case_dir("cb_daily_missing_codes")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        loader.cache_store.save_time_series(
            loader.source_name,
            "cb_daily",
            "110001.SH",
            pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-01",
                        "pre_close": 100.0,
                        "open": 101.0,
                        "high": 102.0,
                        "low": 99.0,
                        "close": 101.0,
                        "price_change": 1.0,
                        "pct_change": 1.0,
                        "volume": 10.0,
                        "amount": 20.0,
                        "ytm": pd.NA,
                        "is_tradable": True,
                    },
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-02",
                        "pre_close": 101.0,
                        "open": 101.0,
                        "high": 101.0,
                        "low": 100.0,
                        "close": 100.0,
                        "price_change": -1.0,
                        "pct_change": -0.99,
                        "volume": 12.0,
                        "amount": 22.0,
                        "ytm": pd.NA,
                        "is_tradable": True,
                    },
                ]
            ),
        )

        frame = loader.get_cb_daily(
            ["110001.SH", "110002.SH"],
            "2026-04-01",
            "2026-04-02",
        )

        self.assertEqual(len(frame), 4)
        cb_daily_calls = [call for call in client.calls if call[0] == "cb_daily"]
        self.assertEqual(len(cb_daily_calls), 1)
        self.assertEqual(cb_daily_calls[0][1]["ts_code"], "110002.SH")
        stats = loader.cache_service.stats_snapshot()
        self.assertEqual(
            int(stats.get("cache_resolution_partial_hit_calls::cb_daily", 0)),
            1,
        )
        self.assertEqual(
            int(stats.get("remote_fill_calls::cb_daily", 0)),
            1,
        )
        self.assertGreaterEqual(
            int(stats.get("stage_elapsed_ms::cb_daily::remote_fetch", 0)),
            1,
        )

    def test_cb_daily_can_enrich_premium_and_ytm(self) -> None:
        client = FakeTushareClient(
            {
                "cb_daily": pd.DataFrame(
                    [
                        {
                            "ts_code": "110001.SH",
                            "trade_date": "20260401",
                            "pre_close": 95.0,
                            "open": 95.0,
                            "high": 96.0,
                            "low": 94.0,
                            "close": 95.0,
                            "change": 0.0,
                            "pct_chg": 0.0,
                            "vol": 10.0,
                            "amount": 20.0,
                            "bond_value": 92.0,
                            "bond_over_rate": 3.26,
                            "cb_value": 90.0,
                            "cb_over_rate": 5.56,
                        }
                    ]
                ),
                "cb_basic": pd.DataFrame(
                    [
                        {
                            "ts_code": "110001.SH",
                            "bond_full_name": "测试转债全称",
                            "bond_short_name": "测试转债",
                            "stk_code": "600001.SH",
                            "stk_short_name": "测试正股",
                            "maturity": 6.0,
                            "par": 100.0,
                            "issue_price": 100.0,
                            "issue_size": 10.0,
                            "remain_size": 8.5,
                            "value_date": "20240101",
                            "maturity_date": "20270401",
                            "rate_type": "固定",
                            "coupon_rate": 5.0,
                            "add_rate": 0.0,
                            "pay_per_year": 1,
                            "list_date": "20240201",
                            "delist_date": None,
                            "exchange": "SH",
                            "conv_start_date": "20240801",
                            "conv_end_date": "20270401",
                            "conv_stop_date": None,
                            "first_conv_price": 12.34,
                            "conv_price": 11.11,
                            "rate_clause": "demo",
                        }
                    ]
                ),
                "cb_rate": pd.DataFrame(
                    [
                        {
                            "ts_code": "110001.SH",
                            "rate_freq": 1,
                            "rate_start_date": "20250401",
                            "rate_end_date": "20260401",
                            "coupon_rate": 5.0,
                        },
                        {
                            "ts_code": "110001.SH",
                            "rate_freq": 1,
                            "rate_start_date": "20260402",
                            "rate_end_date": "20270401",
                            "coupon_rate": 5.0,
                        },
                    ]
                ),
            }
        )

        case_dir = make_case_dir("cb_daily_enriched")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        frame = loader.get_cb_daily(
            "110001.SH",
            "2026-04-01",
            "2026-04-01",
            enrich=True,
        )

        self.assertAlmostEqual(float(frame.iloc[0]["premium_rate"]), 5.56, places=2)
        self.assertTrue(pd.notna(frame.iloc[0]["ytm"]))

    def test_cb_rate_only_fetches_missing_codes(self) -> None:
        client = FakeTushareClient(
            {
                "cb_rate": pd.DataFrame(
                    [
                        {
                            "ts_code": "110001.SH",
                            "rate_freq": 1,
                            "rate_start_date": "20250401",
                            "rate_end_date": "20260401",
                            "coupon_rate": 5.0,
                        },
                        {
                            "ts_code": "110002.SH",
                            "rate_freq": 1,
                            "rate_start_date": "20250401",
                            "rate_end_date": "20260401",
                            "coupon_rate": 3.0,
                        },
                    ]
                )
            }
        )

        case_dir = make_case_dir("cb_rate_missing_codes")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        loader.cache_store.save_time_series(
            loader.source_name,
            "cb_rate",
            "110001.SH",
            client.tables["cb_rate"].loc[
                client.tables["cb_rate"]["ts_code"] == "110001.SH"
            ].copy(),
        )

        frame = loader.get_cb_rate(["110001.SH", "110002.SH"])

        self.assertEqual(set(frame["cb_code"]), {"110001.SH", "110002.SH"})
        cb_rate_calls = [call for call in client.calls if call[0] == "cb_rate"]
        self.assertEqual(len(cb_rate_calls), 1)
        self.assertEqual(cb_rate_calls[0][1]["ts_code"], "110002.SH")
        stats = loader.cache_service.stats_snapshot()
        self.assertEqual(
            int(stats.get("cache_resolution_partial_hit_calls::cb_rate", 0)),
            1,
        )
        self.assertEqual(
            int(stats.get("remote_fill_calls::cb_rate", 0)),
            1,
        )

    def test_cb_rate_can_batch_load_cached_codes_without_remote_calls(self) -> None:
        client = FakeTushareClient(
            {
                "cb_rate": pd.DataFrame(
                    columns=[
                        "ts_code",
                        "rate_freq",
                        "rate_start_date",
                        "rate_end_date",
                        "coupon_rate",
                    ]
                )
            }
        )

        case_dir = make_case_dir("cb_rate_batch_cache")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        loader.CB_RATE_BATCH_CACHE_THRESHOLD = 2
        for code, coupon in [("110001.SH", 5.0), ("110002.SH", 3.0)]:
            loader.cache_store.save_time_series(
                loader.source_name,
                "cb_rate",
                code,
                pd.DataFrame(
                    [
                        {
                            "cb_code": code,
                            "rate_frequency": 1,
                            "rate_start_date": "2025-04-01",
                            "rate_end_date": "2026-04-01",
                            "coupon_rate": coupon,
                        }
                    ]
                ),
            )

        frame = loader.get_cb_rate(["110001.SH", "110002.SH"])

        self.assertEqual(set(frame["cb_code"]), {"110001.SH", "110002.SH"})
        self.assertEqual(len(client.calls), 0)

    def test_cb_call_can_cache_and_filter(self) -> None:
        client = FakeTushareClient(
            {
                "cb_call": pd.DataFrame(
                    [
                        {
                            "ts_code": "110001.SH",
                            "call_type": "强赎",
                            "is_call": "公告强赎",
                            "ann_date": "20260401",
                            "call_date": "20260420",
                        },
                        {
                            "ts_code": "110002.SH",
                            "call_type": "强赎",
                            "is_call": "公告不强赎",
                            "ann_date": "20260402",
                            "call_date": None,
                        },
                    ]
                )
            }
        )

        case_dir = make_case_dir("cb_call_cache")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        frame = loader.get_cb_call("2026-04-01", "2026-04-03", codes="110001.SH")
        frame_again = loader.get_cb_call("2026-04-01", "2026-04-03", codes="110001.SH")

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["call_status"], "公告强赎")
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(len(frame_again), 1)

    def test_persist_cb_daily_cross_section_derived_fields_fills_missing_ytm(self) -> None:
        client = FakeTushareClient({"trade_cal": pd.DataFrame()})

        case_dir = make_case_dir("persist_cross_section_derived_fields")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        loader.cache_store.save_time_series(
            loader.source_name,
            "cb_daily_cross_section",
            "20260401",
            pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-01",
                        "pre_close": 100.0,
                        "open": 101.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 101.0,
                        "price_change": 1.0,
                        "pct_change": 1.0,
                        "volume": 10.0,
                        "amount": 20.0,
                        "ytm": pd.NA,
                        "is_tradable": True,
                    }
                ]
            ),
        )

        loader.persist_cb_daily_cross_section_derived_fields(
            pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": pd.Timestamp("2026-04-01"),
                        "ytm": 0.052,
                    }
                ]
            )
        )

        cached = loader.cache_store.load_time_series(
            loader.source_name,
            "cb_daily_cross_section",
            "20260401",
        )
        cached = DataSchema.standardize("cb_daily", cached)
        self.assertAlmostEqual(float(cached.iloc[0]["ytm"]), 0.052, places=6)

    def test_macro_daily_supports_direct_index_and_curve_indicators(self) -> None:
        client = FakeTushareClient(
            {
                "index_daily": pd.DataFrame(
                    [
                        {
                            "ts_code": "000300.SH",
                            "trade_date": "20260401",
                            "close": 4000.0,
                            "open": 3980.0,
                            "high": 4010.0,
                            "low": 3970.0,
                            "pre_close": 3990.0,
                            "change": 10.0,
                            "pct_chg": 0.25,
                            "vol": 1.0,
                            "amount": 1000000.0,
                        },
                        {
                            "ts_code": "H11001.CSI",
                            "trade_date": "20260401",
                            "close": 250.0,
                            "open": 249.0,
                            "high": 251.0,
                            "low": 248.0,
                            "pre_close": 249.5,
                            "change": 0.5,
                            "pct_chg": 0.2,
                            "vol": None,
                            "amount": None,
                        },
                    ]
                ),
                "yc_cb": pd.DataFrame(
                    [
                        {
                            "ts_code": "1001.CB",
                            "trade_date": "20260401",
                            "curve_type": "0",
                            "curve_term": 10.0,
                            "yield": 1.80,
                        }
                    ]
                ),
            }
        )

        case_dir = make_case_dir("macro_direct")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        frame = loader.get_macro_daily(
            ["csi300", "csi300_amount", "bond_index", "treasury_10y"],
            "2026-04-01",
            "2026-04-01",
        )

        self.assertEqual(set(frame["indicator_code"]), {"csi300", "csi300_amount", "bond_index", "treasury_10y"})
        treasury_value = float(frame.loc[frame["indicator_code"] == "treasury_10y", "value"].iloc[0])
        self.assertAlmostEqual(treasury_value, 1.80, places=6)

    def test_macro_daily_auto_refreshes_credit_spread_reference_when_coverage_is_short(self) -> None:
        case_dir = make_case_dir("macro_credit_spread_auto_refresh")
        loader = DataLoader(cache_dir=case_dir / "cache", client=FakeTushareClient({}))
        reference_path = loader.cache_service.reference_frame_path("macro", "credit_spread")
        reference_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2025-01-01", "2025-01-02"]),
                "value": [0.91, 0.92],
                "indicator_code": ["credit_spread"] * 2,
                "source_table": ["local_snapshot"] * 2,
            }
        ).to_csv(reference_path, index=False, encoding="utf-8-sig")

        refresh_calls: list[tuple[pd.Timestamp, pd.Timestamp, bool]] = []

        def fake_refresh(
            start_date: object,
            end_date: object,
            use_existing_on_failure: bool = True,
            primary_source=None,
            backup_sources=None,
        ) -> pd.DataFrame:
            refresh_calls.append(
                (
                    pd.Timestamp(start_date).normalize(),
                    pd.Timestamp(end_date).normalize(),
                    bool(use_existing_on_failure),
                )
            )
            merged = DataSchema.standardize(
                "macro_daily",
                pd.DataFrame(
                    {
                        "trade_date": pd.to_datetime(
                            ["2024-12-02", "2024-12-03", "2025-01-01", "2025-01-02"]
                        ),
                        "value": [0.89, 0.90, 0.91, 0.92],
                        "indicator_code": ["credit_spread"] * 4,
                        "source_table": ["refreshed"] * 4,
                    }
                ),
            )
            merged.to_csv(reference_path, index=False, encoding="utf-8-sig")
            return merged

        loader.refresh_credit_spread_reference = fake_refresh

        frame = loader.get_macro_daily(
            ["credit_spread"],
            "2024-12-02",
            "2025-01-02",
        )

        self.assertEqual(len(refresh_calls), 1)
        self.assertEqual(
            refresh_calls[0],
            (pd.Timestamp("2024-12-02"), pd.Timestamp("2025-01-02"), True),
        )
        self.assertEqual(frame["trade_date"].min(), pd.Timestamp("2024-12-02"))
        self.assertEqual(frame["trade_date"].max(), pd.Timestamp("2025-01-02"))
        self.assertTrue((frame["indicator_code"] == "credit_spread").all())

    def test_cb_equal_weight_index_can_be_built_from_cross_section(self) -> None:
        client = FakeTushareClient(
            {
                "trade_cal": pd.DataFrame(
                    [
                        {"exchange": "SSE", "cal_date": "20260401", "is_open": 1, "pretrade_date": "20260331"},
                        {"exchange": "SSE", "cal_date": "20260402", "is_open": 1, "pretrade_date": "20260401"},
                    ]
                ),
                "cb_daily": pd.DataFrame(
                    [
                        {
                            "ts_code": "110001.SH",
                            "trade_date": "20260401",
                            "pre_close": 100.0,
                            "open": 101.0,
                            "high": 101.0,
                            "low": 100.0,
                            "close": 101.0,
                            "change": 1.0,
                            "pct_chg": 1.0,
                            "vol": 10.0,
                            "amount": 20.0,
                            "bond_value": 95.0,
                            "bond_over_rate": 6.31,
                            "cb_value": 98.0,
                            "cb_over_rate": 3.06,
                        },
                        {
                            "ts_code": "110002.SH",
                            "trade_date": "20260401",
                            "pre_close": 100.0,
                            "open": 99.0,
                            "high": 100.0,
                            "low": 99.0,
                            "close": 99.0,
                            "change": -1.0,
                            "pct_chg": -1.0,
                            "vol": 11.0,
                            "amount": 21.0,
                            "bond_value": 96.0,
                            "bond_over_rate": 3.13,
                            "cb_value": 97.0,
                            "cb_over_rate": 2.06,
                        },
                        {
                            "ts_code": "110001.SH",
                            "trade_date": "20260402",
                            "pre_close": 101.0,
                            "open": 102.0,
                            "high": 102.0,
                            "low": 101.0,
                            "close": 102.0,
                            "change": 1.0,
                            "pct_chg": 0.99,
                            "vol": 12.0,
                            "amount": 22.0,
                            "bond_value": 95.5,
                            "bond_over_rate": 6.81,
                            "cb_value": 99.0,
                            "cb_over_rate": 3.03,
                        },
                        {
                            "ts_code": "110002.SH",
                            "trade_date": "20260402",
                            "pre_close": 99.0,
                            "open": 100.0,
                            "high": 100.0,
                            "low": 99.0,
                            "close": 100.0,
                            "change": 1.0,
                            "pct_chg": 1.01,
                            "vol": 13.0,
                            "amount": 23.0,
                            "bond_value": 96.5,
                            "bond_over_rate": 3.63,
                            "cb_value": 98.0,
                            "cb_over_rate": 2.04,
                        },
                    ]
                ),
            }
        )

        case_dir = make_case_dir("cb_equal_weight")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        frame = loader.get_cb_equal_weight_index("2026-04-01", "2026-04-02")

        self.assertEqual(list(frame["indicator_code"].unique()), ["cb_equal_weight"])
        self.assertEqual(len(frame), 2)
        self.assertAlmostEqual(float(frame.iloc[0]["value"]), 100.0, places=6)
        self.assertGreater(float(frame.iloc[1]["value"]), 100.0)

    def test_cb_daily_cross_section_only_fetches_missing_trade_days(self) -> None:
        client = FakeTushareClient(
            {
                "trade_cal": pd.DataFrame(
                    [
                        {"exchange": "SSE", "cal_date": "20260401", "is_open": 1, "pretrade_date": "20260331"},
                        {"exchange": "SSE", "cal_date": "20260402", "is_open": 1, "pretrade_date": "20260401"},
                    ]
                ),
                "cb_daily": pd.DataFrame(
                    [
                        {
                            "ts_code": "110001.SH",
                            "trade_date": "20260401",
                            "pre_close": 100.0,
                            "open": 101.0,
                            "high": 101.0,
                            "low": 100.0,
                            "close": 101.0,
                            "change": 1.0,
                            "pct_chg": 1.0,
                            "vol": 10.0,
                            "amount": 20.0,
                        },
                        {
                            "ts_code": "110002.SH",
                            "trade_date": "20260401",
                            "pre_close": 100.0,
                            "open": 99.0,
                            "high": 100.0,
                            "low": 99.0,
                            "close": 99.0,
                            "change": -1.0,
                            "pct_chg": -1.0,
                            "vol": 11.0,
                            "amount": 21.0,
                        },
                        {
                            "ts_code": "110001.SH",
                            "trade_date": "20260402",
                            "pre_close": 101.0,
                            "open": 102.0,
                            "high": 102.0,
                            "low": 101.0,
                            "close": 102.0,
                            "change": 1.0,
                            "pct_chg": 0.99,
                            "vol": 12.0,
                            "amount": 22.0,
                        },
                        {
                            "ts_code": "110002.SH",
                            "trade_date": "20260402",
                            "pre_close": 99.0,
                            "open": 100.0,
                            "high": 100.0,
                            "low": 99.0,
                            "close": 100.0,
                            "change": 1.0,
                            "pct_chg": 1.01,
                            "vol": 13.0,
                            "amount": 23.0,
                        },
                    ]
                ),
            }
        )

        case_dir = make_case_dir("cb_daily_cross_section_missing_days")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        loader.cache_store.save_time_series(
            loader.source_name,
            "cb_daily_cross_section",
            "20260401",
            client.tables["cb_daily"].loc[client.tables["cb_daily"]["trade_date"] == "20260401"].copy(),
        )

        frame = loader.get_cb_daily_cross_section("2026-04-01", "2026-04-02")

        self.assertEqual(len(frame), 4)
        cb_daily_calls = [call for call in client.calls if call[0] == "cb_daily"]
        self.assertEqual(len(cb_daily_calls), 1)
        self.assertEqual(cb_daily_calls[0][1]["trade_date"], "20260402")
        stats = loader.cache_service.stats_snapshot()
        self.assertEqual(
            int(stats.get("cache_resolution_partial_hit_calls::cb_daily_cross_section", 0)),
            1,
        )
        self.assertEqual(
            int(stats.get("remote_fill_calls::cb_daily_cross_section", 0)),
            1,
        )
        self.assertGreaterEqual(
            int(stats.get("stage_elapsed_ms::cb_daily_cross_section::remote_fetch", 0)),
            1,
        )

    def test_cb_daily_cross_section_can_batch_load_cached_days_without_remote_calls(self) -> None:
        client = FakeTushareClient(
            {
                "trade_cal": pd.DataFrame(
                    [
                        {"exchange": "SSE", "cal_date": "20260401", "is_open": 1, "pretrade_date": "20260331"},
                        {"exchange": "SSE", "cal_date": "20260402", "is_open": 1, "pretrade_date": "20260401"},
                    ]
                ),
                "cb_daily": pd.DataFrame(
                    columns=[
                        "ts_code",
                        "trade_date",
                        "pre_close",
                        "open",
                        "high",
                        "low",
                        "close",
                        "change",
                        "pct_chg",
                        "vol",
                        "amount",
                    ]
                ),
            }
        )

        case_dir = make_case_dir("cb_daily_cross_section_batch_cache")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        loader.CB_DAILY_CROSS_SECTION_BATCH_CACHE_THRESHOLD = 2
        for trade_date, code, close in [
            ("2026-04-01", "110001.SH", 101.0),
            ("2026-04-02", "110002.SH", 102.0),
        ]:
            loader.cache_store.save_time_series(
                loader.source_name,
                "cb_daily_cross_section",
                trade_date.replace("-", ""),
                pd.DataFrame(
                    [
                        {
                            "cb_code": code,
                            "trade_date": trade_date,
                            "pre_close": close - 1.0,
                            "open": close,
                            "high": close,
                            "low": close - 2.0,
                            "close": close,
                            "price_change": 1.0,
                            "pct_change": 1.0,
                            "volume": 10.0,
                            "amount": 20.0,
                            "ytm": pd.NA,
                            "is_tradable": True,
                        }
                    ]
                ),
            )

        frame = loader.get_cb_daily_cross_section("2026-04-01", "2026-04-02")

        self.assertEqual(len(frame), 2)
        self.assertEqual(set(frame["cb_code"]), {"110001.SH", "110002.SH"})
        cb_daily_calls = [call for call in client.calls if call[0] == "cb_daily"]
        self.assertEqual(len(cb_daily_calls), 0)

    def test_cb_daily_cross_section_can_project_columns_from_cached_days(self) -> None:
        client = FakeTushareClient(
            {
                "trade_cal": pd.DataFrame(
                    [
                        {"exchange": "SSE", "cal_date": "20260401", "is_open": 1, "pretrade_date": "20260331"},
                        {"exchange": "SSE", "cal_date": "20260402", "is_open": 1, "pretrade_date": "20260401"},
                    ]
                ),
                "cb_daily": pd.DataFrame(columns=["ts_code", "trade_date"]),
            }
        )

        case_dir = make_case_dir("cb_daily_cross_section_projection")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        loader.CB_DAILY_CROSS_SECTION_BATCH_CACHE_THRESHOLD = 2
        for trade_date, code, close in [
            ("2026-04-01", "110001.SH", 101.0),
            ("2026-04-02", "110002.SH", 102.0),
        ]:
            loader.cache_store.save_time_series(
                loader.source_name,
                "cb_daily_cross_section",
                trade_date.replace("-", ""),
                pd.DataFrame(
                    [
                        {
                            "cb_code": code,
                            "trade_date": trade_date,
                            "pre_close": close - 1.0,
                            "open": close,
                            "high": close,
                            "low": close - 2.0,
                            "close": close,
                            "price_change": 1.0,
                            "pct_change": 1.0,
                            "volume": 10.0,
                            "amount": 20.0,
                            "convert_value": 95.0,
                            "premium_rate": 3.0,
                            "ytm": pd.NA,
                            "is_tradable": True,
                        }
                    ]
                ),
            )

        frame = loader.get_cb_daily_cross_section(
            "2026-04-01",
            "2026-04-02",
            columns=["cb_code", "trade_date", "close", "amount", "ytm"],
        )

        self.assertEqual(
            list(frame.columns),
            ["cb_code", "trade_date", "close", "amount", "ytm"],
        )
        self.assertEqual(len(frame), 2)
        cb_daily_calls = [call for call in client.calls if call[0] == "cb_daily"]
        self.assertEqual(len(cb_daily_calls), 0)

    def test_cb_daily_cross_section_can_build_and_reuse_request_panel_cache(self) -> None:
        client = FakeTushareClient(
            {
                "trade_cal": pd.DataFrame(
                    [
                        {"exchange": "SSE", "cal_date": "20260401", "is_open": 1, "pretrade_date": "20260331"},
                        {"exchange": "SSE", "cal_date": "20260402", "is_open": 1, "pretrade_date": "20260401"},
                    ]
                ),
                "cb_daily": pd.DataFrame(columns=["ts_code", "trade_date"]),
            }
        )

        case_dir = make_case_dir("cb_daily_cross_section_monthly_aggregate")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        loader.CB_DAILY_CROSS_SECTION_BATCH_CACHE_THRESHOLD = 2
        loader.CB_DAILY_CROSS_SECTION_AGGREGATE_MIN_DAYS = 2
        requested_columns = [
            "cb_code",
            "trade_date",
            "close",
            "amount",
            "premium_rate",
            "ytm",
            "convert_value",
            "is_tradable",
        ]
        for trade_date, code, close in [
            ("2026-04-01", "110001.SH", 101.0),
            ("2026-04-02", "110002.SH", 102.0),
        ]:
            loader.cache_store.save_time_series(
                loader.source_name,
                "cb_daily_cross_section",
                trade_date.replace("-", ""),
                pd.DataFrame(
                    [
                        {
                            "cb_code": code,
                            "trade_date": trade_date,
                            "close": close,
                            "amount": 20.0,
                            "premium_rate": 3.0,
                            "ytm": pd.NA,
                            "convert_value": 95.0,
                            "is_tradable": True,
                        }
                    ]
                ),
            )

        first = loader.get_cb_daily_cross_section(
            "2026-04-01",
            "2026-04-02",
            columns=requested_columns,
            aggregate_profile="factor_history_v1",
        )
        aggregate_dir = (
            case_dir
            / "cache"
            / loader.source_name
            / "time_series_aggregate"
            / "cb_daily_cross_section"
            / "factor_history_v1"
        )
        self.assertTrue((aggregate_dir / "202604.csv").exists())
        self.assertTrue((aggregate_dir / "202604.meta.json").exists())

        shutil.rmtree(case_dir / "cache" / loader.source_name / "time_series")
        shutil.rmtree(case_dir / "cache" / loader.source_name / "time_series_aggregate")
        calls_before = len([call for call in client.calls if call[0] == "cb_daily"])
        second = loader.get_cb_daily_cross_section(
            "2026-04-01",
            "2026-04-02",
            columns=requested_columns,
            aggregate_profile="factor_history_v1",
        )
        calls_after = len([call for call in client.calls if call[0] == "cb_daily"])

        self.assertEqual(len(first), 2)
        self.assertEqual(len(second), 2)
        self.assertEqual(calls_before, calls_after)
        self.assertGreaterEqual(
            loader.cache_service.stats_snapshot().get(
                "panel_memory_hit_calls::cb_daily_cross_section::factor_history_v1",
                0,
            ),
            1,
        )
        self.assertGreaterEqual(
            int(
                loader.cache_service.stats_snapshot().get(
                    "cache_resolution_hit_calls::cb_daily_cross_section::factor_history_v1",
                    0,
                )
            ),
            2,
        )
        self.assertGreaterEqual(
            int(
                loader.cache_service.stats_snapshot().get(
                    "stage_elapsed_ms::cb_daily_cross_section::factor_history_v1::request_panel_lookup",
                    0,
                )
            ),
            1,
        )

    def test_cb_daily_cross_section_writeback_invalidates_aggregate_and_request_panel(self) -> None:
        client = FakeTushareClient(
            {
                "trade_cal": pd.DataFrame(
                    [
                        {"exchange": "SSE", "cal_date": "20260401", "is_open": 1, "pretrade_date": "20260331"},
                        {"exchange": "SSE", "cal_date": "20260402", "is_open": 1, "pretrade_date": "20260401"},
                    ]
                ),
                "cb_daily": pd.DataFrame(columns=["ts_code", "trade_date"]),
            }
        )

        case_dir = make_case_dir("cb_daily_cross_section_writeback_invalidation")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        loader.CB_DAILY_CROSS_SECTION_BATCH_CACHE_THRESHOLD = 2
        loader.CB_DAILY_CROSS_SECTION_AGGREGATE_MIN_DAYS = 2
        requested_columns = [
            "cb_code",
            "trade_date",
            "close",
            "amount",
            "premium_rate",
            "ytm",
            "convert_value",
            "is_tradable",
        ]
        for trade_date, code, close in [
            ("2026-04-01", "110001.SH", 101.0),
            ("2026-04-02", "110002.SH", 102.0),
        ]:
            loader.cache_store.save_time_series(
                loader.source_name,
                "cb_daily_cross_section",
                trade_date.replace("-", ""),
                pd.DataFrame(
                    [
                        {
                            "cb_code": code,
                            "trade_date": trade_date,
                            "close": close,
                            "amount": 20.0,
                            "premium_rate": 3.0,
                            "ytm": pd.NA,
                            "convert_value": 95.0,
                            "is_tradable": True,
                        }
                    ]
                ),
            )

        first = loader.get_cb_daily_cross_section(
            "2026-04-01",
            "2026-04-02",
            columns=requested_columns,
            aggregate_profile="factor_history_v1",
        )
        aggregate_dir = (
            case_dir
            / "cache"
            / loader.source_name
            / "time_series_aggregate"
            / "cb_daily_cross_section"
            / "factor_history_v1"
        )
        self.assertTrue((aggregate_dir / "202604.csv").exists())
        self.assertTrue((aggregate_dir / "202604.meta.json").exists())
        self.assertEqual(int(first["ytm"].isna().sum()), 2)

        loader.persist_cb_daily_cross_section_derived_fields(
            pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": pd.Timestamp("2026-04-01"),
                        "ytm": 0.052,
                    }
                ]
            )
        )

        self.assertFalse((aggregate_dir / "202604.csv").exists())
        self.assertFalse((aggregate_dir / "202604.meta.json").exists())

        second = loader.get_cb_daily_cross_section(
            "2026-04-01",
            "2026-04-02",
            columns=requested_columns,
            aggregate_profile="factor_history_v1",
        )

        updated = second.set_index("cb_code")["ytm"]
        self.assertAlmostEqual(float(updated["110001.SH"]), 0.052, places=6)
        self.assertTrue(pd.isna(updated["110002.SH"]))
        self.assertGreaterEqual(
            loader.cache_service.stats_snapshot().get(
                "panel_memory_invalidation_calls::cb_daily_cross_section",
                0,
            ),
            1,
        )

    def test_cb_daily_cross_section_rebuilds_aggregate_when_projection_changes(self) -> None:
        client = FakeTushareClient(
            {
                "trade_cal": pd.DataFrame(
                    [
                        {"exchange": "SSE", "cal_date": "20260401", "is_open": 1, "pretrade_date": "20260331"},
                        {"exchange": "SSE", "cal_date": "20260402", "is_open": 1, "pretrade_date": "20260401"},
                    ]
                ),
                "cb_daily": pd.DataFrame(columns=["ts_code", "trade_date"]),
            }
        )

        case_dir = make_case_dir("cb_daily_cross_section_projection_rebuild")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        loader.CB_DAILY_CROSS_SECTION_BATCH_CACHE_THRESHOLD = 2
        loader.CB_DAILY_CROSS_SECTION_AGGREGATE_MIN_DAYS = 2
        requested_columns = [
            "cb_code",
            "trade_date",
            "close",
            "amount",
            "premium_rate",
            "ytm",
            "convert_value",
            "is_tradable",
        ]

        for trade_date, code, close in [
            ("2026-04-01", "110001.SH", 101.0),
            ("2026-04-02", "110002.SH", 102.0),
        ]:
            loader.cache_store.save_time_series(
                loader.source_name,
                "cb_daily_cross_section",
                trade_date.replace("-", ""),
                pd.DataFrame(
                    [
                        {
                            "cb_code": code,
                            "trade_date": trade_date,
                            "close": close,
                            "amount": 20.0,
                            "premium_rate": 3.0,
                            "ytm": pd.NA,
                            "convert_value": 95.0,
                            "is_tradable": True,
                        }
                    ]
                ),
            )

        loader.cache_store.save_time_series_aggregate(
            loader.source_name,
            "cb_daily_cross_section",
            "factor_history_v1",
            "202604",
            pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": "2026-04-01",
                        "close": 999.0,
                    },
                    {
                        "cb_code": "110002.SH",
                        "trade_date": "2026-04-02",
                        "close": 999.0,
                    },
                ]
            ),
        )
        loader.cache_service.save_time_series_aggregate_metadata(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            payload={
                "covered_trade_days": ["20260401", "20260402"],
                "projection_columns": ["cb_code", "trade_date", "close"],
            },
            standardized_name="cb_daily",
        )

        frame = loader.get_cb_daily_cross_section(
            "2026-04-01",
            "2026-04-02",
            columns=requested_columns,
            aggregate_profile="factor_history_v1",
        )

        by_code = frame.set_index("cb_code")
        self.assertEqual(list(frame.columns), requested_columns)
        self.assertAlmostEqual(float(by_code.loc["110001.SH", "close"]), 101.0, places=6)
        self.assertAlmostEqual(float(by_code.loc["110002.SH", "close"]), 102.0, places=6)
        self.assertAlmostEqual(float(by_code.loc["110001.SH", "amount"]), 20.0, places=6)
        self.assertEqual(len([call for call in client.calls if call[0] == "cb_daily"]), 0)

        metadata = loader.cache_service.load_time_series_aggregate_metadata(
            dataset_name="cb_daily_cross_section",
            profile="factor_history_v1",
            partition_key="202604",
            standardized_name="cb_daily",
            requested_columns=requested_columns,
        )
        self.assertIsNotNone(metadata)
        assert metadata is not None
        self.assertEqual(metadata["projection_columns"], requested_columns)

    def test_cb_equal_weight_index_refetches_when_cached_dates_have_gap(self) -> None:
        client = FakeTushareClient(
            {
                "trade_cal": pd.DataFrame(
                    [
                        {"exchange": "SSE", "cal_date": "20260401", "is_open": 1, "pretrade_date": "20260331"},
                        {"exchange": "SSE", "cal_date": "20260402", "is_open": 1, "pretrade_date": "20260401"},
                        {"exchange": "SSE", "cal_date": "20260403", "is_open": 1, "pretrade_date": "20260402"},
                    ]
                ),
                "cb_daily": pd.DataFrame(
                    [
                        {
                            "ts_code": "110001.SH",
                            "trade_date": "20260401",
                            "pre_close": 100.0,
                            "open": 101.0,
                            "high": 101.0,
                            "low": 100.0,
                            "close": 101.0,
                            "change": 1.0,
                            "pct_chg": 1.0,
                            "vol": 10.0,
                            "amount": 20.0,
                            "bond_value": 95.0,
                            "bond_over_rate": 6.31,
                            "cb_value": 98.0,
                            "cb_over_rate": 3.06,
                        },
                        {
                            "ts_code": "110002.SH",
                            "trade_date": "20260401",
                            "pre_close": 100.0,
                            "open": 99.0,
                            "high": 100.0,
                            "low": 99.0,
                            "close": 99.0,
                            "change": -1.0,
                            "pct_chg": -1.0,
                            "vol": 11.0,
                            "amount": 21.0,
                            "bond_value": 96.0,
                            "bond_over_rate": 3.13,
                            "cb_value": 97.0,
                            "cb_over_rate": 2.06,
                        },
                        {
                            "ts_code": "110001.SH",
                            "trade_date": "20260402",
                            "pre_close": 101.0,
                            "open": 102.0,
                            "high": 102.0,
                            "low": 101.0,
                            "close": 102.0,
                            "change": 1.0,
                            "pct_chg": 0.99,
                            "vol": 12.0,
                            "amount": 22.0,
                            "bond_value": 95.5,
                            "bond_over_rate": 6.81,
                            "cb_value": 99.0,
                            "cb_over_rate": 3.03,
                        },
                        {
                            "ts_code": "110002.SH",
                            "trade_date": "20260402",
                            "pre_close": 99.0,
                            "open": 100.0,
                            "high": 100.0,
                            "low": 99.0,
                            "close": 100.0,
                            "change": 1.0,
                            "pct_chg": 1.01,
                            "vol": 13.0,
                            "amount": 23.0,
                            "bond_value": 96.5,
                            "bond_over_rate": 3.63,
                            "cb_value": 98.0,
                            "cb_over_rate": 2.04,
                        },
                        {
                            "ts_code": "110001.SH",
                            "trade_date": "20260403",
                            "pre_close": 102.0,
                            "open": 103.0,
                            "high": 103.0,
                            "low": 102.0,
                            "close": 103.0,
                            "change": 1.0,
                            "pct_chg": 0.98,
                            "vol": 14.0,
                            "amount": 24.0,
                            "bond_value": 96.0,
                            "bond_over_rate": 7.29,
                            "cb_value": 100.0,
                            "cb_over_rate": 3.0,
                        },
                        {
                            "ts_code": "110002.SH",
                            "trade_date": "20260403",
                            "pre_close": 100.0,
                            "open": 101.0,
                            "high": 101.0,
                            "low": 100.0,
                            "close": 101.0,
                            "change": 1.0,
                            "pct_chg": 1.0,
                            "vol": 15.0,
                            "amount": 25.0,
                            "bond_value": 97.0,
                            "bond_over_rate": 4.12,
                            "cb_value": 99.0,
                            "cb_over_rate": 2.02,
                        },
                    ]
                ),
            }
        )

        case_dir = make_case_dir("cb_equal_weight_sparse_gap")
        loader = DataLoader(cache_dir=case_dir / "cache", client=client)
        loader.cache_store.save_time_series(
            loader.source_name,
            "cb_equal_weight",
            "ALL",
            pd.DataFrame(
                [
                    {
                        "indicator_code": "cb_equal_weight",
                        "trade_date": "2026-04-01",
                        "value": 100.0,
                        "source_table": "cached",
                    },
                    {
                        "indicator_code": "cb_equal_weight",
                        "trade_date": "2026-04-03",
                        "value": 101.0,
                        "source_table": "cached",
                    },
                ]
            ),
        )

        frame = loader.get_cb_equal_weight_index("2026-04-01", "2026-04-03")

        self.assertEqual(len(frame), 3)
        self.assertEqual(
            frame["trade_date"].dt.strftime("%Y-%m-%d").tolist(),
            ["2026-04-01", "2026-04-02", "2026-04-03"],
        )
        self.assertTrue(any(call[0] == "cb_daily" for call in client.calls))


if __name__ == "__main__":
    unittest.main()
