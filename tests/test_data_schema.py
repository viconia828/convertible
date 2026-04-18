from __future__ import annotations

import unittest

import pandas as pd

from data.schema import DataSchema


class DataSchemaTests(unittest.TestCase):
    def test_cb_basic_can_split_fixed_and_mutable_fields(self) -> None:
        raw = pd.DataFrame(
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

        parts = DataSchema.split_by_mutability("cb_basic", raw)

        self.assertIn("issue_size", parts["fixed"].columns)
        self.assertNotIn("remain_size", parts["fixed"].columns)
        self.assertIn("remain_size", parts["mutable"].columns)
        self.assertIn("conv_price", parts["mutable"].columns)

    def test_daily_standardization_adds_is_tradable(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "ts_code": "110001.SH",
                    "trade_date": "20260410",
                    "pre_close": 100,
                    "open": 101,
                    "high": 102,
                    "low": 99,
                    "close": 101,
                    "change": 1,
                    "pct_chg": 1.0,
                    "vol": 12.0,
                    "amount": 34.0,
                },
                {
                    "ts_code": "110001.SH",
                    "trade_date": "20260411",
                    "pre_close": 101,
                    "open": 101,
                    "high": 101,
                    "low": 101,
                    "close": 101,
                    "change": 0,
                    "pct_chg": 0.0,
                    "vol": 0.0,
                    "amount": 0.0,
                },
            ]
        )

        standardized = DataSchema.standardize("cb_daily", raw)

        self.assertTrue(bool(standardized.iloc[0]["is_tradable"]))
        self.assertFalse(bool(standardized.iloc[1]["is_tradable"]))


if __name__ == "__main__":
    unittest.main()
