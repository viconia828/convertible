from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from factor.factor_engine import FactorEngine


class FactorEngineTests(unittest.TestCase):
    def _make_history(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        dates = pd.date_range("2026-01-01", periods=80, freq="B")

        frames: list[pd.DataFrame] = []
        for idx, code in enumerate(["CB_A", "CB_B", "CB_C"]):
            base_close = 100 + idx * 5
            close_series = (
                np.linspace(base_close, base_close + 10, len(dates))
                if code == "CB_A"
                else np.linspace(base_close + 10, base_close - 5, len(dates))
                if code == "CB_B"
                else np.linspace(base_close, base_close + 3, len(dates))
            )
            premium = (
                np.full(len(dates), 5.0)
                if code == "CB_A"
                else np.full(len(dates), 40.0)
                if code == "CB_B"
                else np.full(len(dates), 20.0)
            )
            ytm = (
                np.full(len(dates), 0.05)
                if code == "CB_A"
                else np.full(len(dates), 0.01)
                if code == "CB_B"
                else np.full(len(dates), 0.03)
            )
            amount = (
                np.full(len(dates), 500.0)
                if code != "CB_C"
                else np.full(len(dates), 10.0)
            )
            returns = np.diff(close_series, prepend=close_series[0]) / close_series
            volatility_proxy = np.abs(returns)

            frame = pd.DataFrame(
                {
                    "cb_code": code,
                    "trade_date": dates,
                    "pre_close": np.r_[close_series[0], close_series[:-1]],
                    "open": close_series,
                    "high": close_series,
                    "low": close_series,
                    "close": close_series,
                    "price_change": close_series - np.r_[close_series[0], close_series[:-1]],
                    "pct_change": returns * 100,
                    "volume": np.full(len(dates), 10_000.0),
                    "amount": amount,
                    "bond_value": close_series - 2.0,
                    "bond_premium_rate": np.full(len(dates), 2.0),
                    "convert_value": close_series - premium / 2.0,
                    "premium_rate": premium,
                    "ytm": ytm,
                    "is_tradable": True,
                    "volatility_proxy": volatility_proxy,
                }
            )
            frames.append(frame)

        cb_daily = pd.concat(frames, ignore_index=True)

        cb_basic = pd.DataFrame(
            {
                "cb_code": ["CB_A", "CB_B", "CB_C"],
                "remain_size": [8e8, 8e8, 8e8],
                "list_date": [
                    pd.Timestamp("2025-01-01"),
                    pd.Timestamp("2025-01-01"),
                    pd.Timestamp("2025-01-01"),
                ],
                "delist_date": [pd.NaT, pd.NaT, pd.NaT],
                "conv_stop_date": [pd.NaT, pd.NaT, pd.NaT],
            }
        )

        cb_call = pd.DataFrame(
            {
                "cb_code": ["CB_X"],
                "call_type": ["强赎"],
                "call_status": ["公告强赎"],
                "announcement_date": [pd.Timestamp("2026-03-01")],
                "call_date": [pd.Timestamp("2026-03-15")],
            }
        )
        return cb_daily, cb_basic, cb_call

    def test_factor_directions_and_universe_filter(self) -> None:
        cb_daily, cb_basic, cb_call = self._make_history()
        engine = FactorEngine(min_avg_amount_20=200.0)

        result = engine.compute("2026-04-22", cb_daily, cb_basic, cb_call)

        self.assertEqual(set(result["cb_code"]), {"CB_A", "CB_B"})
        row_a = result.loc[result["cb_code"] == "CB_A"].iloc[0]
        row_b = result.loc[result["cb_code"] == "CB_B"].iloc[0]

        self.assertGreater(float(row_a["value_score"]), float(row_b["value_score"]))
        self.assertGreater(float(row_a["carry_score"]), float(row_b["carry_score"]))
        self.assertGreater(float(row_a["trend_score"]), float(row_b["trend_score"]))
        self.assertGreater(float(row_a["stability_score"]), float(row_b["stability_score"]))

    def test_compute_ignores_future_rows(self) -> None:
        cb_daily, cb_basic, cb_call = self._make_history()
        engine = FactorEngine(min_avg_amount_20=200.0)

        baseline = engine.compute("2026-04-22", cb_daily, cb_basic, cb_call)

        future_rows = cb_daily.loc[cb_daily["trade_date"] > pd.Timestamp("2026-04-22")].copy()
        future_rows.loc[future_rows["cb_code"] == "CB_A", "close"] = 1000.0
        altered = pd.concat(
            [
                cb_daily.loc[cb_daily["trade_date"] <= pd.Timestamp("2026-04-22")].copy(),
                future_rows,
            ],
            ignore_index=True,
        )
        rerun = engine.compute("2026-04-22", altered, cb_basic, cb_call)

        pd.testing.assert_frame_equal(baseline.reset_index(drop=True), rerun.reset_index(drop=True))

    def test_call_announced_bonds_are_filtered_out(self) -> None:
        cb_daily, cb_basic, _ = self._make_history()
        cb_call = pd.DataFrame(
            {
                "cb_code": ["CB_B"],
                "call_type": ["强赎"],
                "call_status": ["公告强赎"],
                "announcement_date": [pd.Timestamp("2026-04-01")],
                "call_date": [pd.NaT],
            }
        )
        engine = FactorEngine(min_avg_amount_20=200.0)

        result = engine.compute("2026-04-22", cb_daily, cb_basic, cb_call)

        self.assertEqual(set(result["cb_code"]), {"CB_A"})


if __name__ == "__main__":
    unittest.main()
