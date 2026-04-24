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

    def _make_history_with_midlife_bond(
        self,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        as_of = pd.Timestamp("2026-04-22")

        def build_rows(
            code: str,
            periods: int,
            start_close: float,
            end_close: float,
            premium: float,
            ytm: float,
        ) -> pd.DataFrame:
            dates = pd.bdate_range(end=as_of, periods=periods)
            close_series = np.linspace(start_close, end_close, len(dates))
            return pd.DataFrame(
                {
                    "cb_code": code,
                    "trade_date": dates,
                    "close": close_series,
                    "amount": np.full(len(dates), 500.0),
                    "premium_rate": np.full(len(dates), premium),
                    "ytm": np.full(len(dates), ytm),
                    "convert_value": close_series - premium / 2.0,
                    "is_tradable": True,
                }
            )

        cb_daily = pd.concat(
            [
                build_rows("OLD_A", 80, 100.0, 115.0, 10.0, 0.05),
                build_rows("OLD_B", 80, 115.0, 101.0, 28.0, 0.02),
                build_rows("MID_C", 45, 102.0, 108.0, 18.0, 0.035),
                build_rows("NEW_D", 20, 103.0, 107.0, 15.0, 0.03),
            ],
            ignore_index=True,
        )
        cb_basic = pd.DataFrame(
            {
                "cb_code": ["OLD_A", "OLD_B", "MID_C", "NEW_D"],
                "remain_size": [8e8, 8e8, 8e8, 8e8],
            }
        )
        cb_call = pd.DataFrame(
            columns=["cb_code", "call_type", "call_status", "announcement_date", "call_date"]
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

    def test_compute_with_diagnostics_includes_raw_factor_columns(self) -> None:
        cb_daily, cb_basic, cb_call = self._make_history()
        engine = FactorEngine(min_avg_amount_20=200.0)

        diagnostics = engine.compute_with_diagnostics(
            "2026-04-22",
            cb_daily,
            cb_basic,
            cb_call,
        )

        for column in (
            "double_low",
            "value_raw",
            "carry_raw",
            "structure_raw",
            "trend_raw",
            "stability_raw",
            "exclude_reason",
        ):
            self.assertIn(column, diagnostics.columns)
        self.assertGreater(
            float(
                diagnostics.loc[diagnostics["cb_code"] == "CB_B", "double_low"].iloc[0]
            ),
            float(
                diagnostics.loc[diagnostics["cb_code"] == "CB_A", "double_low"].iloc[0]
            ),
        )

    def test_midlife_bond_uses_neutral_trend_score_until_momentum_is_ready(self) -> None:
        cb_daily, cb_basic, cb_call = self._make_history_with_midlife_bond()
        engine = FactorEngine(min_avg_amount_20=200.0)

        diagnostics = engine.compute_with_diagnostics(
            "2026-04-22",
            cb_daily,
            cb_basic,
            cb_call,
        )
        result = engine.compute("2026-04-22", cb_daily, cb_basic, cb_call)

        mid_row = diagnostics.loc[diagnostics["cb_code"] == "MID_C"].iloc[0]
        recent_row = diagnostics.loc[diagnostics["cb_code"] == "NEW_D"].iloc[0]

        self.assertTrue(bool(mid_row["eligible"]))
        self.assertTrue(bool(mid_row["has_required_fields"]))
        self.assertTrue(bool(mid_row["uses_trend_neutral_score"]))
        self.assertTrue(pd.isna(mid_row["trend_raw"]))
        self.assertAlmostEqual(float(mid_row["trend_score"]), 0.5, places=8)
        self.assertEqual(str(mid_row["exclude_reason"]), "")
        self.assertIn("MID_C", set(result["cb_code"]))

        self.assertFalse(bool(recent_row["eligible"]))
        self.assertFalse(bool(recent_row["uses_trend_neutral_score"]))
        self.assertIn("missing_required_fields", str(recent_row["exclude_reason"]))
        self.assertIn("recently_listed", str(recent_row["exclude_reason"]))

    def test_neutral_trend_bonds_do_not_shift_existing_trend_percentiles(self) -> None:
        cb_daily, cb_basic, cb_call = self._make_history_with_midlife_bond()
        engine = FactorEngine(min_avg_amount_20=200.0)

        baseline = engine.compute_with_diagnostics(
            "2026-04-22",
            cb_daily.loc[cb_daily["cb_code"] != "MID_C"].copy(),
            cb_basic,
            cb_call,
        )
        with_midlife = engine.compute_with_diagnostics(
            "2026-04-22",
            cb_daily,
            cb_basic,
            cb_call,
        )

        for code in ("OLD_A", "OLD_B"):
            baseline_score = float(
                baseline.loc[baseline["cb_code"] == code, "trend_score"].iloc[0]
            )
            with_midlife_score = float(
                with_midlife.loc[with_midlife["cb_code"] == code, "trend_score"].iloc[0]
            )
            self.assertAlmostEqual(baseline_score, with_midlife_score, places=8)

    def test_append_weighted_total_score_handles_pd_na_factor_scores(self) -> None:
        engine = FactorEngine()
        frame = pd.DataFrame(
            {
                "cb_code": ["A", "B", "C"],
                "eligible": [True, False, True],
                "value_score": [0.8, pd.NA, 0.1],
                "carry_score": [0.6, pd.NA, 0.2],
                "structure_score": [0.4, pd.NA, pd.NA],
                "trend_score": [0.2, pd.NA, 0.4],
                "stability_score": [0.0, pd.NA, 0.5],
            }
        )
        weights = {
            "value": 0.2,
            "carry": 0.2,
            "structure": 0.2,
            "trend": 0.2,
            "stability": 0.2,
        }

        result = engine.append_weighted_total_score(frame, weights, column_name="total_score")

        self.assertAlmostEqual(float(result.loc[0, "total_score"]), 0.4, places=8)
        self.assertTrue(pd.isna(result.loc[1, "total_score"]))
        self.assertTrue(pd.isna(result.loc[2, "total_score"]))


if __name__ == "__main__":
    unittest.main()
