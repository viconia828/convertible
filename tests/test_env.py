from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from env.environment_detector import EnvironmentDetector


class EnvironmentDetectorTests(unittest.TestCase):
    def test_recommended_export_warmup_observation_count_defaults_to_20(self) -> None:
        detector = EnvironmentDetector()

        self.assertEqual(detector.recommended_export_warmup_observation_count(), 20)

    def test_compute_with_warmup_identifies_first_fully_ready_trade_date(self) -> None:
        dates = pd.date_range("2024-01-01", periods=160, freq="B")
        macro_daily = pd.DataFrame(
            {
                "trade_date": list(dates) * 6,
                "indicator_code": (
                    ["csi300"] * len(dates)
                    + ["csi300_amount"] * len(dates)
                    + ["bond_index"] * len(dates)
                    + ["treasury_10y"] * len(dates)
                    + ["credit_spread"] * len(dates)
                    + ["cb_equal_weight"] * len(dates)
                ),
                "value": np.concatenate(
                    [
                        np.linspace(3500, 4500, len(dates)),
                        np.linspace(8e11, 1.2e12, len(dates)),
                        np.linspace(200, 240, len(dates)),
                        np.linspace(2.8, 1.8, len(dates)),
                        np.linspace(1.8, 1.2, len(dates)),
                        np.linspace(100, 140, len(dates)),
                    ]
                ),
                "source_table": ["test"] * len(dates) * 6,
            }
        )

        detector = EnvironmentDetector()
        result = detector.compute_with_warmup(macro_daily)

        self.assertEqual(result.first_fully_ready_trade_date, dates[79])
        readiness = result.readiness.set_index("trade_date")
        self.assertFalse(bool(readiness.loc[dates[78], "fully_ready"]))
        self.assertTrue(bool(readiness.loc[dates[79], "fully_ready"]))

    def test_compute_returns_expected_columns_and_bounds(self) -> None:
        dates = pd.date_range("2024-01-01", periods=320, freq="B")
        macro_daily = pd.DataFrame(
            {
                "trade_date": list(dates) * 6,
                "indicator_code": (
                    ["csi300"] * len(dates)
                    + ["csi300_amount"] * len(dates)
                    + ["bond_index"] * len(dates)
                    + ["treasury_10y"] * len(dates)
                    + ["credit_spread"] * len(dates)
                    + ["cb_equal_weight"] * len(dates)
                ),
                "value": np.concatenate(
                    [
                        np.linspace(3500, 4500, len(dates)),
                        np.linspace(8e11, 1.2e12, len(dates)),
                        np.linspace(200, 240, len(dates)),
                        np.linspace(2.8, 1.8, len(dates)),
                        np.linspace(1.8, 1.2, len(dates)),
                        np.linspace(100, 140, len(dates)),
                    ]
                ),
                "source_table": ["test"] * len(dates) * 6,
            }
        )

        detector = EnvironmentDetector()
        result = detector.compute(macro_daily)

        self.assertEqual(
            list(result.columns),
            ["trade_date", "equity_strength", "bond_strength", "trend_strength"],
        )
        self.assertEqual(len(result), len(dates))
        self.assertTrue(result["equity_strength"].between(0, 1).all())
        self.assertTrue(result["bond_strength"].between(0, 1).all())
        self.assertTrue(result["trend_strength"].between(0, 1).all())

    def test_compute_requires_all_indicators(self) -> None:
        macro_daily = pd.DataFrame(
            {
                "trade_date": [pd.Timestamp("2026-01-01")],
                "indicator_code": ["csi300"],
                "value": [4000.0],
                "source_table": ["test"],
            }
        )

        detector = EnvironmentDetector()
        with self.assertRaises(ValueError):
            detector.compute(macro_daily)


if __name__ == "__main__":
    unittest.main()
