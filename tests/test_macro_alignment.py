from __future__ import annotations

import unittest

import pandas as pd

from env.environment_detector import EnvironmentDetector
from env.macro_alignment import MacroAligner


class MacroAlignerTests(unittest.TestCase):
    def _make_calendar(self) -> pd.DataFrame:
        dates = pd.date_range("2026-04-01", periods=5, freq="B")
        return pd.DataFrame(
            {
                "calendar_date": dates,
                "is_open": [1] * len(dates),
            }
        )

    def _make_macro(self, missing_by_indicator: dict[str, list[str]] | None = None) -> pd.DataFrame:
        missing_by_indicator = missing_by_indicator or {}
        dates = pd.date_range("2026-04-01", periods=5, freq="B")
        indicators = {
            "csi300": [4000, 4010, 4020, 4030, 4040],
            "csi300_amount": [1.0e9, 1.1e9, 1.2e9, 1.3e9, 1.4e9],
            "bond_index": [250, 251, 252, 253, 254],
            "treasury_10y": [1.9, 1.88, 1.87, 1.86, 1.85],
            "credit_spread": [0.90, 0.89, 0.88, 0.87, 0.86],
            "cb_equal_weight": [100, 101, 102, 103, 104],
        }

        rows: list[dict[str, object]] = []
        for indicator, values in indicators.items():
            missing_dates = {pd.Timestamp(day) for day in missing_by_indicator.get(indicator, [])}
            for trade_date, value in zip(dates, values, strict=True):
                if trade_date in missing_dates:
                    continue
                rows.append(
                    {
                        "trade_date": trade_date,
                        "indicator_code": indicator,
                        "value": float(value),
                        "source_table": f"source_{indicator}",
                    }
                )
        return pd.DataFrame(rows)

    def test_align_uses_calendar_and_limited_forward_fill(self) -> None:
        calendar = self._make_calendar()
        macro = self._make_macro(
            {
                "credit_spread": ["2026-04-02"],
                "treasury_10y": ["2026-04-02"],
            }
        )

        aligned, summary = MacroAligner().align(macro, calendar)
        pivot = aligned.pivot_table(
            index="trade_date",
            columns="indicator_code",
            values="value",
            aggfunc="last",
        ).sort_index()

        self.assertEqual(len(pivot), 5)
        self.assertAlmostEqual(float(pivot.loc[pd.Timestamp("2026-04-02"), "credit_spread"]), 0.90)
        self.assertAlmostEqual(float(pivot.loc[pd.Timestamp("2026-04-02"), "treasury_10y"]), 1.90)
        self.assertEqual(summary.filled_days_by_indicator["credit_spread"], 1)
        self.assertEqual(summary.dropped_days, 0)

    def test_align_drops_dates_when_gap_exceeds_limit(self) -> None:
        calendar = self._make_calendar()
        macro = self._make_macro(
            {
                "credit_spread": [
                    "2026-04-02",
                    "2026-04-03",
                    "2026-04-06",
                    "2026-04-07",
                ]
            }
        )

        aligned, summary = MacroAligner().align(macro, calendar)
        pivot = aligned.pivot_table(
            index="trade_date",
            columns="indicator_code",
            values="value",
            aggfunc="last",
        ).sort_index()

        self.assertEqual(len(pivot), 4)
        self.assertNotIn(pd.Timestamp("2026-04-07"), pivot.index)
        self.assertEqual(summary.kept_days, 4)
        self.assertEqual(summary.dropped_days, 1)
        self.assertGreaterEqual(summary.invalid_days_by_indicator["credit_spread"], 1)

    def test_environment_detector_can_compute_after_alignment(self) -> None:
        calendar = pd.DataFrame(
            {
                "calendar_date": pd.date_range("2025-01-01", periods=90, freq="B"),
                "is_open": [1] * 90,
            }
        )
        dates = pd.date_range("2025-01-01", periods=90, freq="B")
        rows: list[dict[str, object]] = []
        for indicator, values in {
            "csi300": range(4000, 4090),
            "csi300_amount": [1e9 + i * 1e7 for i in range(90)],
            "bond_index": [250 + i * 0.1 for i in range(90)],
            "treasury_10y": [2.2 - i * 0.003 for i in range(90)],
            "credit_spread": [1.1 - i * 0.002 for i in range(90)],
            "cb_equal_weight": [100 + i * 0.3 for i in range(90)],
        }.items():
            for idx, trade_date in enumerate(dates):
                if indicator == "credit_spread" and trade_date in {dates[10], dates[11]}:
                    continue
                rows.append(
                    {
                        "trade_date": trade_date,
                        "indicator_code": indicator,
                        "value": float(list(values)[idx]) if not isinstance(values, range) else float(idx + 4000),
                        "source_table": "test",
                    }
                )

        macro = pd.DataFrame(rows)
        detector = EnvironmentDetector()
        result, summary = detector.compute_aligned(macro, calendar)

        self.assertEqual(summary.dropped_days, 0)
        self.assertEqual(len(result), len(dates))
        self.assertTrue(result["equity_strength"].between(0, 1).all())
        self.assertTrue(result["bond_strength"].between(0, 1).all())
        self.assertTrue(result["trend_strength"].between(0, 1).all())


if __name__ == "__main__":
    unittest.main()
