from __future__ import annotations

import unittest

import pandas as pd

from strategy.portfolio import PortfolioBuilder
from strategy_config import StrategyPortfolioParameters


class StrategyPortfolioTests(unittest.TestCase):
    def test_score_proportional_portfolio_respects_caps_and_top_n(self) -> None:
        builder = PortfolioBuilder(
            params=StrategyPortfolioParameters(
                top_n=2,
                min_names=1,
                weighting_method="score_proportional",
                single_name_max_weight=0.60,
                cash_buffer=0.0,
            )
        )
        scored = pd.DataFrame(
            [
                {"cb_code": "110001.SH", "trade_date": "2026-04-20", "eligible": True, "total_score": 1.8},
                {"cb_code": "110002.SH", "trade_date": "2026-04-20", "eligible": True, "total_score": 1.2},
                {"cb_code": "110003.SH", "trade_date": "2026-04-20", "eligible": True, "total_score": 0.3},
            ]
        )

        result = builder.build(scored)

        self.assertEqual(result.selected_count, 2)
        self.assertEqual(result.eligible_count, 3)
        self.assertAlmostEqual(result.holdings["target_weight"].sum(), 1.0, places=8)
        self.assertTrue(result.holdings["target_weight"].le(0.60 + 1e-9).all())
        self.assertEqual(result.holdings.iloc[0]["cb_code"], "110001.SH")

    def test_equal_scores_fall_back_to_equal_weight(self) -> None:
        builder = PortfolioBuilder(
            params=StrategyPortfolioParameters(
                top_n=3,
                min_names=1,
                weighting_method="score_proportional",
                single_name_max_weight=0.50,
                cash_buffer=0.0,
            )
        )
        scored = pd.DataFrame(
            [
                {"cb_code": "110001.SH", "trade_date": "2026-04-20", "eligible": True, "total_score": 0.5},
                {"cb_code": "110002.SH", "trade_date": "2026-04-20", "eligible": True, "total_score": 0.5},
                {"cb_code": "110003.SH", "trade_date": "2026-04-20", "eligible": True, "total_score": 0.5},
            ]
        )

        result = builder.build(scored)

        self.assertEqual(result.selected_count, 3)
        for value in result.holdings["target_weight"].tolist():
            self.assertAlmostEqual(value, 1.0 / 3.0, places=8)

    def test_underfilled_portfolio_leaves_cash_when_cap_prevents_full_investment(self) -> None:
        builder = PortfolioBuilder(
            params=StrategyPortfolioParameters(
                top_n=2,
                min_names=3,
                weighting_method="equal_weight",
                single_name_max_weight=0.20,
                cash_buffer=0.0,
            )
        )
        scored = pd.DataFrame(
            [
                {"cb_code": "110001.SH", "trade_date": "2026-04-20", "eligible": True, "total_score": 0.8},
                {"cb_code": "110002.SH", "trade_date": "2026-04-20", "eligible": True, "total_score": 0.7},
            ]
        )

        result = builder.build(scored)

        self.assertAlmostEqual(result.holdings["target_weight"].sum(), 0.4, places=8)
        self.assertAlmostEqual(result.cash_weight, 0.6, places=8)
        self.assertTrue(result.notes)

    def test_empty_eligible_pool_returns_empty_portfolio(self) -> None:
        builder = PortfolioBuilder()
        scored = pd.DataFrame(
            [
                {"cb_code": "110001.SH", "trade_date": "2026-04-20", "eligible": False, "total_score": pd.NA},
            ]
        )

        result = builder.build(scored)

        self.assertTrue(result.holdings.empty)
        self.assertEqual(result.selected_count, 0)
        self.assertAlmostEqual(result.cash_weight, 1.0, places=8)


if __name__ == "__main__":
    unittest.main()
