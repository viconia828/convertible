from __future__ import annotations

import unittest

from strategy_config import load_strategy_parameters


class StrategyConfigTests(unittest.TestCase):
    def test_default_strategy_parameter_file_loads(self) -> None:
        config = load_strategy_parameters()

        self.assertEqual(config.data.source_name, "tushare")
        self.assertEqual(config.env.percentile_window, 252)
        self.assertEqual(config.factor.min_listing_days, 30)
        self.assertEqual(config.model.base_weights["value"], 0.25)
        self.assertEqual(config.exports.factor_max_codes_per_run, 20)

    def test_runtime_overrides_only_affect_current_load(self) -> None:
        base = load_strategy_parameters()
        overridden = load_strategy_parameters(
            overrides={
                "factor": {
                    "min_listing_days": 45,
                },
                "exports": {
                    "factor_max_codes_per_run": 5,
                },
            }
        )

        self.assertEqual(base.factor.min_listing_days, 30)
        self.assertEqual(overridden.factor.min_listing_days, 45)
        self.assertEqual(base.exports.factor_max_codes_per_run, 20)
        self.assertEqual(overridden.exports.factor_max_codes_per_run, 5)


if __name__ == "__main__":
    unittest.main()
