from __future__ import annotations

import unittest

from model.weight_mapper import DEFAULT_BASE_WEIGHTS, WeightMapper, compute_factor_weights


class WeightMapperTests(unittest.TestCase):
    def test_neutral_environment_returns_base_weights(self) -> None:
        mapper = WeightMapper()

        result = mapper.compute(
            {
                "equity_strength": 0.5,
                "bond_strength": 0.5,
                "trend_strength": 0.5,
            }
        )

        self.assertEqual(set(result), set(DEFAULT_BASE_WEIGHTS))
        for factor, expected in DEFAULT_BASE_WEIGHTS.items():
            self.assertAlmostEqual(result[factor], expected, places=8)

    def test_environment_shift_direction_is_correct(self) -> None:
        mapper = WeightMapper()

        equity_trend = mapper.compute(
            {
                "equity_strength": 0.8,
                "bond_strength": 0.2,
                "trend_strength": 0.9,
            }
        )
        bond_defensive = mapper.compute(
            {
                "equity_strength": 0.2,
                "bond_strength": 0.9,
                "trend_strength": 0.2,
            }
        )

        self.assertGreater(equity_trend["trend"], DEFAULT_BASE_WEIGHTS["trend"])
        self.assertLess(equity_trend["value"], DEFAULT_BASE_WEIGHTS["value"])
        self.assertLess(equity_trend["carry"], DEFAULT_BASE_WEIGHTS["carry"])

        self.assertGreater(bond_defensive["value"], DEFAULT_BASE_WEIGHTS["value"])
        self.assertGreater(bond_defensive["carry"], DEFAULT_BASE_WEIGHTS["carry"])
        self.assertLess(bond_defensive["trend"], DEFAULT_BASE_WEIGHTS["trend"])

    def test_weights_remain_bounded_and_normalized(self) -> None:
        mapper = WeightMapper()

        result = mapper.compute(
            {
                "equity_strength": 1.0,
                "bond_strength": 0.0,
                "trend_strength": 1.0,
            }
        )

        self.assertAlmostEqual(sum(result.values()), 1.0, places=8)
        for value in result.values():
            self.assertGreaterEqual(value, 0.05 - 1e-9)
            self.assertLessEqual(value, 0.40 + 1e-9)

    def test_optional_smoothing_blends_with_previous_weights(self) -> None:
        mapper = WeightMapper(smooth_alpha=0.2)
        previous = {
            "value": 0.40,
            "carry": 0.20,
            "structure": 0.15,
            "trend": 0.15,
            "stability": 0.10,
        }

        unsmoothed = mapper.compute(
            {
                "equity_strength": 0.9,
                "bond_strength": 0.1,
                "trend_strength": 0.9,
            }
        )
        smoothed = mapper.compute(
            {
                "equity_strength": 0.9,
                "bond_strength": 0.1,
                "trend_strength": 0.9,
            },
            prev_weights=previous,
        )

        self.assertAlmostEqual(sum(smoothed.values()), 1.0, places=8)
        self.assertGreater(smoothed["trend"], previous["trend"])
        self.assertLess(smoothed["trend"], unsmoothed["trend"])
        self.assertLess(smoothed["value"], previous["value"])
        self.assertGreater(smoothed["value"], unsmoothed["value"])

    def test_function_wrapper_matches_class_interface(self) -> None:
        env = {
            "equity_strength": 0.65,
            "bond_strength": 0.45,
            "trend_strength": 0.55,
        }
        mapper = WeightMapper()

        self.assertEqual(compute_factor_weights(env), mapper.compute(env))

    def test_invalid_environment_keys_raise(self) -> None:
        mapper = WeightMapper()

        with self.assertRaises(ValueError):
            mapper.compute({"equity_strength": 0.5, "bond_strength": 0.5})


if __name__ == "__main__":
    unittest.main()
