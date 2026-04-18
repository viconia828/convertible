"""Map environment vectors into factor weights for the baseline model."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from strategy_config import ModelParameters, load_strategy_parameters


FACTOR_NAMES = ("value", "carry", "structure", "trend", "stability")
ENV_NAMES = ("equity_strength", "bond_strength", "trend_strength")


_DEFAULT_MODEL_PARAMETERS = load_strategy_parameters().model
DEFAULT_BASE_WEIGHTS: dict[str, float] = dict(_DEFAULT_MODEL_PARAMETERS.base_weights)
DEFAULT_SHIFT_MATRIX: dict[str, dict[str, float]] = {
    env_name: dict(weights)
    for env_name, weights in _DEFAULT_MODEL_PARAMETERS.shift_matrix.items()
}


class WeightMapper:
    """Compute factor weights from the current environment vector.

    `smooth_alpha` is the weight assigned to the new target weights when
    `prev_weights` are provided. A value of `1.0` disables smoothing.
    """

    def __init__(
        self,
        base_weights: Mapping[str, float] | None = None,
        shift_matrix: Mapping[str, Mapping[str, float]] | None = None,
        min_weight: float | None = None,
        max_weight: float | None = None,
        smooth_alpha: float | None = None,
        params: ModelParameters | None = None,
        config_path: str | Path | None = None,
    ) -> None:
        model_params = params or load_strategy_parameters(config_path).model
        resolved_min_weight = (
            model_params.min_weight if min_weight is None else float(min_weight)
        )
        resolved_max_weight = (
            model_params.max_weight if max_weight is None else float(max_weight)
        )
        resolved_smooth_alpha = (
            model_params.smooth_alpha if smooth_alpha is None else float(smooth_alpha)
        )

        if resolved_min_weight < 0:
            raise ValueError("min_weight must be non-negative")
        if resolved_max_weight <= 0:
            raise ValueError("max_weight must be positive")
        if resolved_min_weight > resolved_max_weight:
            raise ValueError("min_weight must be <= max_weight")
        if resolved_min_weight * len(FACTOR_NAMES) > 1.0:
            raise ValueError("min_weight is too large for the number of factors")
        if resolved_max_weight * len(FACTOR_NAMES) < 1.0:
            raise ValueError("max_weight is too small for the number of factors")

        self.min_weight = float(resolved_min_weight)
        self.max_weight = float(resolved_max_weight)
        self.smooth_alpha = self._validate_alpha(resolved_smooth_alpha)
        self.base_weights = self._bounded_normalize(
            self._coerce_factor_vector(
                base_weights or model_params.base_weights,
                "base_weights",
            )
        )
        self.shift_matrix = self._coerce_shift_matrix(
            shift_matrix or model_params.shift_matrix
        )

    def compute(
        self,
        env: Mapping[str, float],
        prev_weights: Mapping[str, float] | None = None,
        smooth_alpha: float | None = None,
    ) -> dict[str, float]:
        """Return bounded, normalized factor weights for the given environment."""

        env_vector = self._coerce_env_vector(env)
        adjustment = {factor: 0.0 for factor in FACTOR_NAMES}
        for env_name in ENV_NAMES:
            deviation = env_vector[env_name] - 0.5
            shift_row = self.shift_matrix[env_name]
            for factor in FACTOR_NAMES:
                adjustment[factor] += deviation * shift_row[factor]

        target = {
            factor: self.base_weights[factor] + adjustment[factor] for factor in FACTOR_NAMES
        }
        bounded = self._bounded_normalize(target)

        alpha = self.smooth_alpha if smooth_alpha is None else self._validate_alpha(smooth_alpha)
        if prev_weights is None or alpha >= 1.0:
            return bounded

        previous = self._bounded_normalize(
            self._coerce_factor_vector(prev_weights, "prev_weights")
        )
        blended = {
            factor: alpha * bounded[factor] + (1.0 - alpha) * previous[factor]
            for factor in FACTOR_NAMES
        }
        return self._bounded_normalize(blended)

    def _coerce_factor_vector(
        self, values: Mapping[str, float], name: str
    ) -> dict[str, float]:
        missing = [factor for factor in FACTOR_NAMES if factor not in values]
        extra = sorted(set(values) - set(FACTOR_NAMES))
        if missing or extra:
            raise ValueError(f"{name} keys mismatch, missing={missing}, extra={extra}")
        return {factor: float(values[factor]) for factor in FACTOR_NAMES}

    def _coerce_env_vector(self, env: Mapping[str, float]) -> dict[str, float]:
        missing = [name for name in ENV_NAMES if name not in env]
        extra = sorted(set(env) - set(ENV_NAMES))
        if missing or extra:
            raise ValueError(f"env keys mismatch, missing={missing}, extra={extra}")
        return {name: float(env[name]) for name in ENV_NAMES}

    def _coerce_shift_matrix(
        self, shift_matrix: Mapping[str, Mapping[str, float]]
    ) -> dict[str, dict[str, float]]:
        missing = [name for name in ENV_NAMES if name not in shift_matrix]
        extra = sorted(set(shift_matrix) - set(ENV_NAMES))
        if missing or extra:
            raise ValueError(f"shift_matrix keys mismatch, missing={missing}, extra={extra}")
        return {
            env_name: self._coerce_factor_vector(shift_matrix[env_name], env_name)
            for env_name in ENV_NAMES
        }

    def _bounded_normalize(self, values: Mapping[str, float]) -> dict[str, float]:
        weights = {
            factor: min(max(float(values[factor]), self.min_weight), self.max_weight)
            for factor in FACTOR_NAMES
        }

        for _ in range(16):
            total = sum(weights.values())
            diff = 1.0 - total
            if abs(diff) < 1e-12:
                break
            if diff > 0:
                eligible = [
                    factor
                    for factor in FACTOR_NAMES
                    if weights[factor] < self.max_weight - 1e-12
                ]
                if not eligible:
                    break
                slack = sum(self.max_weight - weights[factor] for factor in eligible)
                if slack <= 0:
                    break
                for factor in eligible:
                    room = self.max_weight - weights[factor]
                    weights[factor] += diff * room / slack
            else:
                eligible = [
                    factor
                    for factor in FACTOR_NAMES
                    if weights[factor] > self.min_weight + 1e-12
                ]
                if not eligible:
                    break
                slack = sum(weights[factor] - self.min_weight for factor in eligible)
                if slack <= 0:
                    break
                for factor in eligible:
                    room = weights[factor] - self.min_weight
                    weights[factor] += diff * room / slack

            for factor in FACTOR_NAMES:
                weights[factor] = min(max(weights[factor], self.min_weight), self.max_weight)

        final_total = sum(weights.values())
        if final_total <= 0:
            equal_weight = 1.0 / len(FACTOR_NAMES)
            return {factor: equal_weight for factor in FACTOR_NAMES}

        normalized = {factor: weights[factor] / final_total for factor in FACTOR_NAMES}
        if abs(sum(normalized.values()) - 1.0) <= 1e-9:
            return normalized
        return {
            factor: min(max(normalized[factor], self.min_weight), self.max_weight)
            for factor in FACTOR_NAMES
        }

    def _validate_alpha(self, alpha: float) -> float:
        value = float(alpha)
        if not 0.0 <= value <= 1.0:
            raise ValueError("smooth_alpha must be between 0.0 and 1.0")
        return value


def compute_factor_weights(
    env: Mapping[str, float],
    base_weights: Mapping[str, float] | None = None,
    shift_matrix: Mapping[str, Mapping[str, float]] | None = None,
    min_weight: float | None = None,
    max_weight: float | None = None,
    prev_weights: Mapping[str, float] | None = None,
    smooth_alpha: float | None = None,
    config_path: str | Path | None = None,
) -> dict[str, float]:
    """Convenience wrapper that mirrors the doc-level pure-function interface."""

    mapper = WeightMapper(
        base_weights=base_weights,
        shift_matrix=shift_matrix,
        min_weight=min_weight,
        max_weight=max_weight,
        smooth_alpha=smooth_alpha,
        config_path=config_path,
    )
    return mapper.compute(env, prev_weights=prev_weights)
