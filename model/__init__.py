"""Baseline factor-weight mapping for the convertible bond slow strategy."""

from .weight_mapper import (
    DEFAULT_BASE_WEIGHTS,
    DEFAULT_SHIFT_MATRIX,
    FACTOR_NAMES,
    WeightMapper,
    compute_factor_weights,
)

__all__ = [
    "DEFAULT_BASE_WEIGHTS",
    "DEFAULT_SHIFT_MATRIX",
    "FACTOR_NAMES",
    "WeightMapper",
    "compute_factor_weights",
]
