"""Typed loader for the root strategy parameter file."""

from __future__ import annotations

import copy
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


DEFAULT_STRATEGY_CONFIG_PATH = Path(__file__).resolve().parent / "\u7b56\u7565\u53c2\u6570.txt"
USER_EDITABLE_TOP_LEVEL_KEYS = frozenset({"env", "factor", "model"})

DEFAULT_CONFIG_PAYLOAD: dict[str, Any] = {
    "meta": {
        "strategy_name": "\u53ef\u8f6c\u503a\u591a\u56e0\u5b50\u6162\u7b56\u7565",
        "config_version": 1,
    },
    "data": {
        "source_name": "tushare",
        "calendar_exchange": "SSE",
        "tushare_api_url": "http://api.tushare.pro",
        "tushare_timeout": 20,
        "tushare_max_retries": 2,
        "tushare_retry_delay": 1.0,
        "treasury_curve_code": "1001.CB",
        "treasury_curve_type": "0",
        "treasury_curve_term": 10.0,
        "credit_spread_timeout": 30,
    },
    "env": {
        "export_default_refresh": False,
        "percentile_window": 252,
        "percentile_min_periods": 20,
        "equity_ema_span": 20,
        "bond_ema_span": 20,
        "trend_ema_span": 10,
        "annualization_days": 252,
        "neutral_score": 0.5,
        "equity": {
            "momentum_window": 60,
            "above_ma_window": 120,
            "above_ma_min_periods": 20,
            "amount_short_window": 20,
            "amount_short_min_periods": 5,
            "amount_long_window": 60,
            "amount_long_min_periods": 10,
            "amount_ratio_lower": 0.5,
            "amount_ratio_upper": 1.5,
            "momentum_weight": 0.4,
            "above_ma_weight": 0.3,
            "amount_weight": 0.3,
        },
        "bond": {
            "yield_change_window": 60,
            "bond_momentum_window": 20,
            "yield_change_weight": 0.4,
            "credit_spread_weight": 0.3,
            "bond_index_weight": 0.3,
        },
        "trend": {
            "adx_period": 14,
            "adx_scale": 40.0,
            "adx_neutral_value": 20.0,
            "ma_window": 60,
            "ma_min_periods": 20,
            "persist_window": 60,
            "persist_min_periods": 20,
            "vol_window": 20,
            "vol_min_periods": 10,
            "exist_weight": 0.4,
            "persist_weight": 0.4,
            "stable_weight": 0.2,
        },
        "alignment": {
            "csi300": {"max_forward_fill_days": 3, "required": True},
            "csi300_amount": {"max_forward_fill_days": 3, "required": True},
            "bond_index": {"max_forward_fill_days": 3, "required": True},
            "treasury_10y": {"max_forward_fill_days": 3, "required": True},
            "credit_spread": {"max_forward_fill_days": 3, "required": True},
            "cb_equal_weight": {"max_forward_fill_days": 3, "required": True},
        },
    },
    "factor": {
        "export_default_refresh": False,
        "premium_center": 20.0,
        "premium_width": 15.0,
        "structure_gaussian_decay": 0.5,
        "min_listing_days": 30,
        "min_remain_size": 50000000.0,
        "min_avg_amount_20": 200.0,
        "winsor_lower": 0.01,
        "winsor_upper": 0.99,
        "momentum_window": 60,
        "volatility_window": 60,
        "volatility_min_periods": 20,
        "amount_mean_window": 20,
        "amount_mean_min_periods": 5,
        "annualization_days": 252,
        "zscore_ddof": 0,
    },
    "model": {
        "min_weight": 0.05,
        "max_weight": 0.40,
        "smooth_alpha": 1.0,
        "base_weights": {
            "value": 0.25,
            "carry": 0.20,
            "structure": 0.20,
            "trend": 0.20,
            "stability": 0.15,
        },
        "shift_matrix": {
            "equity_strength": {
                "value": -0.05,
                "carry": -0.03,
                "structure": 0.00,
                "trend": 0.06,
                "stability": -0.03,
            },
            "bond_strength": {
                "value": 0.04,
                "carry": 0.06,
                "structure": 0.02,
                "trend": -0.03,
                "stability": -0.02,
            },
            "trend_strength": {
                "value": -0.05,
                "carry": -0.02,
                "structure": 0.00,
                "trend": 0.07,
                "stability": -0.03,
            },
        },
    },
    "exports": {
        "output_dir": "\u5bfc\u51fa\u7ed3\u679c",
        "excel_engine": "openpyxl",
        "timestamp_format": "%Y%m%d_%H%M%S",
        "date_token_format": "%Y%m%d",
        "env_filename_prefix": "\u73af\u5883\u6253\u5206",
        "factor_filename_prefix": "\u56e0\u5b50\u6253\u5206",
        "env_sheet_name": "env_scores",
        "factor_sheet_name": "factor_scores",
        "summary_sheet_name": "run_summary",
        "diagnostics_sheet_name": "filter_diagnostics",
        "env_history_buffer_calendar_days": 550,
        "factor_history_buffer_calendar_days": 550,
        "factor_max_codes_per_run": 20,
    },
}


@dataclass(frozen=True)
class DataParameters:
    source_name: str
    calendar_exchange: str
    tushare_api_url: str
    tushare_timeout: int
    tushare_max_retries: int
    tushare_retry_delay: float
    treasury_curve_code: str
    treasury_curve_type: str
    treasury_curve_term: float
    credit_spread_timeout: int


@dataclass(frozen=True)
class AlignmentRuleParameters:
    max_forward_fill_days: int | None
    required: bool


@dataclass(frozen=True)
class MacroAlignmentParameters:
    rules: dict[str, AlignmentRuleParameters]


@dataclass(frozen=True)
class EnvironmentEquityParameters:
    momentum_window: int
    above_ma_window: int
    above_ma_min_periods: int
    amount_short_window: int
    amount_short_min_periods: int
    amount_long_window: int
    amount_long_min_periods: int
    amount_ratio_lower: float
    amount_ratio_upper: float
    momentum_weight: float
    above_ma_weight: float
    amount_weight: float


@dataclass(frozen=True)
class EnvironmentBondParameters:
    yield_change_window: int
    bond_momentum_window: int
    yield_change_weight: float
    credit_spread_weight: float
    bond_index_weight: float


@dataclass(frozen=True)
class EnvironmentTrendParameters:
    adx_period: int
    adx_scale: float
    adx_neutral_value: float
    ma_window: int
    ma_min_periods: int
    persist_window: int
    persist_min_periods: int
    vol_window: int
    vol_min_periods: int
    exist_weight: float
    persist_weight: float
    stable_weight: float


@dataclass(frozen=True)
class EnvironmentParameters:
    export_default_refresh: bool
    percentile_window: int
    percentile_min_periods: int
    equity_ema_span: int
    bond_ema_span: int
    trend_ema_span: int
    annualization_days: int
    neutral_score: float
    equity: EnvironmentEquityParameters
    bond: EnvironmentBondParameters
    trend: EnvironmentTrendParameters
    alignment: MacroAlignmentParameters


@dataclass(frozen=True)
class FactorParameters:
    export_default_refresh: bool
    premium_center: float
    premium_width: float
    structure_gaussian_decay: float
    min_listing_days: int
    min_remain_size: float
    min_avg_amount_20: float
    winsor_lower: float
    winsor_upper: float
    momentum_window: int
    volatility_window: int
    volatility_min_periods: int
    amount_mean_window: int
    amount_mean_min_periods: int
    annualization_days: int
    zscore_ddof: int


@dataclass(frozen=True)
class ModelParameters:
    base_weights: dict[str, float]
    shift_matrix: dict[str, dict[str, float]]
    min_weight: float
    max_weight: float
    smooth_alpha: float


@dataclass(frozen=True)
class ExportParameters:
    output_dir: str
    excel_engine: str
    timestamp_format: str
    date_token_format: str
    env_filename_prefix: str
    factor_filename_prefix: str
    env_sheet_name: str
    factor_sheet_name: str
    summary_sheet_name: str
    diagnostics_sheet_name: str
    env_history_buffer_calendar_days: int
    factor_history_buffer_calendar_days: int
    factor_max_codes_per_run: int


@dataclass(frozen=True)
class StrategyParameters:
    data: DataParameters
    env: EnvironmentParameters
    factor: FactorParameters
    model: ModelParameters
    exports: ExportParameters
    path: Path
    raw: dict[str, Any]

    @classmethod
    def load(
        cls,
        path: str | Path | None = None,
        overrides: Mapping[str, Any] | None = None,
    ) -> StrategyParameters:
        config_path = Path(path or DEFAULT_STRATEGY_CONFIG_PATH).resolve()
        if not config_path.exists():
            raise FileNotFoundError(f"Strategy parameter file not found: {config_path}")

        text = config_path.read_text(encoding="utf-8-sig")
        file_payload = tomllib.loads(text)
        merged_payload = _deep_merge(
            copy.deepcopy(DEFAULT_CONFIG_PAYLOAD),
            _filter_user_editable_payload(file_payload),
        )
        if overrides:
            merged_payload = _deep_merge(merged_payload, dict(overrides))
        return cls.from_dict(merged_payload, path=config_path)

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
        path: str | Path | None = None,
    ) -> StrategyParameters:
        data = payload["data"]
        env = payload["env"]
        factor = payload["factor"]
        model = payload["model"]
        exports = payload["exports"]

        alignment_rules = {
            name: AlignmentRuleParameters(
                max_forward_fill_days=rule.get("max_forward_fill_days"),
                required=bool(rule["required"]),
            )
            for name, rule in env["alignment"].items()
        }

        return cls(
            data=DataParameters(
                source_name=str(data["source_name"]),
                calendar_exchange=str(data["calendar_exchange"]),
                tushare_api_url=str(data["tushare_api_url"]),
                tushare_timeout=int(data["tushare_timeout"]),
                tushare_max_retries=int(data["tushare_max_retries"]),
                tushare_retry_delay=float(data["tushare_retry_delay"]),
                treasury_curve_code=str(data["treasury_curve_code"]),
                treasury_curve_type=str(data["treasury_curve_type"]),
                treasury_curve_term=float(data["treasury_curve_term"]),
                credit_spread_timeout=int(data["credit_spread_timeout"]),
            ),
            env=EnvironmentParameters(
                export_default_refresh=bool(env.get("export_default_refresh", False)),
                percentile_window=int(env["percentile_window"]),
                percentile_min_periods=int(env["percentile_min_periods"]),
                equity_ema_span=int(env["equity_ema_span"]),
                bond_ema_span=int(env["bond_ema_span"]),
                trend_ema_span=int(env["trend_ema_span"]),
                annualization_days=int(env["annualization_days"]),
                neutral_score=float(env["neutral_score"]),
                equity=EnvironmentEquityParameters(
                    momentum_window=int(env["equity"]["momentum_window"]),
                    above_ma_window=int(env["equity"]["above_ma_window"]),
                    above_ma_min_periods=int(env["equity"]["above_ma_min_periods"]),
                    amount_short_window=int(env["equity"]["amount_short_window"]),
                    amount_short_min_periods=int(env["equity"]["amount_short_min_periods"]),
                    amount_long_window=int(env["equity"]["amount_long_window"]),
                    amount_long_min_periods=int(env["equity"]["amount_long_min_periods"]),
                    amount_ratio_lower=float(env["equity"]["amount_ratio_lower"]),
                    amount_ratio_upper=float(env["equity"]["amount_ratio_upper"]),
                    momentum_weight=float(env["equity"]["momentum_weight"]),
                    above_ma_weight=float(env["equity"]["above_ma_weight"]),
                    amount_weight=float(env["equity"]["amount_weight"]),
                ),
                bond=EnvironmentBondParameters(
                    yield_change_window=int(env["bond"]["yield_change_window"]),
                    bond_momentum_window=int(env["bond"]["bond_momentum_window"]),
                    yield_change_weight=float(env["bond"]["yield_change_weight"]),
                    credit_spread_weight=float(env["bond"]["credit_spread_weight"]),
                    bond_index_weight=float(env["bond"]["bond_index_weight"]),
                ),
                trend=EnvironmentTrendParameters(
                    adx_period=int(env["trend"]["adx_period"]),
                    adx_scale=float(env["trend"]["adx_scale"]),
                    adx_neutral_value=float(env["trend"]["adx_neutral_value"]),
                    ma_window=int(env["trend"]["ma_window"]),
                    ma_min_periods=int(env["trend"]["ma_min_periods"]),
                    persist_window=int(env["trend"]["persist_window"]),
                    persist_min_periods=int(env["trend"]["persist_min_periods"]),
                    vol_window=int(env["trend"]["vol_window"]),
                    vol_min_periods=int(env["trend"]["vol_min_periods"]),
                    exist_weight=float(env["trend"]["exist_weight"]),
                    persist_weight=float(env["trend"]["persist_weight"]),
                    stable_weight=float(env["trend"]["stable_weight"]),
                ),
                alignment=MacroAlignmentParameters(rules=alignment_rules),
            ),
            factor=FactorParameters(
                export_default_refresh=bool(factor.get("export_default_refresh", False)),
                premium_center=float(factor["premium_center"]),
                premium_width=float(factor["premium_width"]),
                structure_gaussian_decay=float(factor["structure_gaussian_decay"]),
                min_listing_days=int(factor["min_listing_days"]),
                min_remain_size=float(factor["min_remain_size"]),
                min_avg_amount_20=float(factor["min_avg_amount_20"]),
                winsor_lower=float(factor["winsor_lower"]),
                winsor_upper=float(factor["winsor_upper"]),
                momentum_window=int(factor["momentum_window"]),
                volatility_window=int(factor["volatility_window"]),
                volatility_min_periods=int(factor["volatility_min_periods"]),
                amount_mean_window=int(factor["amount_mean_window"]),
                amount_mean_min_periods=int(factor["amount_mean_min_periods"]),
                annualization_days=int(factor["annualization_days"]),
                zscore_ddof=int(factor["zscore_ddof"]),
            ),
            model=ModelParameters(
                base_weights={
                    str(key): float(value)
                    for key, value in model["base_weights"].items()
                },
                shift_matrix={
                    str(env_name): {
                        str(factor_name): float(weight)
                        for factor_name, weight in weights.items()
                    }
                    for env_name, weights in model["shift_matrix"].items()
                },
                min_weight=float(model["min_weight"]),
                max_weight=float(model["max_weight"]),
                smooth_alpha=float(model["smooth_alpha"]),
            ),
            exports=ExportParameters(
                output_dir=str(exports["output_dir"]),
                excel_engine=str(exports["excel_engine"]),
                timestamp_format=str(exports["timestamp_format"]),
                date_token_format=str(exports["date_token_format"]),
                env_filename_prefix=str(exports["env_filename_prefix"]),
                factor_filename_prefix=str(exports["factor_filename_prefix"]),
                env_sheet_name=str(exports["env_sheet_name"]),
                factor_sheet_name=str(exports["factor_sheet_name"]),
                summary_sheet_name=str(exports["summary_sheet_name"]),
                diagnostics_sheet_name=str(exports["diagnostics_sheet_name"]),
                env_history_buffer_calendar_days=int(
                    exports["env_history_buffer_calendar_days"]
                ),
                factor_history_buffer_calendar_days=int(
                    exports["factor_history_buffer_calendar_days"]
                ),
                factor_max_codes_per_run=int(exports["factor_max_codes_per_run"]),
            ),
            path=Path(path or DEFAULT_STRATEGY_CONFIG_PATH).resolve(),
            raw=copy.deepcopy(dict(payload)),
        )


def load_strategy_parameters(
    path: str | Path | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> StrategyParameters:
    """Load the root strategy parameter file."""

    return StrategyParameters.load(path=path, overrides=overrides)


def _deep_merge(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _filter_user_editable_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): copy.deepcopy(value)
        for key, value in payload.items()
        if key in USER_EDITABLE_TOP_LEVEL_KEYS
    }
