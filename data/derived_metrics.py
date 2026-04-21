"""Derived metrics built from raw Tushare datasets."""

from __future__ import annotations

from dataclasses import dataclass
import math

import numpy as np
import pandas as pd

_NAT_DAY = np.iinfo("int64").min


@dataclass(frozen=True)
class _CompiledCashFlowSchedule:
    par_value: float
    maturity_day: int
    basic_coupon_rate: float
    basic_rate_freq: int
    payment_days: np.ndarray
    coupon_rates: np.ndarray
    suffix_rate_freq: np.ndarray


def enrich_cb_daily(
    cb_daily: pd.DataFrame,
    cb_basic: pd.DataFrame,
    cb_rate: pd.DataFrame,
) -> pd.DataFrame:
    """Fill derived convertible-bond daily fields such as ytm."""

    if cb_daily.empty:
        return cb_daily.copy()

    enriched = cb_daily.copy()

    if "premium_rate" in enriched.columns and "convert_value" in enriched.columns:
        missing_premium = enriched["premium_rate"].isna() & enriched["convert_value"].gt(0)
        enriched.loc[missing_premium, "premium_rate"] = (
            enriched.loc[missing_premium, "close"]
            / enriched.loc[missing_premium, "convert_value"]
            - 1.0
        ) * 100.0

    if "bond_premium_rate" in enriched.columns and "bond_value" in enriched.columns:
        missing_bond_premium = (
            enriched["bond_premium_rate"].isna() & enriched["bond_value"].gt(0)
        )
        enriched.loc[missing_bond_premium, "bond_premium_rate"] = (
            enriched.loc[missing_bond_premium, "close"]
            / enriched.loc[missing_bond_premium, "bond_value"]
            - 1.0
        ) * 100.0

    ytm_values = estimate_ytm_series(
        cb_daily=enriched,
        cb_basic=cb_basic,
        cb_rate=cb_rate,
    )
    enriched["ytm"] = ytm_values
    return enriched


def estimate_ytm_series(
    cb_daily: pd.DataFrame,
    cb_basic: pd.DataFrame,
    cb_rate: pd.DataFrame,
) -> pd.Series:
    """Estimate annualized yield to maturity from future coupon cash flows."""

    if cb_daily.empty:
        return pd.Series(dtype="float64")

    basic_lookup = (
        cb_basic.drop_duplicates(subset=["cb_code"], keep="last").set_index("cb_code")
        if not cb_basic.empty
        else pd.DataFrame()
    )
    rate_lookup = (
        {
            code: frame.reset_index(drop=True)
            for code, frame in cb_rate.sort_values(
                ["cb_code", "rate_end_date"],
                kind="stable",
            ).groupby("cb_code", sort=False)
        }
        if not cb_rate.empty
        else {}
    )

    estimates = pd.Series(math.nan, index=cb_daily.index, dtype="float64")
    if basic_lookup.empty:
        return estimates

    for code, code_frame in cb_daily.groupby("cb_code", sort=False):
        if code not in basic_lookup.index:
            continue

        basic_row = basic_lookup.loc[code]
        if isinstance(basic_row, pd.DataFrame):
            basic_row = basic_row.iloc[-1]
        schedule = _compile_cash_flow_schedule(
            basic_row=basic_row,
            rate_frame=rate_lookup.get(code, pd.DataFrame()),
        )
        estimates.loc[code_frame.index] = _estimate_code_ytm_series(
            code_frame=code_frame,
            schedule=schedule,
        )

    return estimates


def _compile_cash_flow_schedule(
    basic_row: pd.Series,
    rate_frame: pd.DataFrame,
) -> _CompiledCashFlowSchedule:
    par_value = _finite_or_default(_to_float(basic_row.get("par_value", 100.0)), 100.0)
    maturity_day = _timestamp_to_day_number(basic_row.get("maturity_date"))
    basic_coupon_rate = _to_float(basic_row.get("coupon_rate", math.nan))
    basic_rate_freq = _normalize_rate_frequency(_to_float(basic_row.get("pay_per_year", 1)))

    if rate_frame.empty:
        payment_days = np.empty(0, dtype="int64")
        coupon_rates = np.empty(0, dtype="float64")
        suffix_rate_freq = np.empty(0, dtype="float64")
    else:
        schedule_frame = (
            rate_frame.loc[:, ["rate_end_date", "coupon_rate", "rate_frequency"]]
            .dropna(subset=["rate_end_date"])
            .reset_index(drop=True)
        )
        payment_days = _series_to_day_numbers(schedule_frame["rate_end_date"])
        coupon_rates = pd.to_numeric(
            schedule_frame["coupon_rate"],
            errors="coerce",
        ).to_numpy(dtype="float64", copy=False)
        rate_frequencies = pd.to_numeric(
            schedule_frame["rate_frequency"],
            errors="coerce",
        ).to_numpy(dtype="float64", copy=False)
        suffix_rate_freq = _build_suffix_rate_frequency(rate_frequencies)

    return _CompiledCashFlowSchedule(
        par_value=par_value,
        maturity_day=maturity_day,
        basic_coupon_rate=basic_coupon_rate,
        basic_rate_freq=basic_rate_freq,
        payment_days=payment_days,
        coupon_rates=coupon_rates,
        suffix_rate_freq=suffix_rate_freq,
    )


def _estimate_code_ytm_series(
    code_frame: pd.DataFrame,
    schedule: _CompiledCashFlowSchedule,
) -> np.ndarray:
    trade_days = _series_to_day_numbers(code_frame["trade_date"])
    prices = pd.to_numeric(code_frame["close"], errors="coerce").to_numpy(
        dtype="float64",
        copy=False,
    )
    estimates = np.full(len(code_frame), math.nan, dtype="float64")
    if len(code_frame) == 0:
        return estimates

    future_indices = (
        np.searchsorted(schedule.payment_days, trade_days, side="right")
        if schedule.payment_days.size
        else np.zeros(len(code_frame), dtype="int64")
    )
    previous_estimate = math.nan
    for index, trade_day in enumerate(trade_days):
        price = prices[index]
        if trade_day == _NAT_DAY or not math.isfinite(price) or price <= 0:
            continue

        cash_flows, payment_exponents, rate_freq = _build_cash_flows(
            trade_day=trade_day,
            schedule=schedule,
            future_index=int(future_indices[index]),
        )
        estimate = _solve_ytm(
            price=price,
            cash_flows=cash_flows,
            payment_exponents=payment_exponents,
            rate_freq=rate_freq,
            initial_guess=previous_estimate,
        )
        estimates[index] = estimate
        if math.isfinite(estimate):
            previous_estimate = estimate
    return estimates


def _build_cash_flows(
    trade_day: int,
    schedule: _CompiledCashFlowSchedule,
    future_index: int,
) -> tuple[np.ndarray, np.ndarray, int]:
    if schedule.maturity_day == _NAT_DAY or schedule.maturity_day <= trade_day:
        return np.empty(0, dtype="float64"), np.empty(0, dtype="float64"), 1

    if future_index < schedule.payment_days.size:
        rate_freq = _resolve_rate_frequency(schedule, future_index)
        payment_days = schedule.payment_days[future_index:]
        coupon_rates = schedule.coupon_rates[future_index:]
        valid_coupon_mask = np.isfinite(coupon_rates)
        if valid_coupon_mask.any():
            payment_days = payment_days[valid_coupon_mask]
            coupon_rates = coupon_rates[valid_coupon_mask]
            cash_flows = schedule.par_value * coupon_rates / 100.0 / rate_freq
            if schedule.maturity_day <= payment_days[-1]:
                cash_flows = cash_flows.copy()
                cash_flows[-1] += schedule.par_value
            else:
                payment_days = np.concatenate(
                    [payment_days, np.array([schedule.maturity_day], dtype="int64")]
                )
                cash_flows = np.concatenate(
                    [cash_flows, np.array([schedule.par_value], dtype="float64")]
                )
            return cash_flows, _payment_exponents(payment_days, trade_day, rate_freq), rate_freq
        return np.empty(0, dtype="float64"), np.empty(0, dtype="float64"), rate_freq

    coupon_rate = schedule.basic_coupon_rate
    if not math.isfinite(coupon_rate):
        return np.empty(0, dtype="float64"), np.empty(0, dtype="float64"), schedule.basic_rate_freq
    coupon_cash = schedule.par_value * coupon_rate / 100.0 / schedule.basic_rate_freq
    payment_days = np.array([schedule.maturity_day], dtype="int64")
    cash_flows = np.array([coupon_cash + schedule.par_value], dtype="float64")
    return (
        cash_flows,
        _payment_exponents(payment_days, trade_day, schedule.basic_rate_freq),
        schedule.basic_rate_freq,
    )


def _solve_ytm(
    price: float,
    cash_flows: np.ndarray,
    payment_exponents: np.ndarray,
    rate_freq: int,
    initial_guess: float = math.nan,
) -> float:
    if price <= 0 or cash_flows.size == 0:
        return math.nan

    if cash_flows.size == 1 and payment_exponents[0] > 0:
        return _solve_single_cash_flow_ytm(
            price=price,
            final_cash=float(cash_flows[0]),
            payment_exponent=float(payment_exponents[0]),
            rate_freq=rate_freq,
        )

    newton_guess = initial_guess
    if not math.isfinite(newton_guess):
        newton_guess = _approximate_terminal_yield(
            price=price,
            cash_flows=cash_flows,
            payment_exponents=payment_exponents,
            rate_freq=rate_freq,
        )
    newton_estimate = _solve_ytm_newton(
        price=price,
        cash_flows=cash_flows,
        payment_exponents=payment_exponents,
        rate_freq=rate_freq,
        initial_guess=newton_guess,
    )
    if math.isfinite(newton_estimate):
        return newton_estimate

    low = -0.95
    high = 1.50
    f_low = _present_value(low, cash_flows, payment_exponents, rate_freq) - price
    f_high = _present_value(high, cash_flows, payment_exponents, rate_freq) - price

    expand_count = 0
    while f_low * f_high > 0 and expand_count < 6:
        high *= 2.0
        f_high = _present_value(high, cash_flows, payment_exponents, rate_freq) - price
        expand_count += 1

    if f_low * f_high > 0:
        return _approximate_terminal_yield(
            price=price,
            cash_flows=cash_flows,
            payment_exponents=payment_exponents,
            rate_freq=rate_freq,
        )

    for _ in range(64):
        mid = (low + high) / 2.0
        f_mid = _present_value(mid, cash_flows, payment_exponents, rate_freq) - price
        if abs(f_mid) < 1e-8:
            return mid
        if f_low * f_mid <= 0:
            high = mid
        else:
            low = mid
            f_low = f_mid
    return (low + high) / 2.0


def _present_value(
    annual_yield: float,
    cash_flows: np.ndarray,
    payment_exponents: np.ndarray,
    rate_freq: int,
) -> float:
    period_rate = 1.0 + annual_yield / max(rate_freq, 1)
    if period_rate <= 0:
        return math.inf
    return float(np.sum(cash_flows / np.power(period_rate, payment_exponents)))


def _present_value_and_derivative(
    annual_yield: float,
    cash_flows: np.ndarray,
    payment_exponents: np.ndarray,
    rate_freq: int,
) -> tuple[float, float]:
    normalized_rate_freq = max(rate_freq, 1)
    period_rate = 1.0 + annual_yield / normalized_rate_freq
    if period_rate <= 0:
        return math.inf, math.nan

    discount = np.power(period_rate, payment_exponents)
    pv = np.sum(cash_flows / discount)
    derivative = np.sum(
        (-cash_flows * payment_exponents / normalized_rate_freq)
        / (discount * period_rate)
    )
    return float(pv), float(derivative)


def _solve_ytm_newton(
    price: float,
    cash_flows: np.ndarray,
    payment_exponents: np.ndarray,
    rate_freq: int,
    initial_guess: float,
) -> float:
    if not math.isfinite(initial_guess):
        return math.nan

    current = initial_guess
    for _ in range(8):
        pv, derivative = _present_value_and_derivative(
            annual_yield=current,
            cash_flows=cash_flows,
            payment_exponents=payment_exponents,
            rate_freq=rate_freq,
        )
        diff = pv - price
        if abs(diff) < 1e-8:
            return current
        if not math.isfinite(derivative) or derivative == 0:
            return math.nan
        next_estimate = current - diff / derivative
        if not math.isfinite(next_estimate) or next_estimate <= -0.999999:
            return math.nan
        if abs(next_estimate - current) < 1e-10:
            return next_estimate
        current = next_estimate
    final_pv = _present_value(current, cash_flows, payment_exponents, rate_freq)
    if abs(final_pv - price) < 1e-6:
        return current
    return math.nan


def _solve_single_cash_flow_ytm(
    price: float,
    final_cash: float,
    payment_exponent: float,
    rate_freq: int,
) -> float:
    if price <= 0 or final_cash <= 0 or payment_exponent <= 0:
        return math.nan
    period_rate = (final_cash / price) ** (1.0 / payment_exponent)
    return max(rate_freq, 1) * (period_rate - 1.0)


def _approximate_terminal_yield(
    price: float,
    cash_flows: np.ndarray,
    payment_exponents: np.ndarray,
    rate_freq: int,
) -> float:
    terminal_years = max(
        float(payment_exponents[-1]) / max(rate_freq, 1),
        0.01,
    )
    total_cash = float(np.sum(cash_flows))
    if total_cash <= 0 or price <= 0:
        return math.nan
    return (total_cash / price) ** (1.0 / terminal_years) - 1.0


def _payment_exponents(
    payment_days: np.ndarray,
    trade_day: int,
    rate_freq: int,
) -> np.ndarray:
    normalized_rate_freq = max(rate_freq, 1)
    payment_years = np.maximum(payment_days - trade_day, 0.0) / 365.0
    return payment_years * normalized_rate_freq


def _resolve_rate_frequency(
    schedule: _CompiledCashFlowSchedule,
    future_index: int,
) -> int:
    if future_index < schedule.suffix_rate_freq.size:
        rate_freq = schedule.suffix_rate_freq[future_index]
        if math.isfinite(rate_freq) and rate_freq > 0:
            return int(rate_freq)
    return schedule.basic_rate_freq


def _build_suffix_rate_frequency(rate_frequencies: np.ndarray) -> np.ndarray:
    suffix = np.full(len(rate_frequencies), np.nan, dtype="float64")
    latest = math.nan
    for index in range(len(rate_frequencies) - 1, -1, -1):
        rate_freq = rate_frequencies[index]
        if math.isfinite(rate_freq) and rate_freq > 0:
            latest = rate_freq
        suffix[index] = latest
    return suffix


def _series_to_day_numbers(values: object) -> np.ndarray:
    if isinstance(values, pd.Series) and pd.api.types.is_datetime64_any_dtype(values):
        return values.to_numpy(dtype="datetime64[D]").astype("int64", copy=False)
    if isinstance(values, pd.DatetimeIndex):
        return values.to_numpy(dtype="datetime64[D]").astype("int64", copy=False)
    if isinstance(values, pd.Timestamp):
        return np.array([values.to_datetime64()], dtype="datetime64[ns]").astype(
            "datetime64[D]"
        ).astype("int64", copy=False)
    timestamps = pd.to_datetime(values, errors="coerce")
    return timestamps.to_numpy(dtype="datetime64[D]").astype("int64", copy=False)


def _timestamp_to_day_number(value: object) -> int:
    if isinstance(value, pd.Timestamp):
        return int(
            np.array([value.to_datetime64()], dtype="datetime64[ns]")
            .astype("datetime64[D]")
            .astype("int64", copy=False)[0]
        )
    return int(_series_to_day_numbers([value])[0])


def _finite_or_default(value: float, default: float) -> float:
    return value if math.isfinite(value) else default


def _normalize_rate_frequency(value: float) -> int:
    if not math.isfinite(value) or value <= 0:
        return 1
    return max(int(value), 1)


def _to_float(value: object) -> float:
    if value is None:
        return math.nan
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan
