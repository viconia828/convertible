from __future__ import annotations

import math
import unittest

import numpy as np
import pandas as pd

from data.derived_metrics import estimate_ytm_series


class DerivedMetricsTests(unittest.TestCase):
    def test_estimate_ytm_series_matches_legacy_reference(self) -> None:
        cb_daily = pd.DataFrame(
            {
                "cb_code": ["CB_A", "CB_A", "CB_A", "CB_B", "CB_B"],
                "trade_date": pd.to_datetime(
                    [
                        "2026-03-31",
                        "2026-04-01",
                        "2026-07-01",
                        "2026-06-01",
                        "2027-06-01",
                    ]
                ),
                "close": [100.2, 100.0, 98.5, 100.0, 100.0],
            }
        )
        cb_basic = pd.DataFrame(
            {
                "cb_code": ["CB_A", "CB_B"],
                "par_value": [100.0, 100.0],
                "maturity_date": pd.to_datetime(["2027-04-01", "2027-06-01"]),
                "coupon_rate": [5.0, 3.0],
                "pay_per_year": [1, 1],
            }
        )
        cb_rate = pd.DataFrame(
            {
                "cb_code": ["CB_A", "CB_A"],
                "rate_frequency": [1, 1],
                "rate_start_date": pd.to_datetime(["2025-04-02", "2026-04-02"]),
                "rate_end_date": pd.to_datetime(["2026-04-01", "2027-04-01"]),
                "coupon_rate": [5.0, 5.0],
            }
        )

        expected = _legacy_estimate_ytm_series(cb_daily, cb_basic, cb_rate)
        actual = estimate_ytm_series(cb_daily, cb_basic, cb_rate)

        np.testing.assert_allclose(
            actual.to_numpy(dtype="float64"),
            expected.to_numpy(dtype="float64"),
            rtol=1e-8,
            atol=1e-8,
            equal_nan=True,
        )

    def test_estimate_ytm_series_respects_same_day_coupon_cutoff(self) -> None:
        trade_date_before_coupon = pd.Timestamp("2026-03-31")
        payment_dates = [pd.Timestamp("2026-04-01"), pd.Timestamp("2027-04-01")]
        payment_cash = [5.0, 105.0]
        target_yield = 0.05
        price_before_coupon = sum(
            cash
            / (1.0 + target_yield)
            ** ((payment_date - trade_date_before_coupon).days / 365.0)
            for payment_date, cash in zip(payment_dates, payment_cash)
        )
        cb_daily = pd.DataFrame(
            {
                "cb_code": ["CB_A", "CB_A"],
                "trade_date": pd.to_datetime(["2026-03-31", "2026-04-01"]),
                "close": [price_before_coupon, 100.0],
            }
        )
        cb_basic = pd.DataFrame(
            {
                "cb_code": ["CB_A"],
                "par_value": [100.0],
                "maturity_date": pd.to_datetime(["2027-04-01"]),
                "coupon_rate": [5.0],
                "pay_per_year": [1],
            }
        )
        cb_rate = pd.DataFrame(
            {
                "cb_code": ["CB_A", "CB_A"],
                "rate_frequency": [1, 1],
                "rate_start_date": pd.to_datetime(["2025-04-02", "2026-04-02"]),
                "rate_end_date": pd.to_datetime(["2026-04-01", "2027-04-01"]),
                "coupon_rate": [5.0, 5.0],
            }
        )

        result = estimate_ytm_series(cb_daily, cb_basic, cb_rate)

        self.assertAlmostEqual(float(result.iloc[0]), 0.05, places=6)
        self.assertAlmostEqual(float(result.iloc[1]), 0.05, places=6)


def _legacy_estimate_ytm_series(
    cb_daily: pd.DataFrame,
    cb_basic: pd.DataFrame,
    cb_rate: pd.DataFrame,
) -> pd.Series:
    if cb_daily.empty:
        return pd.Series(dtype="float64")

    basic_lookup = (
        cb_basic.drop_duplicates(subset=["cb_code"], keep="last").set_index("cb_code")
        if not cb_basic.empty
        else pd.DataFrame()
    )
    rate_lookup = (
        {
            code: frame.sort_values("rate_end_date", kind="stable").reset_index(drop=True)
            for code, frame in cb_rate.groupby("cb_code")
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
        rate_frame = rate_lookup.get(code, pd.DataFrame())

        code_estimates: list[float] = []
        for row in code_frame.loc[:, ["trade_date", "close"]].itertuples(index=False):
            trade_date = pd.Timestamp(row.trade_date)
            price = _legacy_to_float(getattr(row, "close", math.nan))
            if not math.isfinite(price) or price <= 0:
                code_estimates.append(math.nan)
                continue

            cash_flows, rate_freq = _legacy_build_cash_flows(
                trade_date=trade_date,
                basic_row=basic_row,
                rate_frame=rate_frame,
            )
            code_estimates.append(
                _legacy_solve_ytm(
                    price=price,
                    trade_date=trade_date,
                    cash_flows=cash_flows,
                    rate_freq=rate_freq,
                )
            )

        estimates.loc[code_frame.index] = code_estimates

    return estimates


def _legacy_build_cash_flows(
    trade_date: pd.Timestamp,
    basic_row: pd.Series,
    rate_frame: pd.DataFrame,
) -> tuple[list[tuple[pd.Timestamp, float]], int]:
    par_value = _legacy_to_float(basic_row.get("par_value", 100.0))
    if not math.isfinite(par_value):
        par_value = 100.0
    maturity_date = pd.Timestamp(basic_row.get("maturity_date"))
    if pd.isna(maturity_date) or maturity_date <= trade_date:
        return [], 1

    if not rate_frame.empty:
        valid_rates = rate_frame.loc[rate_frame["rate_end_date"].gt(trade_date)]
    else:
        valid_rates = pd.DataFrame()

    rate_freq = 1
    if not valid_rates.empty and valid_rates["rate_frequency"].notna().any():
        rate_freq = int(valid_rates["rate_frequency"].dropna().iloc[-1])
    else:
        pay_per_year = _legacy_to_float(basic_row.get("pay_per_year", 1))
        rate_freq = int(pay_per_year) if math.isfinite(pay_per_year) and pay_per_year > 0 else 1
    rate_freq = max(rate_freq, 1)

    cash_flows: list[tuple[pd.Timestamp, float]] = []
    if valid_rates.empty:
        coupon_rate = _legacy_to_float(basic_row.get("coupon_rate", math.nan))
        if math.isfinite(coupon_rate):
            coupon_cash = par_value * coupon_rate / 100.0 / rate_freq
            cash_flows.append((maturity_date, coupon_cash + par_value))
        return cash_flows, rate_freq

    for rate_row in valid_rates.itertuples(index=False):
        payment_date = pd.Timestamp(rate_row.rate_end_date)
        if payment_date <= trade_date:
            continue
        coupon_rate = _legacy_to_float(getattr(rate_row, "coupon_rate", math.nan))
        if not math.isfinite(coupon_rate):
            continue
        coupon_cash = par_value * coupon_rate / 100.0 / rate_freq
        cash_flows.append((payment_date, coupon_cash))

    if not cash_flows:
        return [], rate_freq

    last_payment_date, last_cash = cash_flows[-1]
    if maturity_date <= last_payment_date:
        cash_flows[-1] = (last_payment_date, last_cash + par_value)
    else:
        cash_flows.append((maturity_date, par_value))
    return cash_flows, rate_freq


def _legacy_solve_ytm(
    price: float,
    trade_date: pd.Timestamp,
    cash_flows: list[tuple[pd.Timestamp, float]],
    rate_freq: int,
) -> float:
    if price <= 0 or not cash_flows:
        return math.nan

    low = -0.95
    high = 1.50
    f_low = _legacy_present_value(low, trade_date, cash_flows, rate_freq) - price
    f_high = _legacy_present_value(high, trade_date, cash_flows, rate_freq) - price

    expand_count = 0
    while f_low * f_high > 0 and expand_count < 6:
        high *= 2.0
        f_high = _legacy_present_value(high, trade_date, cash_flows, rate_freq) - price
        expand_count += 1

    if f_low * f_high > 0:
        terminal_years = max(
            (cash_flows[-1][0] - trade_date).days / 365.0,
            0.01,
        )
        total_cash = sum(cash for _, cash in cash_flows)
        if total_cash <= 0:
            return math.nan
        return (total_cash / price) ** (1.0 / terminal_years) - 1.0

    for _ in range(80):
        mid = (low + high) / 2.0
        f_mid = _legacy_present_value(mid, trade_date, cash_flows, rate_freq) - price
        if abs(f_mid) < 1e-8:
            return mid
        if f_low * f_mid <= 0:
            high = mid
        else:
            low = mid
            f_low = f_mid
    return (low + high) / 2.0


def _legacy_present_value(
    annual_yield: float,
    trade_date: pd.Timestamp,
    cash_flows: list[tuple[pd.Timestamp, float]],
    rate_freq: int,
) -> float:
    period_rate = 1.0 + annual_yield / max(rate_freq, 1)
    if period_rate <= 0:
        return math.inf

    pv = 0.0
    for payment_date, cash in cash_flows:
        years = max((payment_date - trade_date).days / 365.0, 0.0)
        exponent = years * max(rate_freq, 1)
        pv += cash / (period_rate**exponent)
    return pv


def _legacy_to_float(value: object) -> float:
    if value is None:
        return math.nan
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


if __name__ == "__main__":
    unittest.main()
