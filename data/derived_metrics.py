"""Derived metrics built from raw Tushare datasets."""

from __future__ import annotations

import math

import pandas as pd


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
    rate_lookup = {
        code: frame.sort_values("rate_end_date", kind="stable").reset_index(drop=True)
        for code, frame in cb_rate.groupby("cb_code")
    } if not cb_rate.empty else {}

    estimates: list[float] = []
    for row in cb_daily.itertuples(index=False):
        code = getattr(row, "cb_code")
        trade_date = pd.Timestamp(getattr(row, "trade_date"))
        price = _to_float(getattr(row, "close", math.nan))

        if (
            basic_lookup.empty
            or code not in basic_lookup.index
            or not math.isfinite(price)
            or price <= 0
        ):
            estimates.append(math.nan)
            continue

        basic_row = basic_lookup.loc[code]
        if isinstance(basic_row, pd.DataFrame):
            basic_row = basic_row.iloc[-1]
        rate_frame = rate_lookup.get(code, pd.DataFrame())
        cash_flows, rate_freq = _build_cash_flows(
            trade_date=trade_date,
            basic_row=basic_row,
            rate_frame=rate_frame,
        )
        estimates.append(
            _solve_ytm(
                price=price,
                trade_date=trade_date,
                cash_flows=cash_flows,
                rate_freq=rate_freq,
            )
        )

    return pd.Series(estimates, index=cb_daily.index, dtype="float64")


def _build_cash_flows(
    trade_date: pd.Timestamp,
    basic_row: pd.Series,
    rate_frame: pd.DataFrame,
) -> tuple[list[tuple[pd.Timestamp, float]], int]:
    par_value = _to_float(basic_row.get("par_value", 100.0)) or 100.0
    maturity_date = pd.Timestamp(basic_row.get("maturity_date"))
    if pd.isna(maturity_date) or maturity_date <= trade_date:
        return [], 1

    if not rate_frame.empty:
        valid_rates = rate_frame.loc[rate_frame["rate_end_date"].gt(trade_date)].copy()
    else:
        valid_rates = pd.DataFrame()

    rate_freq = 1
    if not valid_rates.empty and valid_rates["rate_frequency"].notna().any():
        rate_freq = int(valid_rates["rate_frequency"].dropna().iloc[-1])
    else:
        rate_freq = int(_to_float(basic_row.get("pay_per_year", 1)) or 1)
    rate_freq = max(rate_freq, 1)

    cash_flows: list[tuple[pd.Timestamp, float]] = []
    if valid_rates.empty:
        coupon_rate = _to_float(basic_row.get("coupon_rate", math.nan))
        if math.isfinite(coupon_rate):
            coupon_cash = par_value * coupon_rate / 100.0 / rate_freq
            cash_flows.append((maturity_date, coupon_cash + par_value))
        return cash_flows, rate_freq

    for rate_row in valid_rates.itertuples(index=False):
        payment_date = pd.Timestamp(rate_row.rate_end_date)
        if payment_date <= trade_date:
            continue
        coupon_rate = _to_float(getattr(rate_row, "coupon_rate", math.nan))
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


def _solve_ytm(
    price: float,
    trade_date: pd.Timestamp,
    cash_flows: list[tuple[pd.Timestamp, float]],
    rate_freq: int,
) -> float:
    if price <= 0 or not cash_flows:
        return math.nan

    low = -0.95
    high = 1.50
    f_low = _present_value(low, trade_date, cash_flows, rate_freq) - price
    f_high = _present_value(high, trade_date, cash_flows, rate_freq) - price

    expand_count = 0
    while f_low * f_high > 0 and expand_count < 6:
        high *= 2.0
        f_high = _present_value(high, trade_date, cash_flows, rate_freq) - price
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
        f_mid = _present_value(mid, trade_date, cash_flows, rate_freq) - price
        if abs(f_mid) < 1e-8:
            return mid
        if f_low * f_mid <= 0:
            high = mid
            f_high = f_mid
        else:
            low = mid
            f_low = f_mid
    return (low + high) / 2.0


def _present_value(
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


def _to_float(value: object) -> float:
    if value is None:
        return math.nan
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan
