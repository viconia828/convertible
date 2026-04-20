"""Five-factor scoring for the convertible bond slow strategy."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from data.derived_metrics import estimate_ytm_series
from strategy_config import FactorParameters, load_strategy_parameters


class FactorEngine:
    """Compute factor scores and apply basic universe filtering."""

    SCORE_COLUMNS = (
        "value_score",
        "carry_score",
        "structure_score",
        "trend_score",
        "stability_score",
    )
    SCORE_COLUMN_BY_FACTOR = {
        "value": "value_score",
        "carry": "carry_score",
        "structure": "structure_score",
        "trend": "trend_score",
        "stability": "stability_score",
    }

    def __init__(
        self,
        params: FactorParameters | None = None,
        config_path: str | Path | None = None,
        premium_center: float | None = None,
        premium_width: float | None = None,
        min_listing_days: int | None = None,
        min_remain_size: float | None = None,
        min_avg_amount_20: float | None = None,
        winsor_lower: float | None = None,
        winsor_upper: float | None = None,
    ) -> None:
        base = params or load_strategy_parameters(config_path).factor
        self.params = FactorParameters(
            export_default_refresh=base.export_default_refresh,
            premium_center=base.premium_center if premium_center is None else float(premium_center),
            premium_width=base.premium_width if premium_width is None else float(premium_width),
            structure_gaussian_decay=base.structure_gaussian_decay,
            min_listing_days=base.min_listing_days
            if min_listing_days is None
            else int(min_listing_days),
            min_remain_size=base.min_remain_size
            if min_remain_size is None
            else float(min_remain_size),
            min_avg_amount_20=base.min_avg_amount_20
            if min_avg_amount_20 is None
            else float(min_avg_amount_20),
            winsor_lower=base.winsor_lower if winsor_lower is None else float(winsor_lower),
            winsor_upper=base.winsor_upper if winsor_upper is None else float(winsor_upper),
            momentum_window=base.momentum_window,
            volatility_window=base.volatility_window,
            volatility_min_periods=base.volatility_min_periods,
            amount_mean_window=base.amount_mean_window,
            amount_mean_min_periods=base.amount_mean_min_periods,
            annualization_days=base.annualization_days,
            zscore_ddof=base.zscore_ddof,
        )

    def compute(
        self,
        as_of_date: object,
        cb_daily: pd.DataFrame,
        cb_basic: pd.DataFrame,
        cb_call: pd.DataFrame | None = None,
        cb_rate: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Compute the five factor scores for the given date."""

        diagnostics = self.compute_with_diagnostics(
            as_of_date=as_of_date,
            cb_daily=cb_daily,
            cb_basic=cb_basic,
            cb_call=cb_call,
            cb_rate=cb_rate,
        )
        if diagnostics.empty:
            return self._empty_result()
        result = diagnostics.loc[diagnostics["eligible"]].copy()
        return result.loc[
            :,
            [
                "cb_code",
                "trade_date",
                "close",
                "premium_rate",
                "ytm",
                "remain_size",
                "amount_mean_20",
                *self.SCORE_COLUMNS,
            ],
        ].reset_index(drop=True)

    def compute_with_diagnostics(
        self,
        as_of_date: object,
        cb_daily: pd.DataFrame,
        cb_basic: pd.DataFrame,
        cb_call: pd.DataFrame | None = None,
        cb_rate: pd.DataFrame | None = None,
        requested_codes: Iterable[str] | None = None,
    ) -> pd.DataFrame:
        """Compute scores plus eligibility diagnostics for the given date."""

        as_of_ts = pd.Timestamp(as_of_date).normalize()
        codes = list(dict.fromkeys(requested_codes or []))
        history = cb_daily.loc[cb_daily["trade_date"].le(as_of_ts)].copy()
        if history.empty:
            return self._empty_result(include_diagnostics=True, requested_codes=codes, as_of_ts=as_of_ts)

        history = history.sort_values(["cb_code", "trade_date"], kind="stable")
        snapshot = self._build_snapshot(
            history=history,
            cb_basic=cb_basic,
            cb_call=cb_call,
            as_of_ts=as_of_ts,
            cb_rate=cb_rate,
        )
        diagnostics = self._score_snapshot(snapshot)
        if codes:
            diagnostics = diagnostics.loc[diagnostics["cb_code"].isin(codes)].copy()
            diagnostics = self._append_missing_requested_diagnostics(
                diagnostics,
                requested_codes=codes,
                as_of_ts=as_of_ts,
            )
        return diagnostics.sort_values(["trade_date", "cb_code"], kind="stable").reset_index(drop=True)

    def compute_panel_with_diagnostics(
        self,
        trade_days: Iterable[object],
        cb_daily: pd.DataFrame,
        cb_basic: pd.DataFrame,
        cb_call: pd.DataFrame | None = None,
        cb_rate: pd.DataFrame | None = None,
        requested_codes: Iterable[str] | None = None,
    ) -> pd.DataFrame:
        """Compute diagnostics for multiple trade days in one panel pass."""

        normalized_days = (
            pd.DatetimeIndex(pd.to_datetime(list(trade_days), errors="coerce"))
            .dropna()
            .normalize()
            .unique()
            .sort_values()
        )
        codes = list(dict.fromkeys(requested_codes or []))
        if len(normalized_days) == 0:
            return pd.concat(
                [
                    self._empty_result(
                        include_diagnostics=True,
                        requested_codes=codes,
                        as_of_ts=pd.Timestamp(day),
                    )
                    for day in normalized_days
                ],
                ignore_index=True,
            ) if codes else self._empty_result(include_diagnostics=True)

        history = cb_daily.loc[
            cb_daily["trade_date"].le(pd.Timestamp(normalized_days.max()))
        ].copy()
        if history.empty:
            if not codes:
                return self._empty_result(include_diagnostics=True)
            return pd.concat(
                [
                    self._empty_result(
                        include_diagnostics=True,
                        requested_codes=codes,
                        as_of_ts=pd.Timestamp(day),
                    )
                    for day in normalized_days
                ],
                ignore_index=True,
            )

        history = history.sort_values(["cb_code", "trade_date"], kind="stable")
        prepared = self._prepare_history_metrics(history)
        snapshot = prepared.loc[
            prepared["trade_date"].isin(normalized_days)
        ].copy()
        if snapshot.empty:
            if not codes:
                return self._empty_result(include_diagnostics=True)
            return pd.concat(
                [
                    self._empty_result(
                        include_diagnostics=True,
                        requested_codes=codes,
                        as_of_ts=pd.Timestamp(day),
                    )
                    for day in normalized_days
                ],
                ignore_index=True,
            )

        snapshot = self._enrich_snapshot_rows(
            snapshot=snapshot,
            cb_basic=cb_basic,
            cb_call=cb_call,
            cb_rate=cb_rate,
        )
        diagnostics = self._score_snapshot(snapshot, group_column="trade_date")
        if codes:
            diagnostics = diagnostics.loc[diagnostics["cb_code"].isin(codes)].copy()
            diagnostics = self._append_missing_requested_panel_diagnostics(
                diagnostics,
                requested_codes=codes,
                trade_days=list(normalized_days),
            )
        return diagnostics.sort_values(["trade_date", "cb_code"], kind="stable").reset_index(drop=True)

    def append_weighted_total_score(
        self,
        frame: pd.DataFrame,
        factor_weights: Mapping[str, float],
        column_name: str = "baseline_total_score",
    ) -> pd.DataFrame:
        """Append one weighted total score column onto a score DataFrame."""

        result = frame.copy()
        total = pd.Series(0.0, index=result.index, dtype="float64")
        for factor, column in self.SCORE_COLUMN_BY_FACTOR.items():
            weight = float(factor_weights[factor])
            total = total + result[column].astype("float64") * weight
        result[column_name] = total
        if "eligible" in result.columns:
            result.loc[~result["eligible"], column_name] = pd.NA
        return result

    def _build_snapshot(
        self,
        history: pd.DataFrame,
        cb_basic: pd.DataFrame,
        cb_call: pd.DataFrame | None,
        as_of_ts: pd.Timestamp,
        cb_rate: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        working = self._prepare_history_metrics(history)
        latest = (
            working.groupby("cb_code", group_keys=False)
            .tail(1)
            .copy()
        )
        latest["trade_date"] = as_of_ts
        return self._enrich_snapshot_rows(
            snapshot=latest,
            cb_basic=cb_basic,
            cb_call=cb_call,
            cb_rate=cb_rate,
        )

    def _prepare_history_metrics(self, history: pd.DataFrame) -> pd.DataFrame:
        working = history.copy()
        grouped = working.groupby("cb_code", group_keys=False)
        working["daily_return"] = grouped["close"].pct_change()
        working["momentum_60"] = grouped["close"].transform(
            lambda series: series / series.shift(self.params.momentum_window) - 1.0
        )
        working["volatility_60"] = grouped["daily_return"].transform(
            lambda series: series.rolling(
                self.params.volatility_window,
                min_periods=self.params.volatility_min_periods,
            ).std()
            * math.sqrt(self.params.annualization_days)
        )
        working["amount_mean_20"] = grouped["amount"].transform(
            lambda series: series.rolling(
                self.params.amount_mean_window,
                min_periods=self.params.amount_mean_min_periods,
            ).mean()
        )
        working["listing_obs"] = grouped.cumcount() + 1
        return working

    def _enrich_snapshot_rows(
        self,
        snapshot: pd.DataFrame,
        cb_basic: pd.DataFrame,
        cb_call: pd.DataFrame | None,
        cb_rate: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        latest = snapshot.copy()
        basic_latest = cb_basic.drop_duplicates(subset=["cb_code"], keep="last").copy()
        latest = latest.merge(
            basic_latest[
                [
                    "cb_code",
                    "remain_size",
                    "list_date",
                    "delist_date",
                    "conv_stop_date",
                ]
            ],
            on="cb_code",
            how="left",
        )

        if "premium_rate" in latest.columns and "convert_value" in latest.columns:
            missing_premium = latest["premium_rate"].isna() & latest["convert_value"].gt(0)
            latest.loc[missing_premium, "premium_rate"] = (
                latest.loc[missing_premium, "close"]
                / latest.loc[missing_premium, "convert_value"]
                - 1.0
            ) * 100.0

        if "ytm" not in latest.columns:
            latest["ytm"] = pd.NA
        missing_ytm = latest["ytm"].isna()
        if missing_ytm.any():
            estimated = estimate_ytm_series(
                cb_daily=latest.loc[missing_ytm, ["cb_code", "trade_date", "close"]].copy(),
                cb_basic=cb_basic,
                cb_rate=cb_rate if cb_rate is not None else pd.DataFrame(),
            )
            latest.loc[missing_ytm, "ytm"] = estimated.to_numpy()

        latest["is_call_announced"] = False
        if cb_call is not None and not cb_call.empty:
            call_type = cb_call["call_type"].fillna("").astype(str)
            call_status = cb_call["call_status"].fillna("").astype(str)
            call_mask = (
                self._contains_any(call_type, ("强赎", "寮鸿祹"))
                & self._contains_any(call_status, ("强赎", "寮鸿祹"))
                & ~self._contains_any(call_status, ("不强赎", "涓嶅己璧", "公告不强赎"))
            )
            called_from = (
                cb_call.loc[call_mask, ["cb_code", "announcement_date"]]
                .dropna(subset=["cb_code", "announcement_date"])
                .sort_values(["cb_code", "announcement_date"], kind="stable")
                .groupby("cb_code", as_index=False)["announcement_date"]
                .min()
                .rename(columns={"announcement_date": "call_announced_from"})
            )
            latest = latest.merge(called_from, on="cb_code", how="left")
            latest["is_call_announced"] = latest["trade_date"].ge(
                latest["call_announced_from"]
            ).fillna(False)

        latest["is_put_triggered"] = False
        latest["is_recently_listed"] = latest["listing_obs"] < self.params.min_listing_days
        latest["is_size_ok"] = latest["remain_size"].fillna(0.0) >= self.params.min_remain_size
        latest["is_amount_ok"] = (
            latest["amount_mean_20"].fillna(0.0) >= self.params.min_avg_amount_20
        )
        latest["is_tradable_now"] = latest["is_tradable"].fillna(False).astype(bool)
        return latest

    def _score_snapshot(
        self,
        snapshot: pd.DataFrame,
        group_column: str | None = None,
    ) -> pd.DataFrame:
        scored = snapshot.copy()
        scored["double_low"] = scored["close"] + scored["premium_rate"]
        scored["value_raw"] = -scored["double_low"]
        scored["carry_raw"] = scored["ytm"]
        scored["structure_raw"] = np.exp(
            -self.params.structure_gaussian_decay
            * ((scored["premium_rate"] - self.params.premium_center) / self.params.premium_width) ** 2
        )
        scored["trend_raw"] = scored["momentum_60"]
        scored["stability_raw"] = scored["volatility_60"]
        scored["has_required_fields"] = scored[
            ["premium_rate", "ytm", "momentum_60", "volatility_60"]
        ].notna().all(axis=1)
        scored["eligible"] = self._eligible_mask(scored)

        for column in self.SCORE_COLUMNS:
            scored[column] = np.nan

        if group_column is None:
            self._assign_cross_section_scores(scored, scored.index[scored["eligible"]].tolist())
        else:
            for _, index in scored.groupby(group_column, sort=True).groups.items():
                group_index = list(index)
                eligible_index = scored.loc[group_index].index[scored.loc[group_index, "eligible"]].tolist()
                self._assign_cross_section_scores(scored, eligible_index)

        scored["exclude_reason"] = scored.apply(self._build_exclude_reason, axis=1)
        return scored.loc[
            :,
            [
                "cb_code",
                "trade_date",
                "close",
                "premium_rate",
                "ytm",
                "remain_size",
                "amount_mean_20",
                *self.SCORE_COLUMNS,
                "eligible",
                "exclude_reason",
                "has_required_fields",
                "is_recently_listed",
                "is_size_ok",
                "is_amount_ok",
                "is_call_announced",
                "is_put_triggered",
                "is_tradable_now",
            ],
        ].copy()

    def _assign_cross_section_scores(
        self,
        scored: pd.DataFrame,
        eligible_index: list[int],
    ) -> None:
        if not eligible_index:
            return
        eligible = scored.loc[eligible_index]
        scored.loc[eligible_index, "value_score"] = self._zscore(
            self._winsorize(eligible["value_raw"])
        ).to_numpy()
        scored.loc[eligible_index, "carry_score"] = self._zscore(
            self._winsorize(eligible["carry_raw"])
        ).to_numpy()
        scored.loc[eligible_index, "structure_score"] = self._percentile_rank(
            eligible["structure_raw"]
        ).to_numpy()
        scored.loc[eligible_index, "trend_score"] = self._percentile_rank(
            eligible["trend_raw"]
        ).to_numpy()
        scored.loc[eligible_index, "stability_score"] = (
            1.0 - self._percentile_rank(eligible["stability_raw"])
        ).to_numpy()

    def _eligible_mask(self, snapshot: pd.DataFrame) -> pd.Series:
        return (
            ~snapshot["is_recently_listed"]
            & snapshot["is_size_ok"]
            & snapshot["is_amount_ok"]
            & ~snapshot["is_call_announced"]
            & ~snapshot["is_put_triggered"]
            & snapshot["is_tradable_now"]
            & snapshot["has_required_fields"]
        )

    def _build_exclude_reason(self, row: pd.Series) -> str:
        reasons: list[str] = []
        if not bool(row.get("has_required_fields", False)):
            reasons.append("missing_required_fields")
        if bool(row.get("is_recently_listed", False)):
            reasons.append("recently_listed")
        if not bool(row.get("is_size_ok", False)):
            reasons.append("remain_size_below_min")
        if not bool(row.get("is_amount_ok", False)):
            reasons.append("amount_below_min")
        if bool(row.get("is_call_announced", False)):
            reasons.append("call_announced")
        if bool(row.get("is_put_triggered", False)):
            reasons.append("put_triggered")
        if not bool(row.get("is_tradable_now", False)):
            reasons.append("not_tradable")
        return "" if not reasons else ",".join(reasons)

    def _append_missing_requested_diagnostics(
        self,
        frame: pd.DataFrame,
        requested_codes: list[str],
        as_of_ts: pd.Timestamp,
    ) -> pd.DataFrame:
        existing = set(frame["cb_code"].dropna().tolist())
        missing = [code for code in requested_codes if code not in existing]
        if not missing:
            return frame
        placeholders = self._empty_result(
            include_diagnostics=True,
            requested_codes=missing,
            as_of_ts=as_of_ts,
        )
        return pd.concat([frame, placeholders], ignore_index=True, sort=False)

    def _append_missing_requested_panel_diagnostics(
        self,
        frame: pd.DataFrame,
        requested_codes: list[str],
        trade_days: list[pd.Timestamp],
    ) -> pd.DataFrame:
        if not requested_codes or not trade_days:
            return frame
        existing = frame.loc[:, ["trade_date", "cb_code"]].drop_duplicates().copy()
        expected = pd.MultiIndex.from_product(
            [pd.DatetimeIndex(trade_days), requested_codes],
            names=["trade_date", "cb_code"],
        ).to_frame(index=False)
        missing = expected.merge(
            existing,
            on=["trade_date", "cb_code"],
            how="left",
            indicator=True,
        )
        missing = missing.loc[missing["_merge"] == "left_only", ["trade_date", "cb_code"]]
        if missing.empty:
            return frame

        placeholders = [
            self._empty_result(
                include_diagnostics=True,
                requested_codes=group["cb_code"].tolist(),
                as_of_ts=pd.Timestamp(trade_date),
            )
            for trade_date, group in missing.groupby("trade_date", sort=True)
        ]
        return pd.concat([frame, *placeholders], ignore_index=True, sort=False)

    def _winsorize(self, series: pd.Series) -> pd.Series:
        valid = series.dropna()
        if valid.empty:
            return series.copy()
        lower = valid.quantile(self.params.winsor_lower)
        upper = valid.quantile(self.params.winsor_upper)
        return series.clip(lower=lower, upper=upper)

    def _zscore(self, series: pd.Series) -> pd.Series:
        valid = series.dropna()
        if valid.empty:
            return pd.Series(np.nan, index=series.index, dtype="float64")
        mean = valid.mean()
        std = valid.std(ddof=self.params.zscore_ddof)
        if pd.isna(std) or std == 0:
            result = pd.Series(0.0, index=series.index, dtype="float64")
            result[series.isna()] = np.nan
            return result
        return (series - mean) / std

    def _percentile_rank(self, series: pd.Series) -> pd.Series:
        valid = series.dropna()
        if valid.empty:
            return pd.Series(np.nan, index=series.index, dtype="float64")
        ranks = valid.rank(method="average", pct=True)
        result = pd.Series(np.nan, index=series.index, dtype="float64")
        result.loc[valid.index] = ranks
        return result

    def _contains_any(
        self,
        series: pd.Series,
        keywords: tuple[str, ...],
    ) -> pd.Series:
        result = pd.Series(False, index=series.index)
        for keyword in keywords:
            result = result | series.str.contains(keyword, regex=False)
        return result

    def _empty_result(
        self,
        include_diagnostics: bool = False,
        requested_codes: list[str] | None = None,
        as_of_ts: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        base_columns = [
            "cb_code",
            "trade_date",
            "close",
            "premium_rate",
            "ytm",
            "remain_size",
            "amount_mean_20",
            *self.SCORE_COLUMNS,
        ]
        if not include_diagnostics:
            return pd.DataFrame(columns=base_columns)

        diagnostics_columns = base_columns + [
            "eligible",
            "exclude_reason",
            "has_required_fields",
            "is_recently_listed",
            "is_size_ok",
            "is_amount_ok",
            "is_call_announced",
            "is_put_triggered",
            "is_tradable_now",
        ]
        if not requested_codes or as_of_ts is None:
            return pd.DataFrame(columns=diagnostics_columns)

        frame = pd.DataFrame({"cb_code": requested_codes})
        frame["trade_date"] = as_of_ts
        for column in diagnostics_columns:
            if column not in frame.columns:
                frame[column] = pd.NA
        frame["eligible"] = False
        frame["exclude_reason"] = "missing_daily_history"
        frame["has_required_fields"] = False
        frame["is_recently_listed"] = False
        frame["is_size_ok"] = False
        frame["is_amount_ok"] = False
        frame["is_call_announced"] = False
        frame["is_put_triggered"] = False
        frame["is_tradable_now"] = False
        return frame.loc[:, diagnostics_columns]
