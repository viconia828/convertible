"""Portfolio construction helpers for the strategy module."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from strategy_config import (
    StrategyPortfolioParameters,
    load_strategy_parameters,
)


@dataclass(frozen=True)
class PortfolioConstructionResult:
    """Result returned by the portfolio builder."""

    holdings: pd.DataFrame
    eligible_count: int
    selected_count: int
    cash_weight: float
    notes: tuple[str, ...] = ()


class PortfolioBuilder:
    """Convert total scores into a target portfolio."""

    def __init__(
        self,
        params: StrategyPortfolioParameters | None = None,
        config_path: str | Path | None = None,
    ) -> None:
        active = params or load_strategy_parameters(config_path).strategy.portfolio
        if int(active.top_n) <= 0:
            raise ValueError("strategy.portfolio.top_n must be positive")
        if int(active.min_names) < 0:
            raise ValueError("strategy.portfolio.min_names must be non-negative")
        if float(active.single_name_max_weight) <= 0:
            raise ValueError("strategy.portfolio.single_name_max_weight must be positive")
        if not 0.0 <= float(active.cash_buffer) < 1.0:
            raise ValueError("strategy.portfolio.cash_buffer must be in [0, 1)")

        self.params = active

    def build(
        self,
        scored: pd.DataFrame,
        score_column: str = "total_score",
    ) -> PortfolioConstructionResult:
        """Select top names and assign target weights."""

        if scored.empty:
            return PortfolioConstructionResult(
                holdings=self._empty_holdings_frame(),
                eligible_count=0,
                selected_count=0,
                cash_weight=1.0,
                notes=("当日无可用因子候选，返回空组合。",),
            )

        eligible_mask = (
            scored["eligible"].fillna(False).astype(bool)
            if "eligible" in scored.columns
            else pd.Series(True, index=scored.index)
        )
        eligible = scored.loc[eligible_mask].copy()
        eligible = eligible.loc[eligible[score_column].notna()].copy()
        if eligible.empty:
            return PortfolioConstructionResult(
                holdings=self._empty_holdings_frame(),
                eligible_count=0,
                selected_count=0,
                cash_weight=1.0,
                notes=("当日无 eligible 候选，返回空组合。",),
            )

        eligible = eligible.sort_values(
            [score_column, "cb_code"],
            ascending=[False, True],
            kind="stable",
        ).reset_index(drop=True)
        selected = eligible.head(int(self.params.top_n)).copy()
        notes: list[str] = []
        if len(selected) < int(self.params.min_names):
            notes.append(
                "可建仓标的少于 strategy.portfolio.min_names，当前组合按实际可用标的构建。"
            )

        investable_weight = max(0.0, 1.0 - float(self.params.cash_buffer))
        weights = self._build_weight_vector(selected[score_column], investable_weight)
        selected["target_weight"] = weights.to_numpy()
        selected["rank"] = range(1, len(selected) + 1)
        holdings = selected.loc[
            :,
            [
                "cb_code",
                "trade_date",
                score_column,
                "target_weight",
                "rank",
            ],
        ].copy()
        holdings = holdings.rename(columns={score_column: "total_score"})
        holdings = holdings.sort_values(
            ["target_weight", "total_score", "cb_code"],
            ascending=[False, False, True],
            kind="stable",
        ).reset_index(drop=True)
        cash_weight = max(0.0, 1.0 - float(holdings["target_weight"].sum()))
        return PortfolioConstructionResult(
            holdings=holdings,
            eligible_count=int(len(eligible)),
            selected_count=int(len(holdings)),
            cash_weight=float(cash_weight),
            notes=tuple(notes),
        )

    def _build_weight_vector(
        self,
        scores: pd.Series,
        investable_weight: float,
    ) -> pd.Series:
        if scores.empty or investable_weight <= 0:
            return pd.Series(0.0, index=scores.index, dtype="float64")

        n_selected = len(scores)
        weight_cap = float(self.params.single_name_max_weight)
        target_total = min(investable_weight, n_selected * weight_cap)
        if target_total <= 0:
            return pd.Series(0.0, index=scores.index, dtype="float64")

        if str(self.params.weighting_method).casefold() == "equal_weight":
            raw = pd.Series(1.0 / n_selected, index=scores.index, dtype="float64")
            return self._normalize_capped_weights(raw, target_total, weight_cap)
        if str(self.params.weighting_method).casefold() != "score_proportional":
            raise ValueError(
                "strategy.portfolio.weighting_method must be 'score_proportional' or 'equal_weight'"
            )

        numeric_scores = scores.astype("float64")
        if numeric_scores.nunique(dropna=False) <= 1:
            raw = pd.Series(1.0 / n_selected, index=scores.index, dtype="float64")
            return self._normalize_capped_weights(raw, target_total, weight_cap)

        shifted = numeric_scores - float(numeric_scores.min())
        epsilon = max(1e-9, abs(float(numeric_scores.max())) * 1e-9)
        signal = shifted + epsilon
        total_signal = float(signal.sum())
        if total_signal <= 0:
            raw = pd.Series(1.0 / n_selected, index=scores.index, dtype="float64")
            return self._normalize_capped_weights(raw, target_total, weight_cap)
        raw = signal / total_signal
        return self._normalize_capped_weights(raw, target_total, weight_cap)

    def _normalize_capped_weights(
        self,
        raw: pd.Series,
        target_total: float,
        cap: float,
    ) -> pd.Series:
        weights = raw.astype("float64").copy()
        total = float(weights.sum())
        if total <= 0:
            weights[:] = 0.0
            return weights
        weights = weights / total * target_total

        for _ in range(32):
            over_mask = weights.gt(cap + 1e-12)
            if not over_mask.any():
                break
            capped_total = float(weights.loc[over_mask].sum())
            weights.loc[over_mask] = cap
            remaining_target = max(0.0, target_total - float(weights.loc[over_mask].sum()))
            under_mask = ~over_mask
            if not under_mask.any() or remaining_target <= 1e-12:
                break
            under_weights = weights.loc[under_mask]
            under_total = float(under_weights.sum())
            if under_total <= 1e-12:
                weights.loc[under_mask] = remaining_target / int(under_mask.sum())
            else:
                weights.loc[under_mask] = under_weights / under_total * remaining_target
            if abs(capped_total - float(weights.sum())) <= 1e-12:
                break

        weights = weights.clip(lower=0.0, upper=cap)
        final_total = float(weights.sum())
        if final_total <= 0:
            weights[:] = 0.0
            return weights
        if final_total > target_total + 1e-9:
            weights = weights / final_total * target_total
        return weights.astype("float64")

    def _empty_holdings_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=["cb_code", "trade_date", "total_score", "target_weight", "rank"]
        )
