"""Pure strategy core built on top of env/factor/model modules."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from env import EnvironmentDetector
from factor import FactorEngine
from model import WeightMapper
from config.strategy_config import StrategyParameters, load_strategy_parameters
from shared.reporting_semantics import (
    DATA_QUALITY_STATUS_OK,
    build_data_quality_warning_note,
    resolve_data_quality_status,
)

from .portfolio import PortfolioBuilder
from .result import StrategyDecision, StrategyDiagnostics
from .snapshot import StrategySnapshot


class StrategyEngine:
    """Pure strategy core for one trade-date snapshot."""

    def __init__(
        self,
        detector: EnvironmentDetector | None = None,
        factor_engine: FactorEngine | None = None,
        weight_mapper: WeightMapper | None = None,
        portfolio_builder: PortfolioBuilder | None = None,
        config: StrategyParameters | None = None,
        config_path: str | Path | None = None,
    ) -> None:
        self.config = config or load_strategy_parameters(config_path)
        self.detector = detector or EnvironmentDetector(params=self.config.env)
        self.factor_engine = factor_engine or FactorEngine(params=self.config.factor)
        self.weight_mapper = weight_mapper or WeightMapper(params=self.config.model)
        self.portfolio_builder = portfolio_builder or PortfolioBuilder(
            params=self.config.strategy.portfolio
        )

    def run(self, snapshot: StrategySnapshot) -> StrategyDecision:
        """Run environment, factor, weight, and portfolio steps for one snapshot."""

        computation, alignment_summary = self.detector.compute_aligned_with_warmup(
            snapshot.macro_daily,
            snapshot.trading_calendar,
        )
        env_row = computation.scores.loc[
            computation.scores["trade_date"].eq(snapshot.trade_date)
        ].copy()
        if env_row.empty:
            raise ValueError(
                f"No environment score available on trade_date {snapshot.trade_date.strftime('%Y-%m-%d')}."
            )
        env_values = (
            env_row.iloc[-1][
                ["equity_strength", "bond_strength", "trend_strength"]
            ]
            .astype("float64")
            .to_dict()
        )
        current_codes: list[str] = []
        if {
            "trade_date",
            "cb_code",
        }.issubset(snapshot.cb_daily.columns):
            current_codes = (
                snapshot.cb_daily.loc[
                    snapshot.cb_daily["trade_date"].eq(snapshot.trade_date),
                    "cb_code",
                ]
                .dropna()
                .astype(str)
                .drop_duplicates()
                .tolist()
            )
        factor_cb_daily = snapshot.cb_daily
        factor_cb_basic = snapshot.cb_basic
        factor_cb_call = snapshot.cb_call
        factor_cb_rate = snapshot.cb_rate
        if current_codes:
            factor_cb_daily = snapshot.cb_daily.loc[
                snapshot.cb_daily["cb_code"].isin(current_codes)
            ].copy()
            if not snapshot.cb_basic.empty and "cb_code" in snapshot.cb_basic.columns:
                factor_cb_basic = snapshot.cb_basic.loc[
                    snapshot.cb_basic["cb_code"].isin(current_codes)
                ].copy()
            if not snapshot.cb_call.empty and "cb_code" in snapshot.cb_call.columns:
                factor_cb_call = snapshot.cb_call.loc[
                    snapshot.cb_call["cb_code"].isin(current_codes)
                ].copy()
            if (
                snapshot.cb_rate is not None
                and not snapshot.cb_rate.empty
                and "cb_code" in snapshot.cb_rate.columns
            ):
                factor_cb_rate = snapshot.cb_rate.loc[
                    snapshot.cb_rate["cb_code"].isin(current_codes)
                ].copy()
        factor_diagnostics = self.factor_engine.compute_with_diagnostics(
            as_of_date=snapshot.trade_date,
            cb_daily=factor_cb_daily,
            cb_basic=factor_cb_basic,
            cb_call=factor_cb_call,
            cb_rate=factor_cb_rate,
        )
        factor_weights = self.weight_mapper.compute(env_values)
        scored = self.factor_engine.append_weighted_total_score(
            factor_diagnostics,
            factor_weights,
            column_name="total_score",
        )
        total_score_columns = [
            column
            for column in (
                "cb_code",
                "trade_date",
                "total_score",
                "eligible",
                "exclude_reason",
                *self.factor_engine.SCORE_COLUMNS,
            )
            if column in scored.columns
        ]
        total_scores = scored.loc[:, total_score_columns].copy()
        total_scores = total_scores.sort_values(
            ["eligible", "total_score", "cb_code"],
            ascending=[False, False, True],
            kind="stable",
        ).reset_index(drop=True)

        portfolio_result = self.portfolio_builder.build(scored, score_column="total_score")
        notes: list[str] = []
        has_data_quality_issue = bool(snapshot.data_quality_hints)
        if (
            computation.first_fully_ready_trade_date is not None
            and snapshot.trade_date < computation.first_fully_ready_trade_date
        ):
            notes.append(
                "当前 trade_date 早于环境 fully-ready 首日，策略结果可能仍受环境预热默认值影响。"
            )
            has_data_quality_issue = True
        notes.extend(portfolio_result.notes)
        data_quality_status = resolve_data_quality_status(has_data_quality_issue)
        if data_quality_status != DATA_QUALITY_STATUS_OK:
            notes.append(build_data_quality_warning_note("策略预览"))
        diagnostics = StrategyDiagnostics(
            history_start_requested=snapshot.history_window.requested_start,
            history_start_used=snapshot.history_window.used_start,
            refresh_requested=snapshot.refresh_requested,
            runtime_snapshot_reused=snapshot.runtime_snapshot_reused,
            requested_codes=snapshot.requested_codes,
            data_quality_status=data_quality_status,
            data_quality_hints=snapshot.data_quality_hints,
            notes=tuple(notes),
            alignment_summary=alignment_summary,
            first_fully_ready_trade_date=computation.first_fully_ready_trade_date,
            cache_diagnostics=snapshot.cache_diagnostics,
        )
        return StrategyDecision(
            trade_date=snapshot.trade_date,
            environment={name: float(value) for name, value in env_values.items()},
            factor_weights={name: float(value) for name, value in factor_weights.items()},
            factor_diagnostics=scored.reset_index(drop=True),
            total_scores=total_scores,
            selected_portfolio=portfolio_result.holdings,
            eligible_count=portfolio_result.eligible_count,
            cash_weight=float(portfolio_result.cash_weight),
            diagnostics=diagnostics,
        )
