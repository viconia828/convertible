from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import pandas as pd

from shared.cache_diagnostics import build_cache_diagnostics
from env.macro_alignment import MacroAlignmentSummary
from strategy.result import StrategyDecision, StrategyDiagnostics
from tools import preview_strategy


class FakeStrategyService:
    def __init__(self, decision: StrategyDecision | None = None, error: Exception | None = None):
        self.decision = decision
        self.error = error
        self.calls: list[tuple[object, tuple[str, ...], bool]] = []

    def run_for_date(self, trade_date, requested_codes=(), refresh=False):
        normalized_codes = tuple(requested_codes or ())
        self.calls.append((trade_date, normalized_codes, bool(refresh)))
        if self.error is not None:
            raise self.error
        return self.decision


class StrategyPreviewToolTests(unittest.TestCase):
    def _build_decision(
        self,
        requested_codes: tuple[str, ...] = (),
        runtime_snapshot_reused: bool = False,
    ) -> StrategyDecision:
        portfolio = pd.DataFrame(
            [
                {
                    "cb_code": "110001.SH",
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "total_score": 0.9123,
                    "target_weight": 0.55,
                    "rank": 1,
                },
                {
                    "cb_code": "110002.SH",
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "total_score": 0.8456,
                    "target_weight": 0.45,
                    "rank": 2,
                },
            ]
        )
        diagnostics = StrategyDiagnostics(
            history_start_requested=pd.Timestamp("2025-10-01"),
            history_start_used=pd.Timestamp("2025-10-01"),
            refresh_requested=False,
            runtime_snapshot_reused=runtime_snapshot_reused,
            requested_codes=requested_codes,
            data_quality_status="正常",
            data_quality_hints=("基于本地缓存构造 snapshot。",),
            notes=("strategy 结果仅代表当前 trade_date 的静态决策。",),
            alignment_summary=MacroAlignmentSummary(
                total_calendar_days=150,
                kept_days=150,
                dropped_days=0,
                filled_days_by_indicator={},
                invalid_days_by_indicator={},
            ),
            first_fully_ready_trade_date=pd.Timestamp("2025-10-01"),
            cache_diagnostics=build_cache_diagnostics(
                {
                    "cache_resolution_hit_calls": 3,
                    "cache_resolution_partial_hit_calls": 1,
                    "remote_fill_calls": 1,
                    "cache_file_scan_calls": 5,
                    "panel_memory_hit_calls": 2,
                    "panel_memory_save_calls": 1,
                    "aggregate_memory_hit_calls": 1,
                    "stage_calls::cb_daily_cross_section::request_panel_lookup": 1,
                    "stage_elapsed_ms::cb_daily_cross_section::request_panel_lookup": 14,
                },
                runtime_snapshot_reused=runtime_snapshot_reused,
            ),
        )
        return StrategyDecision(
            trade_date=pd.Timestamp("2026-04-20"),
            environment={
                "equity_strength": 0.71,
                "bond_strength": 0.43,
                "trend_strength": 0.82,
            },
            factor_weights={
                "value": 0.28,
                "carry": 0.20,
                "structure": 0.18,
                "trend": 0.24,
                "stability": 0.10,
            },
            factor_diagnostics=pd.DataFrame(),
            total_scores=pd.DataFrame(
                [
                    {
                        "cb_code": "110001.SH",
                        "trade_date": pd.Timestamp("2026-04-20"),
                        "total_score": 0.9123,
                        "eligible": True,
                        "exclude_reason": "",
                    },
                    {
                        "cb_code": "110002.SH",
                        "trade_date": pd.Timestamp("2026-04-20"),
                        "total_score": 0.8456,
                        "eligible": True,
                        "exclude_reason": "",
                    },
                    {
                        "cb_code": "110003.SH",
                        "trade_date": pd.Timestamp("2026-04-20"),
                        "total_score": pd.NA,
                        "eligible": False,
                        "exclude_reason": "not_tradable",
                    },
                ]
            ),
            selected_portfolio=portfolio,
            eligible_count=27,
            cash_weight=0.0,
            diagnostics=diagnostics,
        )

    def test_render_strategy_preview_contains_core_sections(self) -> None:
        preview = preview_strategy.render_strategy_preview(self._build_decision())

        self.assertIn("环境分数:", preview)
        self.assertIn("因子权重:", preview)
        self.assertIn("目标组合:", preview)
        self.assertIn("110001.SH", preview)
        self.assertIn("现金权重: 0.00%", preview)
        self.assertIn("取数策略: 缓存优先，完整性优先", preview)
        self.assertIn("刷新请求: 否", preview)
        self.assertIn("数据质量状态: 正常", preview)
        self.assertIn("缓存诊断:", preview)
        self.assertIn("复用层: runtime snapshot=未命中", preview)

    def test_render_strategy_preview_summary_is_compact(self) -> None:
        preview = preview_strategy.render_strategy_preview(
            self._build_decision(),
            detail_level=preview_strategy.DETAIL_LEVEL_SUMMARY,
        )

        self.assertIn("环境摘要:", preview)
        self.assertIn("候选/入选/现金:", preview)
        self.assertIn("目标组合:", preview)
        self.assertIn("统一观测:", preview)
        self.assertNotIn("环境分数:", preview)
        self.assertNotIn("对齐摘要:", preview)
        self.assertNotIn("基于本地缓存构造 snapshot。", preview)

    def test_render_strategy_preview_contains_watchlist_view(self) -> None:
        preview = preview_strategy.render_strategy_preview(
            self._build_decision(requested_codes=("110001.SH", "110003.SH", "128123.SZ"))
        )

        self.assertIn("观察名单视角:", preview)
        self.assertIn("已入选目标组合", preview)
        self.assertIn("不可交易/已剔除", preview)
        self.assertIn("当日无横截面记录", preview)

    def test_main_success_path_prints_preview_and_returns_zero(self) -> None:
        fake_service = FakeStrategyService(decision=self._build_decision())
        stdout = io.StringIO()

        with patch.object(preview_strategy, "StrategyService", return_value=fake_service):
            with patch.object(preview_strategy, "build_proxy_startup_hints", return_value=()):
                with redirect_stdout(stdout):
                    code = preview_strategy.main(["--trade-date", "2026-04-20"])

        self.assertEqual(code, 0)
        output = stdout.getvalue()
        self.assertIn("[完成] strategy 单日期预览成功", output)
        self.assertIn("交易日: 2026-04-20", output)
        self.assertIn("[视图] 摘要模式", output)
        self.assertIn("环境摘要:", output)
        self.assertNotIn("对齐摘要:", output)
        self.assertEqual(fake_service.calls, [("2026-04-20", (), False)])

    def test_main_codes_path_normalizes_watchlist_codes(self) -> None:
        fake_service = FakeStrategyService(
            decision=self._build_decision(requested_codes=("110001.SH", "128044.SZ"))
        )
        stdout = io.StringIO()

        with patch.object(preview_strategy, "StrategyService", return_value=fake_service):
            with patch.object(preview_strategy, "build_proxy_startup_hints", return_value=()):
                with redirect_stdout(stdout):
                    code = preview_strategy.main(
                        ["--trade-date", "2026-04-20", "--codes", "110001, 128044"]
                    )

        self.assertEqual(code, 0)
        self.assertIn("[视角] 观察名单模式：110001.SH, 128044.SZ", stdout.getvalue())
        self.assertEqual(
            fake_service.calls,
            [("2026-04-20", ("110001.SH", "128044.SZ"), False)],
        )

    def test_main_verbose_path_prints_detailed_sections(self) -> None:
        fake_service = FakeStrategyService(
            decision=self._build_decision(runtime_snapshot_reused=True)
        )
        stdout = io.StringIO()

        with patch.object(preview_strategy, "StrategyService", return_value=fake_service):
            with patch.object(preview_strategy, "build_proxy_startup_hints", return_value=()):
                with redirect_stdout(stdout):
                    code = preview_strategy.main(["--trade-date", "2026-04-20", "--verbose"])

        self.assertEqual(code, 0)
        output = stdout.getvalue()
        self.assertIn("[视图] 详细模式", output)
        self.assertIn("环境分数:", output)
        self.assertIn("对齐摘要:", output)
        self.assertIn("缓存诊断:", output)
        self.assertIn("runtime snapshot=命中", output)
        self.assertIn("主要阶段耗时:", output)

    def test_main_failure_path_returns_nonzero(self) -> None:
        fake_service = FakeStrategyService(error=ValueError("boom"))
        stdout = io.StringIO()

        with patch.object(preview_strategy, "StrategyService", return_value=fake_service):
            with patch.object(preview_strategy, "build_proxy_startup_hints", return_value=()):
                with patch.object(preview_strategy, "build_tushare_failure_hints", return_value=()):
                    with redirect_stdout(stdout):
                        code = preview_strategy.main(["--trade-date", "2026-04-20"])

        self.assertEqual(code, 1)
        self.assertIn("[失败] strategy 单日期预览失败: boom", stdout.getvalue())

    def test_main_interactive_path_reads_trade_date_input(self) -> None:
        fake_service = FakeStrategyService(decision=self._build_decision())
        stdout = io.StringIO()

        with patch.object(preview_strategy, "StrategyService", return_value=fake_service):
            with patch.object(preview_strategy, "build_proxy_startup_hints", return_value=()):
                with patch("builtins.input", side_effect=["2026-04-20", ""]):
                    with redirect_stdout(stdout):
                        code = preview_strategy.main(["--interactive"])

        self.assertEqual(code, 0)
        self.assertEqual(fake_service.calls, [("2026-04-20", (), False)])

    def test_main_interactive_path_reads_watchlist_input(self) -> None:
        fake_service = FakeStrategyService(
            decision=self._build_decision(requested_codes=("110001.SH", "128044.SZ"))
        )
        stdout = io.StringIO()

        with patch.object(preview_strategy, "StrategyService", return_value=fake_service):
            with patch.object(preview_strategy, "build_proxy_startup_hints", return_value=()):
                with patch("builtins.input", side_effect=["2026-04-20", "110001 128044"]):
                    with redirect_stdout(stdout):
                        code = preview_strategy.main(["--interactive"])

        self.assertEqual(code, 0)
        self.assertEqual(
            fake_service.calls,
            [("2026-04-20", ("110001.SH", "128044.SZ"), False)],
        )


if __name__ == "__main__":
    unittest.main()
