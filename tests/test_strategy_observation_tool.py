from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import pandas as pd

from exports.strategy_exports import StrategyObservationReport
from tools import export_strategy_observation


def build_fake_report() -> StrategyObservationReport:
    return StrategyObservationReport(
        trade_date=pd.Timestamp("2026-04-20"),
        requested_codes=["110001.SH", "128044.SZ"],
        environment=pd.DataFrame(
            [
                {
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "equity_strength": 0.71,
                    "bond_strength": 0.43,
                    "trend_strength": 0.82,
                }
            ]
        ),
        factor_weights=pd.DataFrame(
            [
                {
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "value": 0.28,
                    "carry": 0.20,
                    "structure": 0.18,
                    "trend": 0.24,
                    "stability": 0.10,
                }
            ]
        ),
        portfolio=pd.DataFrame(
            [
                {
                    "cb_code": "110001.SH",
                    "trade_date": pd.Timestamp("2026-04-20"),
                    "total_score": 0.9123,
                    "target_weight": 0.55,
                    "rank": 1,
                }
            ]
        ),
        watchlist=pd.DataFrame(columns=["代码", "观察状态"]),
        candidate_scores=pd.DataFrame(columns=["cb_code", "trade_date", "total_score"]),
        factor_diagnostics=pd.DataFrame(columns=["cb_code", "trade_date", "value_raw"]),
        cache_diagnostics=pd.DataFrame(columns=["item", "value", "comment"]),
        history_start_requested=pd.Timestamp("2025-10-01"),
        history_start_used=pd.Timestamp("2025-10-01"),
        first_fully_ready_trade_date=pd.Timestamp("2025-10-01"),
        fetch_policy="缓存优先，完整性优先",
        refresh_requested=False,
        runtime_snapshot_reused=False,
        data_quality_status="正常",
        eligible_count=27,
        selected_count=1,
        cash_weight=0.45,
        notes=("strategy 结果仅代表当前 trade_date 的静态决策。",),
    )


class StrategyObservationToolTests(unittest.TestCase):
    def test_main_success_path_prints_export_summary(self) -> None:
        stdout = io.StringIO()
        report = build_fake_report()

        with patch.object(
            export_strategy_observation,
            "build_strategy_observation_report",
            return_value=report,
        ) as build_mock:
            with patch.object(
                export_strategy_observation,
                "write_strategy_observation_xlsx",
                return_value="C:/tmp/strategy.xlsx",
            ) as write_mock:
                with patch.object(
                    export_strategy_observation,
                    "build_proxy_startup_hints",
                    return_value=(),
                ):
                    with redirect_stdout(stdout):
                        code = export_strategy_observation.main(
                            ["--trade-date", "2026-04-20", "--codes", "110001 128044"]
                        )

        self.assertEqual(code, 0)
        self.assertEqual(build_mock.call_args.kwargs["trade_date"], "2026-04-20")
        self.assertEqual(
            build_mock.call_args.kwargs["requested_codes"],
            "110001 128044",
        )
        self.assertEqual(write_mock.call_args.kwargs["report"], report)
        output = stdout.getvalue()
        self.assertIn("[完成] strategy 单交易日观察导出成功", output)
        self.assertIn("输出文件: C:/tmp/strategy.xlsx", output)
        self.assertIn("观察名单代码数: 2", output)

    def test_main_interactive_path_reads_inputs(self) -> None:
        stdout = io.StringIO()
        report = build_fake_report()

        with patch.object(
            export_strategy_observation,
            "build_strategy_observation_report",
            return_value=report,
        ) as build_mock:
            with patch.object(
                export_strategy_observation,
                "write_strategy_observation_xlsx",
                return_value="C:/tmp/strategy.xlsx",
            ):
                with patch.object(
                    export_strategy_observation,
                    "build_proxy_startup_hints",
                    return_value=(),
                ):
                    with patch("builtins.input", side_effect=["2026-04-20", "110001 128044"]):
                        with redirect_stdout(stdout):
                            code = export_strategy_observation.main(["--interactive"])

        self.assertEqual(code, 0)
        self.assertEqual(build_mock.call_args.kwargs["trade_date"], "2026-04-20")
        self.assertEqual(
            build_mock.call_args.kwargs["requested_codes"],
            "110001 128044",
        )


if __name__ == "__main__":
    unittest.main()
