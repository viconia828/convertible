"""Interactive/CLI entrypoint for one-date strategy preview."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from runtime_hints import (  # noqa: E402
    build_proxy_startup_hints,
    build_tushare_failure_hints,
)
from reporting_semantics import format_alignment_summary, yes_no_label  # noqa: E402
from strategy import StrategyDecision, StrategyService  # noqa: E402
from strategy_config import load_strategy_parameters  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preview one-date strategy output.")
    parser.add_argument("--trade-date", help="Trade date, e.g. 2026-04-20")
    parser.add_argument("--config", help="Optional strategy parameter file path")
    refresh_group = parser.add_mutually_exclusive_group()
    refresh_group.add_argument(
        "--refresh",
        action="store_true",
        help="Force-refresh remote-backed caches for this run",
    )
    refresh_group.add_argument(
        "--no-refresh",
        action="store_true",
        help="Disable force-refresh for this run and use cache-first mode",
    )
    parser.add_argument("--interactive", action="store_true", help="Prompt for missing inputs")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = load_strategy_parameters(args.config)
    interactive = args.interactive or not args.trade_date
    trade_date = args.trade_date
    refresh = False
    if args.refresh:
        refresh = True
    elif args.no_refresh:
        refresh = False
    startup_hints = build_proxy_startup_hints()

    if interactive:
        print("=" * 60)
        print("可转债多因子 - strategy 单日期预览")
        print(f"参数文件: {config.path}")
        print("输出内容: 单个交易日的环境分数、因子权重和目标组合")
        print("日期格式示例: 2026-04-20")
        print("取数规则: 默认缓存优先；发现覆盖不足时自动补数。")
        if startup_hints:
            print("运行提示:")
            for hint in startup_hints:
                print(f"- {hint}")
        print("=" * 60)
        trade_date = input("请输入交易日: ").strip()
    elif startup_hints:
        for hint in startup_hints:
            print(f"[提示] {hint}")

    if refresh:
        print("[模式] 强制刷新模式：将尽量重拉本次所需远端数据，并覆盖相关缓存。")
    else:
        print("[模式] 默认模式：缓存优先，发现覆盖不足时自动补数。")

    try:
        service = StrategyService(config=config)
        decision = service.run_for_date(
            trade_date=trade_date,
            refresh=refresh,
        )
    except Exception as exc:
        print(f"[失败] strategy 单日期预览失败: {exc}")
        for hint in build_tushare_failure_hints(str(exc)):
            print(f"[提示] {hint}")
        return 1

    print()
    print(render_strategy_preview(decision))
    return 0


def render_strategy_preview(decision: StrategyDecision) -> str:
    """Render one strategy decision into a readable terminal summary."""

    lines: list[str] = []
    lines.append("[完成] strategy 单日期预览成功")
    lines.append(f"交易日: {decision.trade_date.strftime('%Y-%m-%d')}")
    lines.append(f"取数策略: {decision.diagnostics.fetch_policy}")
    lines.append(f"刷新请求: {yes_no_label(decision.diagnostics.refresh_requested)}")
    lines.append(f"数据质量状态: {decision.diagnostics.data_quality_status}")
    lines.append(
        "历史窗口: "
        f"{decision.diagnostics.history_start_requested.strftime('%Y-%m-%d')} -> "
        f"{decision.diagnostics.history_start_used.strftime('%Y-%m-%d')}"
    )
    lines.append(f"可建仓标的数: {decision.eligible_count}")
    lines.append(f"目标组合标的数: {len(decision.selected_portfolio)}")
    lines.append(f"现金权重: {decision.cash_weight:.2%}")
    if decision.diagnostics.first_fully_ready_trade_date is not None:
        lines.append(
            "环境 fully-ready 首日: "
            f"{decision.diagnostics.first_fully_ready_trade_date.strftime('%Y-%m-%d')}"
        )

    lines.append("")
    lines.append("环境分数:")
    for key in ("equity_strength", "bond_strength", "trend_strength"):
        lines.append(f"- {key}: {float(decision.environment[key]):.4f}")

    lines.append("")
    lines.append("因子权重:")
    for key in ("value", "carry", "structure", "trend", "stability"):
        lines.append(f"- {key}: {float(decision.factor_weights[key]):.2%}")

    lines.append("")
    lines.append("目标组合:")
    lines.extend(_render_portfolio_lines(decision.selected_portfolio))

    alignment_summary = decision.diagnostics.alignment_summary
    if alignment_summary is not None:
        lines.append("")
        lines.append(f"对齐摘要: {format_alignment_summary(alignment_summary)}")

    notes = list(dict.fromkeys(
        list(decision.diagnostics.data_quality_hints) + list(decision.diagnostics.notes)
    ))
    if notes:
        lines.append("")
        lines.append("提示:")
        for note in notes:
            lines.append(f"- {note}")

    return "\n".join(lines)


def _render_portfolio_lines(portfolio: pd.DataFrame) -> list[str]:
    if portfolio.empty:
        return ["- （空组合）"]

    display = portfolio.copy()
    if "trade_date" in display.columns:
        display = display.drop(columns=["trade_date"])
    if "target_weight" in display.columns:
        display["target_weight"] = display["target_weight"].map(lambda value: f"{float(value):.2%}")
    if "total_score" in display.columns:
        display["total_score"] = display["total_score"].map(lambda value: f"{float(value):.4f}")
    table = display.to_string(index=False)
    return table.splitlines()


if __name__ == "__main__":
    raise SystemExit(main())
