"""Interactive/CLI entrypoint for one-date strategy preview."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


DETAIL_LEVEL_SUMMARY = "summary"
DETAIL_LEVEL_VERBOSE = "verbose"


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.strategy_config import load_strategy_parameters  # noqa: E402
from shared.cache_diagnostics import (  # noqa: E402
    build_cache_diagnostics,
    render_cache_diagnostic_lines,
)
from shared.reporting_semantics import (  # noqa: E402
    DATA_QUALITY_STATUS_OK,
    format_alignment_summary,
    yes_no_label,
)
from tools.runtime_hints import (  # noqa: E402
    build_proxy_startup_hints,
    build_tushare_failure_hints,
)
from strategy import (  # noqa: E402
    StrategyDecision,
    StrategyService,
    normalize_requested_codes,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preview one-date strategy output.")
    parser.add_argument("--trade-date", help="Trade date, e.g. 2026-04-20")
    parser.add_argument(
        "--codes",
        help="Optional watchlist codes; 6-digit codes auto-complete exchange suffix",
    )
    parser.add_argument("--config", help="Optional strategy parameter file path")
    detail_group = parser.add_mutually_exclusive_group()
    detail_group.add_argument(
        "--summary-only",
        action="store_true",
        help="Show compact summary view only (default)",
    )
    detail_group.add_argument(
        "--verbose",
        action="store_true",
        help="Show full detail view including alignment summary and all notes",
    )
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
    detail_level = resolve_preview_detail_level(
        summary_only=args.summary_only,
        verbose=args.verbose,
    )
    trade_date = args.trade_date
    raw_codes = args.codes or ""
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
        print("可选输入: 候选代码 / 观察名单，用于聚焦查看指定转债当日状态")
        print("代码格式示例: 110073.SH,128044.SZ 或 110073 128044")
        print("输入六位数字时代码会自动补全交易所后缀。")
        print("取数规则: 默认缓存优先；发现覆盖不足时自动补数。")
        print("输出视图: 默认摘要模式；如需完整诊断请使用 --verbose。")
        if startup_hints:
            print("运行提示:")
            for hint in startup_hints:
                print(f"- {hint}")
        print("=" * 60)
        trade_date = input("请输入交易日: ").strip()
        raw_codes = input("可选，请输入候选代码 / 观察名单: ").strip()
    elif startup_hints:
        for hint in startup_hints:
            print(f"[提示] {hint}")

    requested_codes = normalize_requested_codes(raw_codes)
    if refresh:
        print("[模式] 强制刷新模式：将尽量重拉本次所需远端数据，并覆盖相关缓存。")
    else:
        print("[模式] 默认模式：缓存优先，发现覆盖不足时自动补数。")
    print(f"[视图] {describe_preview_detail_level(detail_level)}")
    if requested_codes:
        print(f"[视角] 观察名单模式：{', '.join(requested_codes)}")

    try:
        service = StrategyService(config=config)
        decision = service.run_for_date(
            trade_date=trade_date,
            requested_codes=requested_codes,
            refresh=refresh,
        )
    except Exception as exc:
        print(f"[失败] strategy 单日期预览失败: {exc}")
        for hint in build_tushare_failure_hints(str(exc)):
            print(f"[提示] {hint}")
        return 1

    print()
    print(render_strategy_preview(decision, detail_level=detail_level))
    return 0


def resolve_preview_detail_level(
    summary_only: bool = False,
    verbose: bool = False,
) -> str:
    if verbose:
        return DETAIL_LEVEL_VERBOSE
    if summary_only:
        return DETAIL_LEVEL_SUMMARY
    return DETAIL_LEVEL_SUMMARY


def describe_preview_detail_level(detail_level: str) -> str:
    if detail_level == DETAIL_LEVEL_VERBOSE:
        return "详细模式：显示完整环境、权重、组合、对齐摘要与提示。"
    return "摘要模式：默认只显示核心结果；如需完整诊断请加 --verbose。"


def render_strategy_preview(
    decision: StrategyDecision,
    detail_level: str = DETAIL_LEVEL_VERBOSE,
) -> str:
    """Render one strategy decision into a readable terminal summary."""

    if detail_level == DETAIL_LEVEL_SUMMARY:
        return _render_strategy_preview_summary(decision)
    if detail_level == DETAIL_LEVEL_VERBOSE:
        return _render_strategy_preview_verbose(decision)
    raise ValueError(f"Unsupported detail_level: {detail_level}")


def _render_strategy_preview_summary(decision: StrategyDecision) -> str:
    lines = _build_preview_header_lines(decision, detail_level=DETAIL_LEVEL_SUMMARY)
    requested_code_lines = _render_requested_code_lines(
        decision,
        detail_level=DETAIL_LEVEL_SUMMARY,
    )
    if requested_code_lines:
        lines.append("")
        lines.append("观察名单视角:")
        lines.extend(requested_code_lines)

    cache_diagnostic_lines = _render_cache_diagnostic_lines(
        decision,
        detail_level=DETAIL_LEVEL_SUMMARY,
    )
    if cache_diagnostic_lines:
        lines.append("")
        lines.append("缓存诊断:")
        lines.extend(cache_diagnostic_lines)

    lines.append("")
    lines.append(
        "环境摘要: "
        + " | ".join(
            f"{key}={float(decision.environment[key]):.4f}"
            for key in ("equity_strength", "bond_strength", "trend_strength")
        )
    )
    lines.append(
        "因子权重: "
        + " | ".join(
            f"{key}={float(decision.factor_weights[key]):.2%}"
            for key in ("value", "carry", "structure", "trend", "stability")
        )
    )

    lines.append("")
    lines.append("目标组合:")
    lines.extend(
        _render_portfolio_lines(
            decision.selected_portfolio,
            detail_level=DETAIL_LEVEL_SUMMARY,
        )
    )

    notes = _collect_preview_notes(decision, detail_level=DETAIL_LEVEL_SUMMARY)
    if notes:
        lines.append("")
        lines.append("提示:")
        for note in notes:
            lines.append(f"- {note}")

    return "\n".join(lines)


def _render_strategy_preview_verbose(decision: StrategyDecision) -> str:
    lines = _build_preview_header_lines(decision, detail_level=DETAIL_LEVEL_VERBOSE)

    requested_code_lines = _render_requested_code_lines(
        decision,
        detail_level=DETAIL_LEVEL_VERBOSE,
    )
    if requested_code_lines:
        lines.append("")
        lines.append("观察名单视角:")
        lines.extend(requested_code_lines)

    cache_diagnostic_lines = _render_cache_diagnostic_lines(
        decision,
        detail_level=DETAIL_LEVEL_VERBOSE,
    )
    if cache_diagnostic_lines:
        lines.append("")
        lines.append("缓存诊断:")
        lines.extend(cache_diagnostic_lines)

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
    lines.extend(
        _render_portfolio_lines(
            decision.selected_portfolio,
            detail_level=DETAIL_LEVEL_VERBOSE,
        )
    )

    alignment_summary = decision.diagnostics.alignment_summary
    if alignment_summary is not None:
        lines.append("")
        lines.append(f"对齐摘要: {format_alignment_summary(alignment_summary)}")

    notes = _collect_preview_notes(decision, detail_level=DETAIL_LEVEL_VERBOSE)
    if notes:
        lines.append("")
        lines.append("提示:")
        for note in notes:
            lines.append(f"- {note}")

    return "\n".join(lines)


def _build_preview_header_lines(
    decision: StrategyDecision,
    detail_level: str,
) -> list[str]:
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
    if detail_level == DETAIL_LEVEL_SUMMARY:
        lines.append(
            "候选/入选/现金: "
            f"{decision.eligible_count} / {len(decision.selected_portfolio)} / {decision.cash_weight:.2%}"
        )
    else:
        lines.append(f"可建仓标的数: {decision.eligible_count}")
        lines.append(f"目标组合标的数: {len(decision.selected_portfolio)}")
        lines.append(f"现金权重: {decision.cash_weight:.2%}")

    first_ready_date = decision.diagnostics.first_fully_ready_trade_date
    if first_ready_date is not None:
        if detail_level == DETAIL_LEVEL_VERBOSE or decision.trade_date < first_ready_date:
            lines.append(
                "环境 fully-ready 首日: "
                f"{first_ready_date.strftime('%Y-%m-%d')}"
            )
    if decision.diagnostics.requested_codes:
        lines.append(f"观察名单代码数: {len(decision.diagnostics.requested_codes)}")
        lines.append(f"观察名单: {', '.join(decision.diagnostics.requested_codes)}")
    return lines


def _render_portfolio_lines(
    portfolio: pd.DataFrame,
    detail_level: str = DETAIL_LEVEL_VERBOSE,
) -> list[str]:
    if portfolio.empty:
        return ["- （空组合）"]

    display = portfolio.copy()
    if "trade_date" in display.columns:
        display = display.drop(columns=["trade_date"])
    if detail_level == DETAIL_LEVEL_SUMMARY:
        display = display.loc[
            :,
            [column for column in ("rank", "cb_code", "target_weight") if column in display.columns],
        ].copy()
        display = display.rename(
            columns={
                "rank": "排名",
                "cb_code": "代码",
                "target_weight": "目标权重",
            }
        )
    if "target_weight" in display.columns:
        display["target_weight"] = display["target_weight"].map(lambda value: f"{float(value):.2%}")
    if "目标权重" in display.columns:
        display["目标权重"] = display["目标权重"].map(lambda value: f"{float(value):.2%}")
    if "total_score" in display.columns:
        display["total_score"] = display["total_score"].map(lambda value: f"{float(value):.4f}")
    table = display.to_string(index=False)
    return table.splitlines()


def _render_requested_code_lines(
    decision: StrategyDecision,
    detail_level: str = DETAIL_LEVEL_VERBOSE,
) -> list[str]:
    requested_codes = tuple(decision.diagnostics.requested_codes)
    if not requested_codes:
        return []

    watchlist = pd.DataFrame({"cb_code": list(requested_codes)})
    total_scores = decision.total_scores.copy()
    required_score_columns = ("cb_code", "trade_date", "eligible", "exclude_reason", "total_score")
    if total_scores.empty:
        total_scores = pd.DataFrame(columns=required_score_columns)
    else:
        total_scores = total_scores.loc[
            :,
            [column for column in required_score_columns if column in total_scores.columns],
        ].copy()
    for column in required_score_columns:
        if column not in total_scores.columns:
            total_scores[column] = pd.NA
    selected = decision.selected_portfolio.copy()
    required_selected_columns = ("cb_code", "rank", "target_weight")
    if selected.empty:
        selected = pd.DataFrame(columns=required_selected_columns)
    else:
        selected = selected.loc[
            :,
            [column for column in required_selected_columns if column in selected.columns],
        ].copy()
    for column in required_selected_columns:
        if column not in selected.columns:
            selected[column] = pd.NA
    selected = selected.rename(
        columns={
            "rank": "portfolio_rank",
            "target_weight": "portfolio_weight",
        }
    )

    eligible_scores = total_scores.loc[
        total_scores["eligible"].fillna(False).astype(bool) & total_scores["total_score"].notna(),
        ["cb_code"],
    ].copy()
    eligible_scores["eligible_rank"] = range(1, len(eligible_scores) + 1)

    watchlist = watchlist.merge(total_scores, on="cb_code", how="left")
    watchlist = watchlist.merge(eligible_scores, on="cb_code", how="left")
    watchlist = watchlist.merge(selected, on="cb_code", how="left")
    watchlist["status"] = watchlist.apply(_resolve_watchlist_status, axis=1)
    watchlist["eligible_display"] = watchlist["eligible"].map(_format_nullable_bool)
    watchlist["eligible_rank"] = watchlist["eligible_rank"].map(_format_rank)
    watchlist["portfolio_rank"] = watchlist["portfolio_rank"].map(_format_rank)
    watchlist["portfolio_weight"] = watchlist["portfolio_weight"].map(_format_percent)
    watchlist["total_score"] = watchlist["total_score"].map(_format_score)
    watchlist["exclude_reason"] = watchlist["exclude_reason"].fillna("")
    display = watchlist.rename(
        columns={
            "cb_code": "代码",
            "status": "观察状态",
            "eligible_display": "可建仓",
            "eligible_rank": "候选排序",
            "portfolio_rank": "组合排序",
            "portfolio_weight": "目标权重",
            "total_score": "总分",
            "exclude_reason": "剔除原因",
        }
    )
    if detail_level == DETAIL_LEVEL_SUMMARY:
        display_columns = ["代码", "观察状态", "组合排序", "目标权重", "总分", "剔除原因"]
    else:
        display_columns = ["代码", "观察状态", "可建仓", "候选排序", "组合排序", "目标权重", "总分", "剔除原因"]
    table = display.loc[:, display_columns].to_string(index=False)
    return table.splitlines()


def _collect_preview_notes(
    decision: StrategyDecision,
    detail_level: str,
) -> list[str]:
    if detail_level == DETAIL_LEVEL_VERBOSE:
        raw_notes = list(decision.diagnostics.data_quality_hints) + list(decision.diagnostics.notes)
    else:
        raw_notes = list(decision.diagnostics.notes)
        if decision.diagnostics.data_quality_status != DATA_QUALITY_STATUS_OK:
            raw_notes = list(decision.diagnostics.data_quality_hints) + raw_notes
    return list(dict.fromkeys(raw_notes))


def _render_cache_diagnostic_lines(
    decision: StrategyDecision,
    detail_level: str,
) -> list[str]:
    diagnostics = decision.diagnostics.cache_diagnostics or build_cache_diagnostics(
        runtime_snapshot_reused=decision.diagnostics.runtime_snapshot_reused
    )
    return render_cache_diagnostic_lines(diagnostics, detail_level=detail_level)


def _resolve_watchlist_status(row: pd.Series) -> str:
    if pd.isna(row.get("trade_date")):
        return "当日无横截面记录"
    if pd.notna(row.get("portfolio_rank")):
        return "已入选目标组合"
    eligible = row.get("eligible")
    if pd.notna(eligible) and bool(eligible):
        return "可建仓未入选"
    return "不可交易/已剔除"


def _format_nullable_bool(value: object) -> str:
    if pd.isna(value):
        return "未知"
    return yes_no_label(bool(value))


def _format_rank(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(int(value))


def _format_percent(value: object) -> str:
    if pd.isna(value):
        return ""
    return f"{float(value):.2%}"


def _format_score(value: object) -> str:
    if pd.isna(value):
        return ""
    return f"{float(value):.4f}"


if __name__ == "__main__":
    raise SystemExit(main())
