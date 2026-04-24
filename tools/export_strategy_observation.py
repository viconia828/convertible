"""Interactive/CLI entrypoint for exporting one-date strategy observation to XLSX."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.strategy_config import load_strategy_parameters  # noqa: E402
from exports.strategy_exports import (  # noqa: E402
    build_strategy_observation_report,
    write_strategy_observation_xlsx,
)
from tools.runtime_hints import (  # noqa: E402
    build_proxy_startup_hints,
    build_tushare_failure_hints,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Export one-date strategy observation to XLSX."
    )
    parser.add_argument("--trade-date", help="Trade date, e.g. 2026-04-20")
    parser.add_argument(
        "--codes",
        help="Optional watchlist codes; 6-digit codes auto-complete exchange suffix",
    )
    parser.add_argument("--output", help="Optional explicit XLSX path")
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
    args = parser.parse_args(argv)

    config = load_strategy_parameters(args.config)
    interactive = args.interactive or not args.trade_date
    trade_date = args.trade_date
    codes = args.codes or ""
    refresh = False
    if args.refresh:
        refresh = True
    elif args.no_refresh:
        refresh = False
    startup_hints = build_proxy_startup_hints()

    if interactive:
        print("=" * 60)
        print("可转债多因子 - strategy 单交易日观察导出")
        print(f"参数文件: {config.path}")
        print("输出内容: 单个交易日的环境分数、因子权重、目标组合与观察明细 XLSX")
        print("日期格式示例: 2026-04-20")
        print("可选输入: 候选代码 / 观察名单，用于额外输出聚焦观察 sheet")
        print("代码格式示例: 110073.SH,128044.SZ 或 110073 128044")
        print("输入六位数字时代码会自动补全交易所后缀。")
        print("取数规则: 默认缓存优先；发现覆盖不足时自动补数。")
        if startup_hints:
            print("运行提示:")
            for hint in startup_hints:
                print(f"- {hint}")
        print("=" * 60)
        trade_date = input("请输入交易日: ").strip()
        codes = input("可选，请输入候选代码 / 观察名单: ").strip()
    elif startup_hints:
        for hint in startup_hints:
            print(f"[提示] {hint}")

    if refresh:
        print("[模式] 强制刷新模式：将尽量重拉本次所需远端数据，并覆盖相关缓存。")
    else:
        print("[模式] 默认模式：缓存优先，发现覆盖不足时自动补数。")

    try:
        report = build_strategy_observation_report(
            trade_date=trade_date,
            requested_codes=codes,
            refresh=refresh,
            config=config,
        )
        output_path = write_strategy_observation_xlsx(
            report=report,
            output_path=args.output,
            config=config,
        )
    except Exception as exc:
        print(f"[失败] strategy 单交易日观察导出失败: {exc}")
        for hint in build_tushare_failure_hints(str(exc)):
            print(f"[提示] {hint}")
        return 1

    print()
    print("[完成] strategy 单交易日观察导出成功")
    print(f"输出文件: {output_path}")
    print(f"交易日: {report.trade_date.strftime('%Y-%m-%d')}")
    print(f"可建仓标的数: {report.eligible_count}")
    print(f"目标组合标的数: {report.selected_count}")
    print(f"现金权重: {report.cash_weight:.2%}")
    print(f"观察名单代码数: {len(report.requested_codes)}")
    print(f"数据质量状态: {report.data_quality_status}")
    if report.data_quality_status == "警告":
        print("[警告] 当前计算结果可能受数据不完整影响，请勿直接据此做投资判断。")
    if report.notes:
        print("提示:")
        for note in report.notes:
            print(f"- {note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
