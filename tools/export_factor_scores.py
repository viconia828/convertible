"""Interactive/CLI entrypoint for exporting factor scores."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scoring_exports import (  # noqa: E402
    build_factor_score_report,
    normalize_cb_codes,
    write_factor_score_xlsx,
)
from runtime_hints import (  # noqa: E402
    build_proxy_startup_hints,
    build_tushare_failure_hints,
)
from strategy_config import load_strategy_parameters  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Export factor scores to XLSX.")
    parser.add_argument("--start-date", help="Start date, e.g. 2026-04-01")
    parser.add_argument("--end-date", help="End date, e.g. 2026-04-30")
    parser.add_argument(
        "--codes",
        help="One or more CB codes; 6-digit codes auto-complete exchange suffix",
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
    args = parser.parse_args()

    config = load_strategy_parameters(args.config)
    interactive = args.interactive or not (args.start_date and args.end_date and args.codes)
    start_date = args.start_date
    end_date = args.end_date
    codes = args.codes
    refresh = config.factor.export_default_refresh
    if args.refresh:
        refresh = True
    elif args.no_refresh:
        refresh = False
    startup_hints = build_proxy_startup_hints()

    if interactive:
        print("=" * 60)
        print("可转债多因子 - 因子打分导出")
        print(f"参数文件: {config.path}")
        print("输出内容: 指定日期窗口内、指定转债代码的逐日因子分数 XLSX")
        print("日期格式示例: 2026-04-01")
        print(
            "代码格式示例: 110073.SH,128044.SZ 或 110073 128044 "
            f"(单次上限 {config.exports.factor_max_codes_per_run} 只)"
        )
        print("输入六位数字时代码会自动补全交易所后缀。")
        print("取数规则: 默认缓存优先；发现覆盖不足时自动补数。")
        print(
            "刷新策略: 交互式运行不再单独询问；默认值来自策略参数文件中的 "
            "`factor.export_default_refresh`。"
        )
        if startup_hints:
            print("运行提示:")
            for hint in startup_hints:
                print(f"- {hint}")
        print("=" * 60)
        start_date = input("请输入开始日期: ").strip()
        end_date = input("请输入结束日期: ").strip()
        codes = input("请输入一个或多个可转债代码: ").strip()
    elif startup_hints:
        for hint in startup_hints:
            print(f"[提示] {hint}")

    if refresh:
        print("[模式] 强制刷新模式：将尽量重拉本次所需远端数据，并覆盖相关缓存。")
    else:
        print("[模式] 默认模式：缓存优先，发现覆盖不足时自动补数。")

    try:
        normalized_codes = normalize_cb_codes(codes or "")
        report = build_factor_score_report(
            start_date=start_date,
            end_date=end_date,
            codes=normalized_codes,
            refresh=refresh,
            config=config,
        )
        output_path = write_factor_score_xlsx(
            report=report,
            output_path=args.output,
            config=config,
        )
    except Exception as exc:
        print(f"[失败] 因子打分导出失败: {exc}")
        for hint in build_tushare_failure_hints(str(exc)):
            print(f"[提示] {hint}")
        return 1

    print()
    print("[完成] 因子打分导出成功")
    print(f"输出文件: {output_path}")
    print(f"代码数量: {len(report.codes)}")
    print(f"记录行数: {len(report.scores)}")
    actual_start = pd.Timestamp(report.scores["trade_date"].min()).strftime("%Y-%m-%d")
    actual_end = pd.Timestamp(report.scores["trade_date"].max()).strftime("%Y-%m-%d")
    print(f"实际输出区间: {actual_start} ~ {actual_end}")
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
