"""Interactive/CLI entrypoint for exporting environment scores."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scoring_exports import (  # noqa: E402
    build_environment_score_report,
    write_environment_score_xlsx,
)
from strategy_config import load_strategy_parameters  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Export environment scores to XLSX.")
    parser.add_argument("--start-date", help="Start date, e.g. 2026-04-01")
    parser.add_argument("--end-date", help="End date, e.g. 2026-04-30")
    parser.add_argument("--output", help="Optional explicit XLSX path")
    parser.add_argument("--config", help="Optional strategy parameter file path")
    parser.add_argument("--refresh", action="store_true", help="Refresh remote-backed caches")
    parser.add_argument("--interactive", action="store_true", help="Prompt for missing inputs")
    args = parser.parse_args()

    config = load_strategy_parameters(args.config)
    interactive = args.interactive or not (args.start_date and args.end_date)
    start_date = args.start_date
    end_date = args.end_date

    if interactive:
        print("=" * 60)
        print("可转债多因子 - 环境打分导出")
        print(f"参数文件: {config.path}")
        print("输出内容: 指定日期窗口内交易日的逐日环境分数 XLSX")
        print("日期格式示例: 2026-04-01")
        print("=" * 60)
        start_date = input("请输入开始日期: ").strip()
        end_date = input("请输入结束日期: ").strip()

    try:
        report = build_environment_score_report(
            start_date=start_date,
            end_date=end_date,
            refresh=args.refresh,
            config=config,
        )
        output_path = write_environment_score_xlsx(
            report=report,
            output_path=args.output,
            config=config,
        )
    except Exception as exc:
        print(f"[失败] 环境打分导出失败: {exc}")
        return 1

    print()
    print("[完成] 环境打分导出成功")
    print(f"输出文件: {output_path}")
    print(f"有效交易日: {len(report.scores)}")
    print(
        "对齐摘要: "
        f"calendar={report.summary.total_calendar_days}, "
        f"kept={report.summary.kept_days}, "
        f"dropped={report.summary.dropped_days}"
    )
    if report.notes:
        print("提示:")
        for note in report.notes:
            print(f"- {note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
