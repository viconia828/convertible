"""Shared reporting semantics for exports and strategy previews."""

from __future__ import annotations

from env.macro_alignment import MacroAlignmentSummary


DEFAULT_FETCH_POLICY = "缓存优先，完整性优先"
DATA_QUALITY_STATUS_OK = "正常"
DATA_QUALITY_STATUS_WARNING = "警告"


def yes_no_label(value: object) -> str:
    """Render one boolean-like value into the standard Chinese label."""

    return "是" if bool(value) else "否"


def resolve_data_quality_status(has_issue: bool) -> str:
    """Resolve the standard data-quality status label."""

    return DATA_QUALITY_STATUS_WARNING if bool(has_issue) else DATA_QUALITY_STATUS_OK


def build_data_quality_warning_note(context: str) -> str:
    """Build the standard data-quality warning note."""

    return (
        f"{context}当前存在数据完整性风险，计算结果可能偏离真实可投资信号，"
        "请勿直接据此做投资判断。"
    )


def format_alignment_summary(summary: MacroAlignmentSummary | None) -> str:
    """Render one compact alignment summary string."""

    if summary is None:
        return ""
    return (
        f"calendar={summary.total_calendar_days}, "
        f"kept={summary.kept_days}, "
        f"dropped={summary.dropped_days}"
    )
