from __future__ import annotations

import unittest

from env.macro_alignment import MacroAlignmentSummary
from reporting_semantics import (
    DATA_QUALITY_STATUS_OK,
    DATA_QUALITY_STATUS_WARNING,
    build_data_quality_warning_note,
    format_alignment_summary,
    resolve_data_quality_status,
    yes_no_label,
)


class ReportingSemanticsTests(unittest.TestCase):
    def test_yes_no_label_formats_bool_like_values(self) -> None:
        self.assertEqual(yes_no_label(True), "是")
        self.assertEqual(yes_no_label(False), "否")
        self.assertEqual(yes_no_label(1), "是")
        self.assertEqual(yes_no_label(0), "否")

    def test_resolve_data_quality_status_maps_issue_flag(self) -> None:
        self.assertEqual(resolve_data_quality_status(False), DATA_QUALITY_STATUS_OK)
        self.assertEqual(resolve_data_quality_status(True), DATA_QUALITY_STATUS_WARNING)

    def test_build_data_quality_warning_note_contains_context(self) -> None:
        note = build_data_quality_warning_note("策略预览")

        self.assertIn("策略预览", note)
        self.assertIn("请勿直接据此做投资判断", note)

    def test_format_alignment_summary_returns_compact_string(self) -> None:
        summary = MacroAlignmentSummary(
            total_calendar_days=10,
            kept_days=9,
            dropped_days=1,
            filled_days_by_indicator={},
            invalid_days_by_indicator={},
        )

        self.assertEqual(
            format_alignment_summary(summary),
            "calendar=10, kept=9, dropped=1",
        )
        self.assertEqual(format_alignment_summary(None), "")


if __name__ == "__main__":
    unittest.main()
