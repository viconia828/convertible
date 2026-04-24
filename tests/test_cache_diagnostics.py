from __future__ import annotations

import unittest

from shared.cache_diagnostics import (
    build_cache_diagnostics,
    build_cache_observability_snapshot,
    diff_cache_stats,
    render_cache_diagnostic_lines,
)


class CacheDiagnosticsTests(unittest.TestCase):
    def test_diff_cache_stats_keeps_positive_delta_only(self) -> None:
        delta = diff_cache_stats(
            {"cache_resolution_hit_calls": 1, "remote_fill_calls": 2},
            {"cache_resolution_hit_calls": 4, "remote_fill_calls": 2},
        )

        self.assertEqual(delta, {"cache_resolution_hit_calls": 3})

    def test_build_cache_diagnostics_summarizes_layers_and_top_stages(self) -> None:
        diagnostics = build_cache_diagnostics(
            {
                "cache_resolution_hit_calls": 5,
                "cache_resolution_partial_hit_calls": 1,
                "remote_fill_calls": 2,
                "cache_file_scan_calls": 4,
                "panel_memory_hit_calls": 3,
                "panel_memory_miss_calls": 1,
                "panel_memory_save_calls": 1,
                "aggregate_memory_hit_calls": 2,
                "aggregate_memory_miss_calls": 1,
                "aggregate_metadata_memory_hit_calls": 4,
                "stage_calls::cb_daily_cross_section::request_panel_lookup": 2,
                "stage_elapsed_ms::cb_daily_cross_section::request_panel_lookup": 21,
                "stage_calls::cb_daily_cross_section::remote_fetch": 1,
                "stage_elapsed_ms::cb_daily_cross_section::remote_fetch": 9,
            },
            runtime_snapshot_reused=False,
        )

        self.assertEqual(int(diagnostics["summary"]["cache_hits"]), 5)
        self.assertEqual(int(diagnostics["summary"]["cache_partial_hits"]), 1)
        self.assertEqual(int(diagnostics["summary"]["remote_fills"]), 2)
        self.assertEqual(
            int(diagnostics["layers"]["request_panel_memory"]["hits"]),
            3,
        )
        self.assertEqual(
            int(diagnostics["layers"]["aggregate_memory"]["hits"]),
            2,
        )
        self.assertEqual(
            diagnostics["top_stages"][0]["name"],
            "cb_daily_cross_section::request_panel_lookup",
        )
        self.assertFalse(bool(diagnostics["runtime_snapshot_reused"]))

    def test_render_cache_diagnostic_lines_supports_summary_and_verbose(self) -> None:
        diagnostics = build_cache_diagnostics(
            {
                "cache_resolution_miss_calls": 2,
                "remote_fill_calls": 1,
                "cache_file_scan_calls": 6,
                "panel_memory_hit_calls": 1,
                "aggregate_memory_hit_calls": 2,
                "aggregate_metadata_memory_hit_calls": 1,
                "stage_calls::cb_daily_cross_section::aggregate_lookup": 1,
                "stage_elapsed_ms::cb_daily_cross_section::aggregate_lookup": 12,
            },
            runtime_snapshot_reused=True,
        )

        summary_lines = render_cache_diagnostic_lines(diagnostics, detail_level="summary")
        verbose_lines = render_cache_diagnostic_lines(diagnostics, detail_level="verbose")

        self.assertIn("runtime snapshot=命中", summary_lines[0])
        self.assertIn("request panel hit/miss/save=1/0/0", summary_lines[0])
        self.assertIn("aggregate hit/miss=2/0", summary_lines[0])
        self.assertIn("统一观测:", summary_lines[1])
        self.assertTrue(any("aggregate metadata hit/miss=1/0" in line for line in verbose_lines))
        self.assertTrue(any("主要阶段耗时:" == line for line in verbose_lines))
        self.assertTrue(any("aggregate_lookup" in line for line in verbose_lines))

    def test_build_cache_observability_snapshot_reads_stage_maps(self) -> None:
        summary = build_cache_observability_snapshot(
            {
                "cache_resolution_hit_calls": 2,
                "stage_calls::cb_daily::remote_fetch": 1,
                "stage_elapsed_ms::cb_daily::remote_fetch": 11,
            }
        )

        self.assertEqual(int(summary["cache_hits"]), 2)
        self.assertEqual(int(summary["stage_calls"]["cb_daily::remote_fetch"]), 1)
        self.assertEqual(int(summary["stage_elapsed_ms"]["cb_daily::remote_fetch"]), 11)


if __name__ == "__main__":
    unittest.main()
