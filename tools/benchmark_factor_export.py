"""Benchmark real-sample factor score export time and memory usage."""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import sys
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.strategy_config import load_strategy_parameters  # noqa: E402
from exports.scoring_exports import (  # noqa: E402
    build_factor_score_report,
    write_factor_score_xlsx,
)
from data.data_loader import DataLoader  # noqa: E402
from shared.cache_diagnostics import build_cache_diagnostics, diff_cache_stats  # noqa: E402


try:  # pragma: no cover - optional dependency
    import psutil  # type: ignore
except Exception:  # noqa: BLE001
    psutil = None

_KERNEL32 = ctypes.WinDLL("kernel32")
_PSAPI = ctypes.WinDLL("psapi")
MEMORY_SAMPLE_INTERVAL_SECONDS = 0.25


@dataclass(frozen=True)
class BenchmarkResult:
    label: str
    start_date: str
    end_date: str
    repeat: int
    requested_codes: list[str]
    requested_code_count: int
    actual_trade_days: int
    score_rows: int
    diagnostics_rows: int
    actual_output_start: str
    actual_output_end: str
    data_quality_status: str
    iteration_build_seconds: list[float]
    iteration_write_seconds: list[float]
    build_seconds: float
    write_seconds: float
    total_seconds: float
    rss_before_mb: float
    rss_after_mb: float
    peak_rss_mb: float
    peak_rss_delta_mb: float
    cache_stats: dict[str, int]
    cache_observability: dict[str, object]
    cache_diagnostics: dict[str, object]
    iteration_cache_diagnostics: list[dict[str, object]]
    output_path: str


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark factor score export on real cached samples."
    )
    parser.add_argument("--start-date", required=True, help="Start date, e.g. 2026-01-05")
    parser.add_argument("--end-date", required=True, help="End date, e.g. 2026-04-17")
    parser.add_argument("--codes", help="Explicit comma-separated CB codes")
    parser.add_argument(
        "--top-codes",
        type=int,
        default=0,
        help="If --codes is omitted, pick the top-N liquid codes from the latest complete cross section",
    )
    parser.add_argument(
        "--label",
        default="factor_benchmark",
        help="Short label used in output naming and JSON result",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "导出结果" / "benchmark"),
        help="Directory for benchmark XLSX outputs",
    )
    parser.add_argument("--config", help="Optional strategy parameter file path")
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Repeat builds in the same process with the same DataLoader",
    )
    parser.add_argument(
        "--skip-write",
        action="store_true",
        help="Measure build only and skip XLSX writing",
    )
    args = parser.parse_args()
    repeat = max(int(args.repeat), 1)

    config = load_strategy_parameters(args.config)
    loader = DataLoader(config=config)
    codes = _resolve_codes(args.codes, args.top_codes)
    if not codes:
        raise ValueError("No benchmark codes resolved. Pass --codes or a positive --top-codes.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{args.label}.xlsx"

    rss_before = _current_rss_bytes()
    peak_rss = rss_before
    stop_event = threading.Event()
    sampler = threading.Thread(
        target=_sample_peak_rss,
        args=(
            stop_event,
            MEMORY_SAMPLE_INTERVAL_SECONDS,
            lambda value: _update_peak(value, peak_holder),
        ),
        daemon=True,
    )
    peak_holder = {"value": peak_rss}
    sampler.start()
    try:
        started = time.perf_counter()
        build_durations: list[float] = []
        write_durations: list[float] = []
        iteration_cache_diagnostics: list[dict[str, object]] = []
        report = None
        final_output_path = ""
        for iteration in range(repeat):
            cache_stats_before_iteration = loader.cache_service.stats_snapshot()
            build_started = time.perf_counter()
            report = build_factor_score_report(
                start_date=args.start_date,
                end_date=args.end_date,
                codes=codes,
                loader=loader,
                refresh=False,
                config=config,
            )
            built = time.perf_counter()
            build_durations.append(round(built - build_started, 3))
            should_write = not args.skip_write and iteration == repeat - 1
            if should_write:
                final_output_path = str(
                    write_factor_score_xlsx(
                        report=report,
                        output_path=output_path,
                        config=config,
                    )
                )
                written = time.perf_counter()
            else:
                written = built
            write_durations.append(round(written - built, 3))
            cache_stats_after_iteration = loader.cache_service.stats_snapshot()
            iteration_cache_diagnostics.append(
                {
                    "iteration": iteration + 1,
                    **build_cache_diagnostics(
                        diff_cache_stats(
                            cache_stats_before_iteration,
                            cache_stats_after_iteration,
                        )
                    ),
                }
            )
        finished = time.perf_counter()
        if report is None:
            raise RuntimeError("Benchmark did not build any report.")
    finally:
        stop_event.set()
        sampler.join(timeout=1.0)

    peak_rss = max(peak_holder["value"], _current_rss_bytes())
    rss_after = _current_rss_bytes()
    actual_start = (
        pd.Timestamp(report.scores["trade_date"].min()).strftime("%Y-%m-%d")
        if not report.scores.empty
        else ""
    )
    actual_end = (
        pd.Timestamp(report.scores["trade_date"].max()).strftime("%Y-%m-%d")
        if not report.scores.empty
        else ""
    )
    result = BenchmarkResult(
        label=args.label,
        start_date=args.start_date,
        end_date=args.end_date,
        repeat=repeat,
        requested_codes=codes,
        requested_code_count=len(codes),
        actual_trade_days=int(report.scores["trade_date"].nunique()) if not report.scores.empty else 0,
        score_rows=len(report.scores),
        diagnostics_rows=len(report.diagnostics),
        actual_output_start=actual_start,
        actual_output_end=actual_end,
        data_quality_status=str(report.data_quality_status),
        iteration_build_seconds=build_durations,
        iteration_write_seconds=write_durations,
        build_seconds=build_durations[-1],
        write_seconds=write_durations[-1],
        total_seconds=round(finished - started, 3),
        rss_before_mb=round(rss_before / 1024 / 1024, 1),
        rss_after_mb=round(rss_after / 1024 / 1024, 1),
        peak_rss_mb=round(peak_rss / 1024 / 1024, 1),
        peak_rss_delta_mb=round((peak_rss - rss_before) / 1024 / 1024, 1),
        cache_stats=loader.cache_service.stats_snapshot(),
        cache_observability=loader.cache_service.observability_snapshot(),
        cache_diagnostics=build_cache_diagnostics(loader.cache_service.stats_snapshot()),
        iteration_cache_diagnostics=iteration_cache_diagnostics,
        output_path=final_output_path,
    )
    print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    return 0


def _resolve_codes(codes_arg: str | None, top_codes: int) -> list[str]:
    if codes_arg:
        return [code.strip() for code in codes_arg.split(",") if code.strip()]
    if top_codes <= 0:
        return []

    latest_path = _latest_complete_cross_section_path()
    frame = pd.read_csv(latest_path)
    if frame.empty:
        return []
    return (
        frame.dropna(subset=["cb_code", "amount"])
        .sort_values("amount", ascending=False, kind="stable")
        ["cb_code"]
        .astype(str)
        .drop_duplicates()
        .head(top_codes)
        .tolist()
    )


def _latest_complete_cross_section_path() -> Path:
    base_dir = PROJECT_ROOT / "data" / "cache" / "tushare" / "time_series" / "cb_daily_cross_section"
    candidates = [
        path for path in sorted(base_dir.glob("*.csv")) if path.stat().st_size > 1024
    ]
    if not candidates:
        raise FileNotFoundError("No complete cb_daily_cross_section cache files found.")
    return candidates[-1]


def _sample_peak_rss(
    stop_event: threading.Event,
    interval_seconds: float,
    callback,
) -> None:
    while not stop_event.is_set():
        callback(_current_rss_bytes())
        time.sleep(interval_seconds)
    callback(_current_rss_bytes())


def _update_peak(value: int, peak_holder: dict[str, int]) -> None:
    peak_holder["value"] = max(peak_holder["value"], value)


def _current_rss_bytes() -> int:
    if psutil is not None:
        return int(psutil.Process(os.getpid()).memory_info().rss)
    counters = _ProcessMemoryCountersEx()
    counters.cb = ctypes.sizeof(_ProcessMemoryCountersEx)
    _KERNEL32.GetCurrentProcess.restype = ctypes.c_void_p
    _PSAPI.GetProcessMemoryInfo.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_ulong,
    ]
    _PSAPI.GetProcessMemoryInfo.restype = ctypes.c_int
    handle = _KERNEL32.GetCurrentProcess()
    success = _PSAPI.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb)
    if not success:
        return 0
    return int(counters.WorkingSetSize)


class _ProcessMemoryCountersEx(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_ulong),
        ("PageFaultCount", ctypes.c_ulong),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
        ("PrivateUsage", ctypes.c_size_t),
    ]


if __name__ == "__main__":
    raise SystemExit(main())
