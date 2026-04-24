"""Shared cache diagnostics helpers for preview and benchmark entrypoints."""

from __future__ import annotations

from collections.abc import Mapping


SUMMARY_KEYS = (
    "cache_hits",
    "cache_misses",
    "cache_partial_hits",
    "cache_refresh_bypass",
    "file_scans",
    "remote_fills",
    "writebacks",
)


def diff_cache_stats(
    before: Mapping[str, object] | None,
    after: Mapping[str, object] | None,
) -> dict[str, int]:
    """Return non-negative cache stat deltas between two snapshots."""

    before_map = dict(before or {})
    after_map = dict(after or {})
    delta: dict[str, int] = {}
    for key in sorted(set(before_map) | set(after_map)):
        after_value = _safe_int(after_map.get(key, 0))
        before_value = _safe_int(before_map.get(key, 0))
        diff = max(after_value - before_value, 0)
        if diff:
            delta[str(key)] = diff
    return delta


def build_cache_observability_snapshot(
    stats: Mapping[str, object] | None,
) -> dict[str, object]:
    """Build the compact unified cache observability payload from raw stats."""

    stats_map = dict(stats or {})
    return {
        "cache_hits": _safe_int(stats_map.get("cache_resolution_hit_calls", 0)),
        "cache_misses": _safe_int(stats_map.get("cache_resolution_miss_calls", 0)),
        "cache_partial_hits": _safe_int(
            stats_map.get("cache_resolution_partial_hit_calls", 0)
        ),
        "cache_refresh_bypass": _safe_int(
            stats_map.get("cache_resolution_refresh_bypass_calls", 0)
        ),
        "file_scans": _safe_int(stats_map.get("cache_file_scan_calls", 0)),
        "remote_fills": _safe_int(stats_map.get("remote_fill_calls", 0)),
        "writebacks": _safe_int(stats_map.get("cache_writeback_calls", 0)),
        "stage_elapsed_ms": {
            str(key).split("stage_elapsed_ms::", 1)[1]: _safe_int(value)
            for key, value in stats_map.items()
            if str(key).startswith("stage_elapsed_ms::")
        },
        "stage_calls": {
            str(key).split("stage_calls::", 1)[1]: _safe_int(value)
            for key, value in stats_map.items()
            if str(key).startswith("stage_calls::")
        },
    }


def build_cache_diagnostics(
    stats: Mapping[str, object] | None = None,
    runtime_snapshot_reused: bool | None = None,
    top_stage_limit: int = 3,
) -> dict[str, object]:
    """Build a terminal-friendly structured cache diagnostics payload."""

    observability = build_cache_observability_snapshot(stats)
    stage_elapsed_ms = _mapping(observability.get("stage_elapsed_ms"))
    stage_calls = _mapping(observability.get("stage_calls"))
    top_stages: list[dict[str, object]] = []
    stage_limit = max(int(top_stage_limit), 0)
    if stage_limit > 0:
        for stage_name, elapsed_ms in sorted(
            stage_elapsed_ms.items(),
            key=lambda item: (-_safe_int(item[1]), str(item[0])),
        ):
            elapsed_value = _safe_int(elapsed_ms)
            if elapsed_value <= 0:
                continue
            top_stages.append(
                {
                    "name": str(stage_name),
                    "elapsed_ms": elapsed_value,
                    "calls": _safe_int(stage_calls.get(stage_name, 0)),
                }
            )
            if len(top_stages) >= stage_limit:
                break

    return {
        "summary": {
            key: _safe_int(observability.get(key, 0))
            for key in SUMMARY_KEYS
        },
        "layers": {
            "request_panel_memory": _layer_counts(
                stats,
                ("panel_memory_hit_calls", "panel_memory_miss_calls", "panel_memory_save_calls"),
                aliases=("hits", "misses", "saves"),
            ),
            "aggregate_memory": _layer_counts(
                stats,
                ("aggregate_memory_hit_calls", "aggregate_memory_miss_calls"),
                aliases=("hits", "misses"),
            ),
            "aggregate_metadata_memory": _layer_counts(
                stats,
                (
                    "aggregate_metadata_memory_hit_calls",
                    "aggregate_metadata_memory_miss_calls",
                ),
                aliases=("hits", "misses"),
            ),
        },
        "top_stages": top_stages,
        "runtime_snapshot_reused": runtime_snapshot_reused,
    }


def render_cache_diagnostic_lines(
    diagnostics: Mapping[str, object] | None,
    detail_level: str = "summary",
) -> list[str]:
    """Render cache diagnostics into readable terminal lines."""

    payload = _mapping(diagnostics)
    summary = _mapping(payload.get("summary"))
    layers = _mapping(payload.get("layers"))
    panel = _mapping(layers.get("request_panel_memory"))
    aggregate = _mapping(layers.get("aggregate_memory"))
    aggregate_metadata = _mapping(layers.get("aggregate_metadata_memory"))
    runtime_snapshot_reused = payload.get("runtime_snapshot_reused")

    reuse_label = "不适用"
    if runtime_snapshot_reused is True:
        reuse_label = "命中"
    elif runtime_snapshot_reused is False:
        reuse_label = "未命中"

    layer_line = (
        f"复用层: runtime snapshot={reuse_label}"
        f" | request panel hit/miss/save="
        f"{_safe_int(panel.get('hits', 0))}/{_safe_int(panel.get('misses', 0))}/{_safe_int(panel.get('saves', 0))}"
        f" | aggregate hit/miss="
        f"{_safe_int(aggregate.get('hits', 0))}/{_safe_int(aggregate.get('misses', 0))}"
    )
    if detail_level == "verbose" or any(_safe_int(value) for value in aggregate_metadata.values()):
        layer_line += (
            " | aggregate metadata hit/miss="
            f"{_safe_int(aggregate_metadata.get('hits', 0))}/{_safe_int(aggregate_metadata.get('misses', 0))}"
        )

    lines = [layer_line]
    lines.append(
        "统一观测: "
        f"hit/miss/partial/refresh={_safe_int(summary.get('cache_hits', 0))}/"
        f"{_safe_int(summary.get('cache_misses', 0))}/"
        f"{_safe_int(summary.get('cache_partial_hits', 0))}/"
        f"{_safe_int(summary.get('cache_refresh_bypass', 0))}"
        f" | remote/file/write="
        f"{_safe_int(summary.get('remote_fills', 0))}/"
        f"{_safe_int(summary.get('file_scans', 0))}/"
        f"{_safe_int(summary.get('writebacks', 0))}"
    )

    top_stages = payload.get("top_stages")
    if detail_level == "verbose" and isinstance(top_stages, list) and top_stages:
        lines.append("主要阶段耗时:")
        for item in top_stages:
            if not isinstance(item, Mapping):
                continue
            lines.append(
                f"- {str(item.get('name', ''))}: "
                f"{_safe_int(item.get('elapsed_ms', 0))}ms / "
                f"{_safe_int(item.get('calls', 0))}次"
            )
    return lines


def _layer_counts(
    stats: Mapping[str, object] | None,
    keys: tuple[str, ...],
    aliases: tuple[str, ...],
) -> dict[str, int]:
    stats_map = dict(stats or {})
    return {
        alias: _safe_int(stats_map.get(key, 0))
        for key, alias in zip(keys, aliases, strict=False)
    }


def _mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    return {}


def _safe_int(value: object) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return 0
