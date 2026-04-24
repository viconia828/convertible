"""User-facing runtime hints for common local environment issues."""

from __future__ import annotations

import os
import urllib.parse


PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "http_proxy",
    "https_proxy",
)


def build_proxy_startup_hints() -> tuple[str, ...]:
    proxies = _collect_proxy_values()
    if not proxies:
        return ()

    unique_values = list(dict.fromkeys(proxies.values()))
    if all(_is_loopback_blackhole_proxy(value) for value in unique_values):
        return (
            "检测到 HTTP(S)_PROXY 指向本机 9 端口，程序会尝试绕过该代理直连 Tushare。",
            "如果后续仍然取数失败，请检查系统代理、终端代理或公司网络策略。",
        )

    return (
        "检测到当前环境存在 HTTP(S)_PROXY 设置；如果 Tushare 取数失败，请优先检查代理是否可用。",
    )


def build_tushare_failure_hints(error_message: str) -> tuple[str, ...]:
    proxies = _collect_proxy_values()
    if not proxies:
        return ()

    unique_values = list(dict.fromkeys(proxies.values()))
    if any(_is_loopback_blackhole_proxy(value) for value in unique_values):
        return (
            "当前环境变量中的 HTTP(S)_PROXY 指向本机 9 端口，这通常是占位或阻断型代理配置。",
            "如果仍然无法取数，请先清理这些代理变量后重试。",
        )

    lowered = error_message.lower()
    if "urlopen error" in lowered or "tushare request failed" in lowered:
        return (
            "当前环境存在 HTTP(S)_PROXY 设置，这类错误常见于代理不可用或代理拦截了 Tushare 请求。",
            "建议先关闭代理后重试，或检查代理软件是否正在运行。",
        )
    return ()


def _collect_proxy_values() -> dict[str, str]:
    proxies: dict[str, str] = {}
    for key in PROXY_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            proxies[key] = value
    return proxies


def _is_loopback_blackhole_proxy(proxy_url: str) -> bool:
    parsed = urllib.parse.urlparse(proxy_url)
    host = (parsed.hostname or "").strip("[]").lower()
    port = parsed.port
    return host in {"127.0.0.1", "localhost", "::1"} and port == 9
