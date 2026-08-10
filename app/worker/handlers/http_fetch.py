import ipaddress
import socket
import time
from urllib.parse import urlparse

import httpx

from app.worker.handlers import JobContext, handler


def _assert_public_host(url: str) -> None:
    """SSRF guard: a job payload is user input, so refuse URLs that resolve to private,
    loopback or link-local addresses — otherwise jobs could probe the internal network
    the workers run in."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError(f"unsupported URL: {url!r}")
    for info in socket.getaddrinfo(parsed.hostname, None):
        addr = ipaddress.ip_address(info[4][0])
        if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved:
            raise ValueError(f"refusing to fetch non-public address {addr} for host {parsed.hostname}")


@handler("http_fetch")
async def http_fetch(payload: dict, ctx: JobContext) -> dict:
    """Fetch a URL and report status + latency. Non-2xx/3xx responses raise, so a dead
    endpoint exercises the retry/backoff path with real network errors."""
    url = payload["url"]
    method = str(payload.get("method", "GET")).upper()
    if method not in ("GET", "HEAD"):
        raise ValueError("http_fetch supports only GET and HEAD")
    _assert_public_host(url)

    start = time.perf_counter()
    async with httpx.AsyncClient(timeout=15, follow_redirects=True, max_redirects=3) as client:
        response = await client.request(method, url)
    latency_ms = round((time.perf_counter() - start) * 1000, 1)
    if response.status_code >= 400:
        raise RuntimeError(f"HTTP {response.status_code} from {url} ({latency_ms}ms)")
    return {
        "url": url,
        "status_code": response.status_code,
        "latency_ms": latency_ms,
        "content_length": len(response.content),
        "content_type": response.headers.get("content-type", ""),
    }
