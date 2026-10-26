"""Model routing with ordered fallback and per-upstream circuit breakers.

A target is retried on the next one in the chain on connection errors, timeouts, 429 and 5xx,
but only before any byte has been sent to the client. Other 4xx responses are the client's
problem and are returned as-is."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from hardcap import metrics
from hardcap.breaker import CircuitBreaker
from hardcap.config import Config, Route, Target, Upstream
from hardcap.providers import adapter_for


@dataclass
class Dispatched:
    target: Target
    upstream: Upstream
    adapter: Any
    resp: aiohttp.ClientResponse
    attempts: list[str] = field(default_factory=list)


class NoUpstreamAvailable(Exception):
    def __init__(self, status: int, detail: str, attempts: list[str]):
        super().__init__(detail)
        self.status = status
        self.detail = detail
        self.attempts = attempts


def _retryable(status: int) -> bool:
    return status == 429 or status >= 500


class Router:
    def __init__(self, cfg: Config, session: aiohttp.ClientSession):
        self.cfg = cfg
        self.session = session
        self.breakers = {
            n: CircuitBreaker(cfg.breaker.failure_threshold, cfg.breaker.cooldown_s)
            for n in cfg.upstreams
        }
        self.adapters = {n: adapter_for(u, cfg.budget.default_max_tokens) for n, u in cfg.upstreams.items()}
        self.timeouts = {
            n: aiohttp.ClientTimeout(total=u.total_timeout_s, connect=u.connect_timeout_s,
                                     sock_read=u.read_timeout_s)
            for n, u in cfg.upstreams.items()
        }

    def _fail(self, name: str) -> None:
        b = self.breakers[name]
        before = b.state
        b.record_failure()
        if b.state != before:
            metrics.BREAKER_TRANSITIONS.labels(name, b.state).inc()

    def _ok(self, name: str) -> None:
        b = self.breakers[name]
        if b.state != CircuitBreaker.CLOSED:
            metrics.BREAKER_TRANSITIONS.labels(name, CircuitBreaker.CLOSED).inc()
        b.record_success()

    async def send(self, route: Route, body: dict[str, Any], headers: dict[str, str]) -> Dispatched:
        attempts: list[str] = []
        last_status, last_detail = 503, "all upstreams have open circuits"
        for target in route.targets:
            name = target.upstream
            if not self.breakers[name].allow():
                attempts.append(f"{name}:circuit_open")
                continue
            up = self.cfg.upstreams[name]
            adapter = self.adapters[name]
            url, up_headers, payload = adapter.build(up, target.model, body)
            up_headers.update(headers)
            t0 = time.perf_counter()
            try:
                resp = await self.session.post(url, json=payload, headers=up_headers,
                                               timeout=self.timeouts[name])
            except (TimeoutError, aiohttp.ClientError, OSError) as e:
                self._fail(name)
                attempts.append(f"{name}:{type(e).__name__}")
                metrics.UPSTREAM_ERRORS.labels(name, type(e).__name__).inc()
                last_status, last_detail = 502, f"upstream {name}: {type(e).__name__}"
                continue
            metrics.UPSTREAM_HEADERS_LATENCY.labels(name).observe(time.perf_counter() - t0)
            if _retryable(resp.status):
                self._fail(name)
                attempts.append(f"{name}:{resp.status}")
                metrics.UPSTREAM_ERRORS.labels(name, str(resp.status)).inc()
                last_status, last_detail = resp.status, await _safe_text(resp)
                resp.release()
                continue
            self._ok(name)
            attempts.append(f"{name}:{resp.status}")
            return Dispatched(target, up, adapter, resp, attempts)
        raise NoUpstreamAvailable(last_status if last_status != 429 else 503, last_detail, attempts)

    def report_stream_failure(self, name: str) -> None:
        self._fail(name)


async def _safe_text(resp: aiohttp.ClientResponse) -> str:
    try:
        return (await resp.text())[:500]
    except Exception:
        return ""
