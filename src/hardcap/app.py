"""The gateway: an OpenAI-compatible endpoint that admits, routes, streams and settles."""

from __future__ import annotations

import asyncio
import math
import os
import time
from typing import Any

import aiohttp
import redis.asyncio as aioredis
from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest

from hardcap import metrics
from hardcap.config import Config, Route, Tenant
from hardcap.estimator import OutputEstimator, count_text_tokens, estimate_input_tokens
from hardcap.ledger import Admission, BudgetLedger
from hardcap.providers import SSE_DONE, Usage, sse
from hardcap.router import Dispatched, NoUpstreamAvailable, Router

_WRITE_ERRORS = (ConnectionResetError, ConnectionError, aiohttp.ClientConnectionResetError) \
    if hasattr(aiohttp, "ClientConnectionResetError") else (ConnectionResetError, ConnectionError)
_UPSTREAM_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError, OSError, ValueError, RuntimeError)


def _error(status: int, type_: str, message: str, code: str | None = None,
           headers: dict[str, str] | None = None) -> web.Response:
    body = {"error": {"type": type_, "message": message, "code": code}}
    return web.json_response(body, status=status, headers=headers)


class Gateway:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.policy = cfg.budget
        self.keys = cfg.tenant_by_key()
        self.estimator = OutputEstimator(cfg.budget)
        self.redis: aioredis.Redis | None = None
        self.session: aiohttp.ClientSession | None = None
        self.ledger: BudgetLedger | None = None
        self.router: Router | None = None

    # ---- lifecycle -------------------------------------------------------------------------

    async def startup(self, _app: web.Application) -> None:
        pool = aioredis.BlockingConnectionPool.from_url(
            self.cfg.redis_url, max_connections=self.cfg.redis_max_connections, timeout=10)
        self.redis = aioredis.Redis(connection_pool=pool)
        await self.redis.ping()
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=0, ttl_dns_cache=300, keepalive_timeout=30),
            read_bufsize=2**20,
        )
        self.ledger = BudgetLedger(self.redis, self.policy, self.cfg.key_prefix)
        self.router = Router(self.cfg, self.session)

    async def shutdown(self, _app: web.Application) -> None:
        if self.session:
            await self.session.close()
        if self.redis:
            await self.redis.aclose()

    def build_app(self) -> web.Application:
        app = web.Application(client_max_size=32 * 2**20)
        app.on_startup.append(self.startup)
        app.on_cleanup.append(self.shutdown)
        app.router.add_post("/v1/chat/completions", self.chat)
        app.router.add_get("/v1/models", self.models)
        app.router.add_get("/v1/hardcap/usage", self.usage)
        app.router.add_get("/healthz", self.health)
        app.router.add_get("/metrics", self.metrics)
        return app

    # ---- small endpoints ---------------------------------------------------------------------

    def _auth(self, request: web.Request) -> Tenant | None:
        h = request.headers.get("authorization", "")
        key = h[7:].strip() if h.lower().startswith("bearer ") else request.headers.get("x-api-key", "")
        return self.keys.get(key)

    async def health(self, _request: web.Request) -> web.Response:
        try:
            await self.redis.ping()
        except Exception as e:  # noqa: BLE001
            return web.json_response({"ok": False, "redis": str(e)}, status=503)
        breakers = {n: b.state for n, b in self.router.breakers.items()}
        return web.json_response({"ok": True, "breakers": breakers})

    async def metrics(self, _request: web.Request) -> web.Response:
        if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
            from prometheus_client import multiprocess

            registry = CollectorRegistry()
            multiprocess.MultiProcessCollector(registry)
            data = generate_latest(registry)
        else:
            data = generate_latest()
        return web.Response(body=data, headers={"content-type": CONTENT_TYPE_LATEST})

    async def models(self, _request: web.Request) -> web.Response:
        data = [{"id": r, "object": "model", "owned_by": "hardcap"} for r in self.cfg.routes]
        return web.json_response({"object": "list", "data": data})

    async def usage(self, request: web.Request) -> web.Response:
        tenant = self._auth(request)
        if tenant is None:
            return _error(401, "invalid_request_error", "invalid api key", "invalid_api_key")
        u = await self.ledger.usage(tenant)
        b = tenant.budget
        return web.json_response({
            "tenant": tenant.name, "window_s": b.window_s,
            "limits": {"input": b.input, "output": b.output, "total": b.total},
            "used": {"input": u.get("in", 0), "output": u.get("out", 0)},
            "reserved": {"input": u.get("rin", 0), "output": u.get("rout", 0)},
        })

    # ---- chat completions ------------------------------------------------------------------

    async def chat(self, request: web.Request) -> web.StreamResponse:
        t0 = time.perf_counter()
        tenant = self._auth(request)
        if tenant is None:
            return _error(401, "invalid_request_error", "invalid api key", "invalid_api_key")
        try:
            body: dict[str, Any] = await request.json()
            assert isinstance(body, dict)
        except Exception:  # noqa: BLE001
            return _error(400, "invalid_request_error", "body must be a JSON object")
        route = self.cfg.routes.get(body.get("model", ""))
        if route is None:
            return _error(404, "invalid_request_error", f"unknown model {body.get('model')!r}",
                          "model_not_found")

        p = self.policy
        stream = bool(body.get("stream"))
        cap_key = "max_completion_tokens" if "max_completion_tokens" in body else "max_tokens"
        max_tokens = int(body.get(cap_key) or p.default_max_tokens)
        est_in = estimate_input_tokens(body)
        est_out = self.estimator.estimate(route.name, max_tokens)
        # Non-streaming responses cannot be cut off mid-way, so in truncate mode they reserve as
        # much as the budget allows (at least the estimate) and the upstream cap is set to it.
        elastic = p.accounting == "reserve" and p.on_overrun == "truncate" and not stream
        adm = await self.ledger.admit(tenant, est_in, est_out, max_tokens if elastic else None)
        if not adm.ok:
            metrics.REJECTIONS.labels(tenant.name, adm.reason).inc()
            metrics.REQUESTS.labels(tenant.name, route.name, "429").inc()
            type_, msg = {
                "rate": ("rate_limit_exceeded", "request rate limit exceeded"),
                "busy": ("budget_busy", "remaining budget is held by in-flight requests; retry"),
            }.get(adm.reason, ("budget_exceeded", f"{adm.reason} token budget exhausted"))
            return _error(429, type_, msg, adm.reason,
                          headers={"retry-after": str(max(1, math.ceil(adm.retry_after_s)))})

        up_body = dict(body)
        up_body[cap_key] = adm.granted_out if elastic else max_tokens
        headers = {"x-hardcap-tenant": tenant.name} if self.cfg.forward_tenant_header else {}
        metrics.ADMISSION_LATENCY.observe(time.perf_counter() - t0)

        try:
            d = await self.router.send(route, up_body, headers)
        except NoUpstreamAvailable as e:
            await self._settle(tenant, adm, 0, 0)
            metrics.REQUESTS.labels(tenant.name, route.name, str(e.status)).inc()
            return _error(e.status, "upstream_error", e.detail, "upstream_unavailable",
                          headers={"x-hardcap-attempts": ",".join(e.attempts)})
        if d.target is not route.targets[0]:
            metrics.FALLBACKS.labels(route.name, d.target.upstream).inc()

        if d.resp.status != 200:  # non-retryable client error from upstream: pass it through
            text = await d.resp.text()
            d.resp.release()
            await self._settle(tenant, adm, 0, 0)
            metrics.REQUESTS.labels(tenant.name, route.name, str(d.resp.status)).inc()
            return web.Response(status=d.resp.status, text=text, content_type="application/json")

        try:
            if stream:
                return await self._stream(request, tenant, route, body, adm, d, est_in, t0)
            return await self._unary(tenant, route, adm, d, est_in, t0)
        finally:
            d.resp.release()

    async def _unary(self, tenant: Tenant, route: Route, adm: Admission, d: Dispatched,
                     est_in: int, t0: float) -> web.Response:
        try:
            data = await d.resp.json(content_type=None)
        except _UPSTREAM_ERRORS as e:
            self.router.report_stream_failure(d.target.upstream)
            await self._settle(tenant, adm, 0, 0)
            metrics.REQUESTS.labels(tenant.name, route.name, "502").inc()
            return _error(502, "upstream_error", f"bad upstream body: {type(e).__name__}")
        out, usage = d.adapter.parse(data)
        if usage is None:
            text = "".join((c.get("message") or {}).get("content") or "" for c in out.get("choices", []))
            usage = Usage(adm.reserved_in, count_text_tokens(text))
        await self._finish(tenant, route, d, adm, usage, completed=True)
        metrics.REQUESTS.labels(tenant.name, route.name, "200").inc()
        metrics.REQUEST_LATENCY.labels(route.name, "false").observe(time.perf_counter() - t0)
        return web.json_response(out, headers={"x-hardcap-upstream": d.target.upstream})

    async def _stream(self, request: web.Request, tenant: Tenant, route: Route,
                      body: dict[str, Any], adm: Admission, d: Dispatched, est_in: int,
                      t0: float) -> web.StreamResponse:
        p = self.policy
        reserve = p.accounting == "reserve"
        truncate = reserve and p.on_overrun == "truncate"
        want_usage = bool((body.get("stream_options") or {}).get("include_usage"))
        low_water = max(1, p.extend_chunk // 2)
        refresh_every = p.reservation_ttl_s / 3

        out = web.StreamResponse(headers={"content-type": "text/event-stream",
                                          "cache-control": "no-cache",
                                          "x-hardcap-upstream": d.target.upstream})
        await out.prepare(request)
        counted, usage = 0, None
        denied = truncated = completed = client_gone = False
        status = "200"
        try:
            async for ev in d.adapter.stream(d.resp):
                if ev.usage is not None:
                    usage = ev.usage
                counted += ev.tokens
                if ev.done:
                    completed = True
                if ev.data is not None and not (ev.usage_only and not want_usage):
                    try:
                        await out.write(ev.data)
                    except _WRITE_ERRORS:
                        client_gone = True
                        status = "499"
                        break
                if ev.done:
                    break
                if not reserve:
                    continue
                # Top up before the reservation runs dry, so the stream never runs on credit.
                # After a denial, truncate mode asks once more at the boundary (other requests
                # may have settled and freed budget) before cutting the stream.
                dry = counted >= adm.granted_out
                if counted + low_water > adm.granted_out and (not denied or (truncate and dry)):
                    need = max(p.extend_chunk, counted + low_water - adm.granted_out)
                    denied = await self.ledger.extend(tenant, adm, need) != 1
                elif time.monotonic() - adm.last_refresh > refresh_every:
                    await self.ledger.extend(tenant, adm, 0)
                if truncate and denied and counted >= adm.granted_out:
                    truncated = True
                    break
        except _UPSTREAM_ERRORS as e:
            self.router.report_stream_failure(d.target.upstream)
            status = "502"
            if not client_gone:
                err = {"error": {"type": "upstream_error", "message": type(e).__name__}}
                await self._try_write(out, sse(err) + SSE_DONE)

        if truncated:
            # Closing the upstream connection stops generation (and billing) there.
            d.resp.close()
            metrics.TRUNCATIONS.labels(tenant.name, route.name).inc()
            final = {"object": "chat.completion.chunk", "model": body.get("model"),
                     "choices": [{"index": 0, "delta": {}, "finish_reason": "length"}],
                     "hardcap": {"truncated": "budget"}}
            tail = sse(final)
            if want_usage:
                tail += sse({"object": "chat.completion.chunk", "choices": [],
                             "usage": {"prompt_tokens": est_in, "completion_tokens": counted,
                                       "total_tokens": est_in + counted}})
            await self._try_write(out, tail + SSE_DONE)
        elif client_gone or not completed:
            d.resp.close()

        if usage is None or truncated or not completed:
            usage = Usage(usage.prompt_tokens if usage else est_in, counted)
        await self._finish(tenant, route, d, adm, usage, completed=completed and not truncated)
        metrics.REQUESTS.labels(tenant.name, route.name, status).inc()
        metrics.REQUEST_LATENCY.labels(route.name, "true").observe(time.perf_counter() - t0)
        if not client_gone:
            await self._try_write_eof(out)
        return out

    # ---- settlement ------------------------------------------------------------------------

    async def _settle(self, tenant: Tenant, adm: Admission, act_in: int, act_out: int) -> None:
        # Shielded: a cancelled handler must not leak its reservation.
        await asyncio.shield(self.ledger.settle(tenant, adm, act_in, act_out))

    async def _finish(self, tenant: Tenant, route: Route, d: Dispatched, adm: Admission,
                      usage: Usage, completed: bool) -> None:
        await self._settle(tenant, adm, usage.prompt_tokens, usage.completion_tokens)
        model = d.target.model
        metrics.TOKENS.labels(tenant.name, route.name, model, "input").inc(usage.prompt_tokens)
        metrics.TOKENS.labels(tenant.name, route.name, model, "output").inc(usage.completion_tokens)
        price = self.cfg.pricing.get(model) or self.cfg.pricing.get(route.name)
        if price:
            cost = (usage.prompt_tokens * price.input + usage.completion_tokens * price.output) / 1e6
            metrics.COST.labels(tenant.name, route.name, model).inc(cost)
        if completed:
            self.estimator.observe(route.name, usage.completion_tokens)
            if self.policy.accounting == "reserve":
                metrics.RESERVATION_ERROR.labels(route.name).observe(
                    usage.completion_tokens - adm.initial_out)

    @staticmethod
    async def _try_write(out: web.StreamResponse, data: bytes) -> None:
        try:
            await out.write(data)
        except _WRITE_ERRORS:
            pass

    @staticmethod
    async def _try_write_eof(out: web.StreamResponse) -> None:
        try:
            await out.write_eof()
        except _WRITE_ERRORS:
            pass


def create_app(cfg: Config) -> web.Application:
    return Gateway(cfg).build_app()

