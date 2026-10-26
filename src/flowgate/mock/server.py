"""Mock LLM upstream speaking the OpenAI and Anthropic streaming formats.

Output lengths come from a log-normal distribution (real completion lengths are heavy-tailed),
capped at the request's max_tokens. It counts the tokens it actually wrote per tenant, using the
x-flowgate-tenant header, which is the ground truth the overshoot benchmark checks against: a
real provider bills what it generated, not what the gateway thinks it forwarded.

Per-request overrides go in a "mock" object in the request body:
    {"mock": {"output_tokens": 300, "ttft_ms": 20, "tpot_ms": 2}}
"""

from __future__ import annotations

import asyncio
import json
import math
import random
import time
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass

from aiohttp import web

from flowgate.estimator import estimate_input_tokens

TOKEN = "tok "  # 4 chars = 1 token under the gateway's chars/4 counter


@dataclass
class MockSettings:
    ttft_ms: float = 50.0
    tpot_ms: float = 5.0
    median_tokens: float = 150.0
    sigma: float = 0.8
    chunk_tokens: int = 1
    error_rate: float = 0.0
    error_status: int = 503
    seed: int | None = None


class MockLLM:
    def __init__(self, settings: MockSettings):
        self.s = settings
        self.rng = random.Random(settings.seed)
        self.stats: dict[str, dict[str, int]] = defaultdict(
            lambda: {"requests": 0, "prompt_tokens": 0, "completion_tokens": 0, "disconnects": 0})

    def build_app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/v1/chat/completions", self.openai)
        app.router.add_post("/v1/messages", self.anthropic)
        app.router.add_get("/admin/stats", self.get_stats)
        app.router.add_post("/admin/reset", self.reset)
        app.router.add_post("/admin/faults", self.faults)
        app.router.add_get("/healthz", self.health)
        return app

    # ---- admin --------------------------------------------------------------------------

    async def get_stats(self, _r: web.Request) -> web.Response:
        return web.json_response({"settings": asdict(self.s), "tenants": self.stats})

    async def health(self, _r: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    async def reset(self, _r: web.Request) -> web.Response:
        self.stats.clear()
        return web.json_response({"ok": True})

    async def faults(self, r: web.Request) -> web.Response:
        for k, v in (await r.json()).items():
            if hasattr(self.s, k):
                cur = getattr(self.s, k)
                setattr(self.s, k, type(cur)(v) if cur is not None and v is not None else v)
        return web.json_response(asdict(self.s))

    # ---- generation ---------------------------------------------------------------------

    def _plan(self, body: dict, cap: int) -> tuple[int, float, float]:
        m = body.get("mock") or {}
        if "output_tokens" in m:
            n = int(m["output_tokens"])
        else:
            n = int(round(math.exp(self.rng.gauss(math.log(self.s.median_tokens), self.s.sigma))))
        n = max(1, min(n, cap))
        return n, float(m.get("ttft_ms", self.s.ttft_ms)), float(m.get("tpot_ms", self.s.tpot_ms))

    def _fault(self) -> web.Response | None:
        if self.s.error_rate and self.rng.random() < self.s.error_rate:
            return web.json_response({"error": {"message": "injected fault"}}, status=self.s.error_status)
        return None

    async def _emit(self, request: web.Request, tenant: str, n: int, ttft: float, tpot: float,
                    frame, head: list[bytes], tail: list[bytes]) -> web.StreamResponse:
        st = self.stats[tenant]
        resp = web.StreamResponse(headers={"content-type": "text/event-stream"})
        await resp.prepare(request)
        try:
            if ttft:
                await asyncio.sleep(ttft / 1000)
            for h in head:
                await resp.write(h)
            sent, k = 0, max(1, self.s.chunk_tokens)
            while sent < n:
                step = min(k, n - sent)
                if tpot:
                    await asyncio.sleep(tpot * step / 1000)
                await resp.write(frame(TOKEN * step))
                sent += step
                st["completion_tokens"] += step  # counted once written: that is what gets billed
            for t in tail:
                await resp.write(t)
            await resp.write_eof()
        except (ConnectionResetError, ConnectionError):
            st["disconnects"] += 1
        return resp

    async def openai(self, request: web.Request) -> web.StreamResponse:
        if (f := self._fault()) is not None:
            return f
        body = await request.json()
        tenant = request.headers.get("x-flowgate-tenant", "anon")
        cap = int(body.get("max_completion_tokens") or body.get("max_tokens") or 4096)
        n, ttft, tpot = self._plan(body, cap)
        prompt = estimate_input_tokens(body)
        st = self.stats[tenant]
        st["requests"] += 1
        st["prompt_tokens"] += prompt
        rid, model, created = f"chatcmpl-{uuid.uuid4().hex[:12]}", body.get("model", "mock"), int(time.time())
        finish = "length" if n >= cap else "stop"
        usage = {"prompt_tokens": prompt, "completion_tokens": n, "total_tokens": prompt + n}

        if not body.get("stream"):
            await asyncio.sleep((ttft + tpot * n) / 1000)
            st["completion_tokens"] += n
            return web.json_response({
                "id": rid, "object": "chat.completion", "created": created, "model": model,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": TOKEN * n},
                             "finish_reason": finish}],
                "usage": usage,
            })

        def chunk(delta: dict, fin: str | None = None) -> bytes:
            c = {"id": rid, "object": "chat.completion.chunk", "created": created, "model": model,
                 "choices": [{"index": 0, "delta": delta, "finish_reason": fin}]}
            return b"data: " + json.dumps(c).encode() + b"\n\n"

        tail = [chunk({}, finish)]
        if (body.get("stream_options") or {}).get("include_usage"):
            u = {"id": rid, "object": "chat.completion.chunk", "created": created, "model": model,
                 "choices": [], "usage": usage}
            tail.append(b"data: " + json.dumps(u).encode() + b"\n\n")
        tail.append(b"data: [DONE]\n\n")
        return await self._emit(request, tenant, n, ttft, tpot,
                                lambda text: chunk({"content": text}),
                                [chunk({"role": "assistant", "content": ""})], tail)

    async def anthropic(self, request: web.Request) -> web.StreamResponse:
        if (f := self._fault()) is not None:
            return f
        body = await request.json()
        tenant = request.headers.get("x-flowgate-tenant", "anon")
        cap = int(body.get("max_tokens") or 4096)
        n, ttft, tpot = self._plan(body, cap)
        msgs = list(body.get("messages") or [])
        if body.get("system"):
            msgs.insert(0, {"role": "system", "content": body["system"]})
        prompt = estimate_input_tokens({"messages": msgs})
        st = self.stats[tenant]
        st["requests"] += 1
        st["prompt_tokens"] += prompt
        rid, model = f"msg_{uuid.uuid4().hex[:12]}", body.get("model", "mock")
        stop = "max_tokens" if n >= cap else "end_turn"

        if not body.get("stream"):
            await asyncio.sleep((ttft + tpot * n) / 1000)
            st["completion_tokens"] += n
            return web.json_response({
                "id": rid, "type": "message", "role": "assistant", "model": model,
                "content": [{"type": "text", "text": TOKEN * n}], "stop_reason": stop,
                "usage": {"input_tokens": prompt, "output_tokens": n},
            })

        def ev(name: str, data: dict) -> bytes:
            return f"event: {name}\ndata: {json.dumps(data)}\n\n".encode()

        head = [
            ev("message_start", {"type": "message_start", "message": {
                "id": rid, "type": "message", "role": "assistant", "model": model, "content": [],
                "usage": {"input_tokens": prompt, "output_tokens": 0}}}),
            ev("content_block_start", {"type": "content_block_start", "index": 0,
                                       "content_block": {"type": "text", "text": ""}}),
        ]
        tail = [
            ev("content_block_stop", {"type": "content_block_stop", "index": 0}),
            ev("message_delta", {"type": "message_delta", "delta": {"stop_reason": stop},
                                 "usage": {"output_tokens": n}}),
            ev("message_stop", {"type": "message_stop"}),
        ]
        frame = lambda text: ev("content_block_delta", {  # noqa: E731
            "type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": text}})
        return await self._emit(request, tenant, n, ttft, tpot, frame, head, tail)


def create_mock_app(settings: MockSettings | None = None) -> web.Application:
    return MockLLM(settings or MockSettings()).build_app()
