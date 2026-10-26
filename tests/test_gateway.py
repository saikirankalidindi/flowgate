"""End-to-end: client -> gateway -> mock upstream(s), all in-process, against a real Redis."""

import asyncio
import json

import aiohttp
import pytest
from aiohttp.test_utils import TestServer

from flowgate.app import Gateway
from flowgate.config import parse_config
from flowgate.mock.server import MockLLM, MockSettings

from .conftest import REDIS_URL

FAST = MockSettings(ttft_ms=0, tpot_ms=0, median_tokens=50, sigma=0.0, seed=1)


@pytest.fixture
async def gateway(prefix, redis_client):
    """Factory: gateway(budget=..., tenant=..., mocks={name: (kind, MockSettings)}, route=[...])."""
    servers, sessions = [], []

    async def make(budget=None, tenant=None, mocks=None, route=None, breaker=None):
        mocks = mocks or {"a": ("openai", FAST)}
        upstreams, mock_objs = {}, {}
        for name, (kind, settings) in mocks.items():
            m = MockLLM(settings)
            srv = TestServer(m.build_app())
            await srv.start_server()
            servers.append(srv)
            mock_objs[name] = (m, srv)
            upstreams[name] = {"kind": kind, "base_url": str(srv.make_url("")),
                               "read_timeout_s": 5}
        cfg = parse_config({
            "redis_url": REDIS_URL,
            "key_prefix": prefix,
            "budget": budget or {},
            "breaker": breaker or {},
            "upstreams": upstreams,
            "routes": {"m": {"targets": [{"upstream": u, "model": "x"} for u in (route or list(mocks))]}},
            "pricing": {"x": {"input": 1.0, "output": 2.0}},
            "tenants": {"acme": {"api_keys": ["sk-acme"], **(tenant or {})}},
        })
        gw = Gateway(cfg)
        srv = TestServer(gw.build_app())
        await srv.start_server()
        servers.append(srv)
        sess = aiohttp.ClientSession(base_url=str(srv.make_url("")),
                                     headers={"authorization": "Bearer sk-acme"})
        sessions.append(sess)
        return gw, sess, mock_objs

    yield make
    for s in sessions:
        await s.close()
    for s in reversed(servers):
        await s.close()


def chat(n=None, stream=False, max_tokens=None, **extra):
    body = {"model": "m", "messages": [{"role": "user", "content": "hello there"}], "stream": stream}
    if n is not None:
        body["mock"] = {"output_tokens": n}
    if max_tokens:
        body["max_tokens"] = max_tokens
    return {**body, **extra}


async def read_sse(resp):
    chunks = []
    async for line in resp.content:
        line = line.strip()
        if line.startswith(b"data:"):
            payload = line[5:].strip()
            chunks.append("DONE" if payload == b"[DONE]" else json.loads(payload))
    return chunks


def content_tokens(chunks):
    return sum(c["choices"][0]["delta"].get("content", "").count("tok")
               for c in chunks if c != "DONE" and c.get("choices"))


async def test_auth_and_unknown_model(gateway):
    _, s, _ = await gateway()
    async with s.post("/v1/chat/completions", json=chat(), headers={"authorization": "Bearer no"}) as r:
        assert r.status == 401
    async with s.post("/v1/chat/completions", json={**chat(), "model": "nope"}) as r:
        assert r.status == 404


async def test_unary_charges_actual_usage(gateway):
    gw, s, _ = await gateway(tenant={"budget": {"total": 100_000}})
    async with s.post("/v1/chat/completions", json=chat(n=42)) as r:
        assert r.status == 200
        data = await r.json()
    assert data["usage"]["completion_tokens"] == 42
    u = await gw.ledger.usage(gw.cfg.tenants["acme"])
    assert u["out"] == 42 and u["rin"] == 0 and u["rout"] == 0


async def test_stream_passthrough_and_usage_settlement(gateway):
    gw, s, _ = await gateway(tenant={"budget": {"total": 100_000}})
    async with s.post("/v1/chat/completions", json=chat(n=30, stream=True)) as r:
        assert r.status == 200
        chunks = await read_sse(r)
    assert chunks[-1] == "DONE"
    assert content_tokens(chunks) == 30
    assert not any(c != "DONE" and c.get("usage") for c in chunks)  # client did not ask for it
    u = await gw.ledger.usage(gw.cfg.tenants["acme"])
    assert u["out"] == 30 and u["rout"] == 0


async def test_stream_usage_forwarded_when_requested(gateway):
    _, s, _ = await gateway()
    body = chat(n=12, stream=True, stream_options={"include_usage": True})
    async with s.post("/v1/chat/completions", json=body) as r:
        chunks = await read_sse(r)
    usage = [c["usage"] for c in chunks if c != "DONE" and c.get("usage")]
    assert usage and usage[0]["completion_tokens"] == 12


async def test_truncate_mode_stops_stream_at_budget(gateway):
    gw, s, mocks = await gateway(
        budget={"estimator": "fixed", "fixed_estimate": 20, "on_overrun": "truncate",
                "extend_chunk": 10},
        tenant={"budget": {"output": 100}},
    )
    async with s.post("/v1/chat/completions", json=chat(n=500, stream=True, max_tokens=1000)) as r:
        chunks = await read_sse(r)
    got = content_tokens(chunks)
    assert got <= 100
    finals = [c for c in chunks if c != "DONE" and c.get("flowgate")]
    assert finals and finals[0]["choices"][0]["finish_reason"] == "length"
    await asyncio.sleep(0.05)
    u = await gw.ledger.usage(gw.cfg.tenants["acme"])
    assert u["out"] <= 100 and u["rout"] == 0


async def test_allow_mode_lets_stream_overrun(gateway):
    gw, s, _ = await gateway(
        budget={"estimator": "fixed", "fixed_estimate": 20, "on_overrun": "allow", "extend_chunk": 10},
        tenant={"budget": {"output": 100}},
    )
    async with s.post("/v1/chat/completions", json=chat(n=300, stream=True, max_tokens=1000)) as r:
        assert content_tokens(await read_sse(r)) == 300
    assert (await gw.ledger.usage(gw.cfg.tenants["acme"]))["out"] == 300


async def test_truncate_mode_caps_unary_max_tokens(gateway):
    gw, s, _ = await gateway(
        budget={"estimator": "fixed", "fixed_estimate": 20, "on_overrun": "truncate"},
        tenant={"budget": {"output": 100}},
    )
    async with s.post("/v1/chat/completions", json=chat(n=500, max_tokens=1000)) as r:
        data = await r.json()
    assert data["usage"]["completion_tokens"] == 100
    assert data["choices"][0]["finish_reason"] == "length"


async def test_budget_rejection_returns_429_with_retry_after(gateway):
    _, s, _ = await gateway(tenant={"budget": {"total": 50, "window_s": 60}})
    async with s.post("/v1/chat/completions", json=chat(n=5, max_tokens=500)) as r:
        assert r.status == 429
        assert (await r.json())["error"]["type"] == "budget_exceeded"
        assert 1 <= int(r.headers["retry-after"]) <= 60


async def test_rate_limit_rejection(gateway):
    _, s, _ = await gateway(tenant={"rate_limit": {"requests": 2, "per_s": 60}})
    statuses = []
    for _ in range(4):
        async with s.post("/v1/chat/completions", json=chat(n=1)) as r:
            statuses.append(r.status)
            if r.status == 429:
                assert (await r.json())["error"]["type"] == "rate_limit_exceeded"
    assert statuses == [200, 200, 429, 429]


async def test_fallback_on_5xx_and_breaker_opens(gateway):
    broken = MockSettings(ttft_ms=0, tpot_ms=0, error_rate=1.0, error_status=503, seed=1)
    gw, s, mocks = await gateway(mocks={"a": ("openai", broken), "b": ("openai", FAST)},
                                 breaker={"failure_threshold": 2, "cooldown_s": 60})
    for _ in range(4):
        async with s.post("/v1/chat/completions", json=chat(n=3)) as r:
            assert r.status == 200
            assert r.headers["x-flowgate-upstream"] == "b"
    assert gw.router.breakers["a"].state == "open"
    assert mocks["b"][0].stats["acme"]["requests"] == 4


async def test_fallback_on_connection_refused(gateway):
    gw, s, mocks = await gateway(mocks={"a": ("openai", FAST), "b": ("openai", FAST)})
    await mocks["a"][1].close()
    async with s.post("/v1/chat/completions", json=chat(n=3, stream=True)) as r:
        assert r.status == 200 and r.headers["x-flowgate-upstream"] == "b"
        assert content_tokens(await read_sse(r)) == 3


async def test_all_upstreams_down_releases_reservation(gateway):
    broken = MockSettings(error_rate=1.0, error_status=500, seed=1)
    gw, s, _ = await gateway(mocks={"a": ("openai", broken)}, tenant={"budget": {"total": 10_000}})
    async with s.post("/v1/chat/completions", json=chat(n=3)) as r:
        assert r.status == 500
    u = await gw.ledger.usage(gw.cfg.tenants["acme"])
    assert u.get("rin", 0) == 0 and u.get("rout", 0) == 0 and u.get("out", 0) == 0


async def test_anthropic_upstream_is_translated(gateway):
    gw, s, mocks = await gateway(mocks={"c": ("anthropic", MockSettings(
        ttft_ms=0, tpot_ms=0, median_tokens=25, sigma=0.0, seed=1))})
    body = chat(stream=True, max_tokens=100, stream_options={"include_usage": True})
    body["messages"].insert(0, {"role": "system", "content": "be brief"})
    async with s.post("/v1/chat/completions", json=body) as r:
        chunks = await read_sse(r)
    assert content_tokens(chunks) == 25
    assert chunks[-1] == "DONE"
    async with s.post("/v1/chat/completions", json=chat(max_tokens=100)) as r:
        data = await r.json()
    assert data["usage"]["completion_tokens"] == 25
    assert data["choices"][0]["message"]["content"].count("tok") == 25
    assert (await gw.ledger.usage(gw.cfg.tenants["acme"]))["out"] == 50


async def test_metrics_and_usage_endpoints(gateway):
    _, s, _ = await gateway(tenant={"budget": {"total": 1000}})
    async with s.post("/v1/chat/completions", json=chat(n=10, max_tokens=100)) as r:
        assert r.status == 200
    async with s.get("/metrics") as r:
        text = await r.text()
    assert 'flowgate_tokens_total{direction="output",model="x",route="m",tenant="acme"}' in text
    assert "flowgate_cost_usd_total" in text
    async with s.get("/v1/flowgate/usage") as r:
        u = await r.json()
    assert u["used"]["output"] == 10 and u["limits"]["total"] == 1000
