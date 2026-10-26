"""Live check against real providers (OpenAI, Anthropic, Groq), streaming and not, through Hardcap.

Costs a few cents. Keys come from the environment or from ~/.config/hardcap/keys.env
(lines like GROQ_API_KEY=...); they are passed to the gateway process through its
environment and never written to disk or printed.

    uv run python bench/live_providers.py

Checks, per provider:
  * unary and streaming responses come back in OpenAI format
  * the ledger charges exactly the usage the provider reported
  * a stream that outgrows a small budget is truncated near the limit
  * a dead primary falls back to the real provider
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

import aiohttp
import redis
from common import REDIS_URL, gateway, save

PORT = 8501
MODELS = {
    "openai": os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
    "anthropic": os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5"),
    "groq": os.environ.get("GROQ_MODEL", "openai/gpt-oss-20b"),
}
BASES = {"openai": "https://api.openai.com", "anthropic": "https://api.anthropic.com",
         "groq": "https://api.groq.com/openai"}
ENV_KEYS = {"openai": "OPENAI_API_KEY", "anthropic": "ANTHROPIC_API_KEY", "groq": "GROQ_API_KEY"}
KINDS = {"openai": "openai", "anthropic": "anthropic", "groq": "openai"}


def load_keys() -> None:
    f = Path.home() / ".config" / "hardcap" / "keys.env"
    if f.exists():
        for line in f.read_text().splitlines():
            line = line.strip().removeprefix("export ")
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))


def config(providers: list[str], prefix: str) -> dict:
    ups = {p: {"kind": KINDS[p], "base_url": BASES[p], "api_key_env": ENV_KEYS[p], "read_timeout_s": 60}
           for p in providers}
    ups["dead"] = {"kind": "openai", "base_url": "http://127.0.0.1:9", "connect_timeout_s": 1}
    routes = {p: {"targets": [{"upstream": p, "model": MODELS[p]}]} for p in providers}
    routes.update({f"{p}-fallback": {"targets": [{"upstream": "dead", "model": "x"},
                                                 {"upstream": p, "model": MODELS[p]}]}
                   for p in providers})
    tenants = {
        "big": {"api_keys": ["k-big"], "budget": {"window_s": 3600, "total": 200_000}},
        "small": {"api_keys": ["k-small"], "budget": {"window_s": 3600, "output": 60}},
    }
    return {"redis_url": REDIS_URL, "key_prefix": prefix,
            "budget": {"estimator": "fixed", "fixed_estimate": 16, "extend_chunk": 16},
            "upstreams": ups, "routes": routes, "tenants": tenants}


async def call(s, route, key, *, stream, max_tokens=300, prompt="Say hello in five words."):
    body = {"model": route, "stream": stream, "max_tokens": max_tokens,
            "messages": [{"role": "system", "content": "Be concise."},
                         {"role": "user", "content": prompt}]}
    if stream:
        body["stream_options"] = {"include_usage": True}
    async with s.post(f"http://127.0.0.1:{PORT}/v1/chat/completions", json=body,
                      headers={"authorization": f"Bearer {key}"}) as r:
        out = {"status": r.status, "upstream": r.headers.get("x-hardcap-upstream")}
        if r.status != 200:
            out["error"] = (await r.text())[:300]
            return out
        if not stream:
            d = await r.json()
            out.update(text=d["choices"][0]["message"]["content"], usage=d.get("usage"),
                       finish=d["choices"][0]["finish_reason"])
            return out
        text, usage, finish, truncated, done = [], None, None, False, False
        async for line in r.content:
            line = line.strip()
            if line == b"data: [DONE]":
                done = True
            elif line.startswith(b"data: {"):
                c = json.loads(line[6:])
                truncated |= bool(c.get("hardcap"))
                usage = c.get("usage") or usage
                for ch in c.get("choices") or ():
                    text.append((ch.get("delta") or {}).get("content") or "")
                    finish = ch.get("finish_reason") or finish
        out.update(text="".join(text), usage=usage, finish=finish, truncated=truncated, done=done)
        return out


def ledger(r, prefix, tenant) -> dict:
    w = int(time.time() // 3600)
    return {k.decode(): int(v) for k, v in r.hgetall(f"{prefix}:{{{tenant}}}:b:{w}").items()}


async def run(providers: list[str]) -> list[dict]:
    prefix = f"live-{int(time.time())}"
    r = redis.Redis.from_url(REDIS_URL)
    g = gateway(config(providers, prefix), PORT).start()
    checks = []
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as s:
            for p in providers:
                for stream in (False, True):
                    before = ledger(r, prefix, "big")
                    res = await call(s, p, "k-big", stream=stream)
                    await asyncio.sleep(0.2)
                    after = ledger(r, prefix, "big")
                    u = res.get("usage") or {}
                    charged = {"in": after.get("in", 0) - before.get("in", 0),
                               "out": after.get("out", 0) - before.get("out", 0)}
                    ok = (res["status"] == 200 and bool(res.get("text"))
                          and charged == {"in": u.get("prompt_tokens"), "out": u.get("completion_tokens")}
                          and (not stream or res.get("done")))
                    checks.append({"provider": p, "check": f"{'stream' if stream else 'unary'}: "
                                   "response + ledger equals provider usage", "ok": ok,
                                   "provider_usage": u, "ledger_charged": charged,
                                   "status": res["status"], "error": res.get("error")})

                res = await call(s, p, "k-small", stream=True, max_tokens=400,
                                 prompt="Write a 300-word story about a lighthouse.")
                await asyncio.sleep(0.3)
                used = ledger(r, prefix, "small")
                checks.append({"provider": p, "check": "stream truncated at a 60-token output budget",
                               "ok": bool(res["status"] == 200 and res.get("truncated")
                                          and used.get("out", 0) <= 60 + 16),
                               "ledger_output_tokens": used.get("out"), "limit": 60,
                               "finish": res.get("finish"), "status": res["status"],
                               "error": res.get("error")})
                r.delete(f"{prefix}:{{small}}:b:{int(time.time() // 3600)}")

                res = await call(s, f"{p}-fallback", "k-big", stream=True)
                checks.append({"provider": p, "check": "dead primary falls back to the provider",
                               "ok": res["status"] == 200 and res.get("upstream") == p
                               and bool(res.get("text")),
                               "served_by": res.get("upstream"), "status": res["status"],
                               "error": res.get("error")})
    finally:
        g.stop()
    return checks


def main() -> None:
    load_keys()
    providers = [p for p in MODELS if os.environ.get(ENV_KEYS[p])]
    if not providers:
        raise SystemExit("set OPENAI_API_KEY, ANTHROPIC_API_KEY or GROQ_API_KEY "
                         "(env or ~/.config/hardcap/keys.env)")
    checks = asyncio.run(run(providers))
    for c in checks:
        print(f"{'PASS' if c['ok'] else 'FAIL'}  {c['provider']:9s} {c['check']}"
              + ("" if c["ok"] else f"  -> {json.dumps({k: v for k, v in c.items() if k not in ('ok',)})}"))
    path = save("live_providers", {"models": {p: MODELS[p] for p in providers}, "checks": checks})
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
