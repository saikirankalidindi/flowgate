"""Budget overshoot under concurrency, across 3 gateway replicas.

Every tenant gets the same total-token budget. C concurrent clients per tenant stream requests
until the gateway says the budget is spent. Ground truth is what the mock upstream actually
generated per tenant (a real provider bills that), compared with the budget.

    uv run python bench/overshoot.py            # full sweep
    uv run python bench/overshoot.py --quick    # smoke run
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import statistics
import time

import aiohttp
import redis
from common import REDIS_URL, gateway, http_json, mock, save

MODES = {
    "posthoc": {"accounting": "posthoc"},
    "reserve_max_tokens": {"accounting": "reserve", "estimator": "max_tokens", "on_overrun": "allow"},
    "reserve_p95_allow": {"accounting": "reserve", "estimator": "p95", "on_overrun": "allow"},
    "reserve_p95_truncate": {"accounting": "reserve", "estimator": "p95", "on_overrun": "truncate"},
}
WINDOW_S = 10_000_000  # one window for the whole run
MOCK_PORT = 9101
GW_PORTS = (8201, 8202, 8203)
PROMPT = "Extract the job title, company and location from the posting below. " * 3


def config(mode: str, prefix: str, tenants: list[str], limit: int) -> dict:
    t = {n: {"api_keys": [n], "budget": {"window_s": WINDOW_S, "total": limit}} for n in tenants}
    t["warmup"] = {"api_keys": ["warmup"], "budget": {"window_s": WINDOW_S}}
    return {
        "redis_url": REDIS_URL,
        "key_prefix": prefix,
        "redis_max_connections": 512,
        "budget": {**MODES[mode], "extend_chunk": 64},
        "upstreams": {"mock": {"base_url": f"http://127.0.0.1:{MOCK_PORT}", "read_timeout_s": 30}},
        "routes": {"m": {"targets": [{"upstream": "mock", "model": "mock-1"}]}},
        "tenants": t,
    }


def body(max_tokens: int) -> dict:
    return {"model": "m", "stream": True, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": PROMPT}]}


async def stream_one(session, url, key, max_tokens) -> tuple[str, int, bool]:
    """Returns (outcome, content tokens received, truncated)."""
    async with session.post(url, json=body(max_tokens),
                            headers={"authorization": f"Bearer {key}"}) as r:
        if r.status == 429:
            return (await r.json())["error"]["type"], 0, False
        if r.status != 200:
            await r.read()
            return f"http_{r.status}", 0, False
        tokens, truncated = 0, False
        async for line in r.content:
            if not line.startswith(b"data: {"):
                continue
            c = json.loads(line[6:])
            if c.get("flowgate"):
                truncated = True
            for ch in c.get("choices") or ():
                tokens += (ch.get("delta") or {}).get("content", "").count("tok")
        return "ok", tokens, truncated


async def tenant_load(session, key: str, conc: int, max_tokens: int, deadline: float) -> dict:
    urls = itertools.cycle([f"http://127.0.0.1:{p}/v1/chat/completions" for p in GW_PORTS])
    s = {"ok": 0, "truncated": 0, "busy": 0, "errors": 0, "tokens_received": 0}
    t0 = time.perf_counter()

    async def worker():
        while time.perf_counter() < deadline:
            outcome, tokens, truncated = await stream_one(session, next(urls), key, max_tokens)
            if outcome == "ok":
                s["ok"] += 1
                s["truncated"] += truncated
                s["tokens_received"] += tokens
            elif outcome == "budget_busy":
                s["busy"] += 1
                await asyncio.sleep(0.05)  # a well-behaved client backs off briefly
            elif outcome == "budget_exceeded":
                return
            else:
                s["errors"] += 1
                await asyncio.sleep(0.05)

    await asyncio.gather(*(worker() for _ in range(conc)))
    s["seconds"] = round(time.perf_counter() - t0, 3)
    return s


async def warmup(session, n: int = 300) -> None:
    urls = itertools.cycle([f"http://127.0.0.1:{p}/v1/chat/completions" for p in GW_PORTS])
    sem = asyncio.Semaphore(30)

    async def one():
        async with sem:
            await stream_one(session, next(urls), "warmup", 1024)

    await asyncio.gather(*(one() for _ in range(n)))


async def run_mode(mode, conc_levels, seeds, tenants_per_run, limit, max_tokens, r) -> list[dict]:
    prefix = f"bench-{mode}-{int(time.time())}"
    names = {(c, s): [f"c{c}-s{s}-t{i}" for i in range(tenants_per_run)]
             for c in conc_levels for s in seeds}
    cfg = config(mode, prefix, [n for v in names.values() for n in v], limit)
    http_json(f"http://127.0.0.1:{MOCK_PORT}/admin/reset", {})  # tenant names repeat per mode
    gws = [gateway(cfg, p).start() for p in GW_PORTS]
    rows = []
    try:
        conn = aiohttp.TCPConnector(limit=0)
        async with aiohttp.ClientSession(connector=conn, timeout=aiohttp.ClientTimeout(total=120)) as sess:
            await warmup(sess)
            for (c, s), tenants in names.items():
                deadline = time.perf_counter() + 120
                loads = await asyncio.gather(*(tenant_load(sess, t, c, max_tokens, deadline)
                                               for t in tenants))
                await asyncio.sleep(0.3)
                served = http_json(f"http://127.0.0.1:{MOCK_PORT}/admin/stats")["tenants"]
                window = int(time.time() // WINDOW_S)
                for t, load in zip(tenants, loads):
                    st = served.get(t, {})
                    total = st.get("prompt_tokens", 0) + st.get("completion_tokens", 0)
                    led = {k.decode(): int(v) for k, v in
                           r.hgetall(f"{prefix}:{{{t}}}:b:{window}").items()}
                    rows.append({
                        "mode": mode, "concurrency": c, "seed": s, "tenant": t, "limit": limit,
                        "served_tokens": total,
                        "overshoot_pct": round(100 * (total - limit) / limit, 3),
                        "ledger_tokens": led.get("in", 0) + led.get("out", 0),
                        "leftover_reserved": led.get("rin", 0) + led.get("rout", 0),
                        **load,
                    })
                last = rows[-tenants_per_run:]
                print(f"{mode:22s} C={c:<4d} seed={s} overshoot max "
                      f"{max(x['overshoot_pct'] for x in last):7.2f}%  mean "
                      f"{statistics.mean(x['overshoot_pct'] for x in last):7.2f}%  "
                      f"truncated {sum(x['truncated'] for x in last)}  "
                      f"busy {sum(x['busy'] for x in last)}", flush=True)
    finally:
        for g in gws:
            g.stop()
    return rows


def summarize(rows: list[dict]) -> list[dict]:
    out = []
    key = lambda x: (x["mode"], x["concurrency"])  # noqa: E731
    for (mode, c), grp in itertools.groupby(sorted(rows, key=key), key=key):
        g = list(grp)
        out.append({
            "mode": mode, "concurrency": c,
            "overshoot_max_pct": max(x["overshoot_pct"] for x in g),
            "overshoot_mean_pct": round(statistics.mean(x["overshoot_pct"] for x in g), 3),
            "utilization_mean_pct": round(statistics.mean(
                100 * min(x["served_tokens"], x["limit"]) / x["limit"] for x in g), 2),
            "seconds_to_exhaust_mean": round(statistics.mean(x["seconds"] for x in g), 2),
            "truncated_per_tenant": round(statistics.mean(x["truncated"] for x in g), 2),
            "busy_retries_per_tenant": round(statistics.mean(x["busy"] for x in g), 1),
            "errors": sum(x["errors"] for x in g),
            "max_leftover_reserved": max(x["leftover_reserved"] for x in g),
            "ledger_vs_served_max_gap": max(x["served_tokens"] - x["ledger_tokens"] for x in g),
        })
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--modes", default=",".join(MODES))
    ap.add_argument("--limit", type=int, default=20_000)
    ap.add_argument("--max-tokens", type=int, default=1024)
    ap.add_argument("--tenants", type=int, default=8)
    args = ap.parse_args()
    conc = [1, 8] if args.quick else [1, 4, 16, 64, 128]
    seeds = [0] if args.quick else [0, 1, 2]
    mock_settings = dict(ttft_ms=20, tpot_ms=1, median_tokens=150, sigma=0.8, chunk_tokens=4, seed=7)

    r = redis.Redis.from_url(REDIS_URL)
    m = mock(MOCK_PORT, **mock_settings).start()
    rows = []
    try:
        for mode in args.modes.split(","):
            rows += asyncio.run(run_mode(mode, conc, seeds, args.tenants, args.limit,
                                         args.max_tokens, r))
    finally:
        m.stop()
    summary = summarize(rows)
    params = {"limit_tokens": args.limit, "max_tokens": args.max_tokens,
              "tenants_per_run": args.tenants, "replicas": len(GW_PORTS), "seeds": seeds,
              "concurrency": conc, "mock": mock_settings}
    path = save("overshoot_quick" if args.quick else "overshoot",
                {"params": params, "summary": summary, "runs": rows})
    print(f"\nwrote {path}")
    for s in summary:
        print(s)


if __name__ == "__main__":
    main()
