"""Latency the gateway adds, at fixed request rates.

Open-loop load (requests leave on a schedule, whether or not earlier ones finished), and
latency is measured from each request's *scheduled* send time, so a stalled client or server
shows up in the tail instead of quietly lowering the rate (no coordinated omission).

Same mock upstream, same load, twice: straight to the mock, then through the gateway with the
full path on (auth, Redis rate-limit + reservation, settlement, metrics).

    uv run python bench/overhead.py
"""

from __future__ import annotations

import argparse
import asyncio
import multiprocessing as mp
import time

import numpy as np
from common import REDIS_URL, gateway, mock, save

MOCK_PORT, GW_PORT = 9201, 8301
TENANTS = [f"tenant-{i}" for i in range(200)]
BODY = {"model": "m", "max_tokens": 256, "messages": [{"role": "user", "content": "ping " * 40}]}


def _loadgen(url: str, rate: float, duration: float, stream: bool, offset: int, q: mp.Queue):
    import aiohttp
    import uvloop

    async def run():
        lat, ttfb, errors = [], [], 0
        body = dict(BODY, stream=stream)
        conn = aiohttp.TCPConnector(limit=0, keepalive_timeout=60)
        async with aiohttp.ClientSession(connector=conn,
                                         timeout=aiohttp.ClientTimeout(total=30)) as s:
            async def one(i: int, due: float):
                nonlocal errors
                key = TENANTS[(i + offset) % len(TENANTS)]
                try:
                    async with s.post(url, json=body, headers={"authorization": f"Bearer {key}"}) as r:
                        if stream:
                            first = None
                            async for line in r.content:
                                if first is None and b'"content":"tok' in line:
                                    first = time.perf_counter()
                            if first is not None and due - start >= 1.0:
                                ttfb.append(first - due)
                        else:
                            await r.read()
                        if r.status != 200:
                            errors += 1
                            return
                except Exception:
                    errors += 1
                    return
                if due - start >= 1.0:  # skip the first second: connection setup
                    lat.append(time.perf_counter() - due)

            n = int(rate * duration)
            start = time.perf_counter() + 0.2
            tasks = []
            for i in range(n):
                due = start + i / rate
                delay = due - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
                tasks.append(asyncio.create_task(one(i, due)))
            await asyncio.gather(*tasks)
            elapsed = time.perf_counter() - start
        q.put({"lat": lat, "ttfb": ttfb, "errors": errors, "n": n, "elapsed": elapsed})

    uvloop.run(run())


def load(url: str, rate: float, duration: float, procs: int, stream: bool) -> dict:
    q: mp.Queue = mp.Queue()
    ps = [mp.Process(target=_loadgen, args=(url, rate / procs, duration, stream, i * 37, q))
          for i in range(procs)]
    for p in ps:
        p.start()
    parts = [q.get() for _ in ps]
    for p in ps:
        p.join()
    lat = np.array([x for part in parts for x in part["lat"]]) * 1000
    ttfb = np.array([x for part in parts for x in part["ttfb"]]) * 1000
    n = sum(p["n"] for p in parts)
    errors = sum(p["errors"] for p in parts)
    elapsed = max(p["elapsed"] for p in parts)

    def pct(a):
        return {f"p{k}": round(float(np.percentile(a, k)), 3) for k in (50, 90, 99, 99.9)} if len(a) else {}

    return {"target_rps": rate, "achieved_rps": round(n / elapsed, 1), "requests": n,
            "errors": errors, "latency_ms": pct(lat), "ttfb_ms": pct(ttfb) if stream else None}


def gw_config() -> dict:
    return {
        "redis_url": REDIS_URL,
        "key_prefix": f"bench-overhead-{int(time.time())}",
        "redis_max_connections": 256,
        "budget": {"accounting": "reserve", "estimator": "p95", "on_overrun": "truncate"},
        "upstreams": {"mock": {"base_url": f"http://127.0.0.1:{MOCK_PORT}"}},
        "routes": {"m": {"targets": [{"upstream": "mock", "model": "mock-1"}]}},
        "pricing": {"mock-1": {"input": 0.15, "output": 0.6}},
        "tenants": {t: {"api_keys": [t], "budget": {"window_s": 3600, "total": 10**12}}
                    for t in TENANTS},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rates", default="1000,2500,5000")
    ap.add_argument("--duration", type=float, default=15)
    ap.add_argument("--loadgen-procs", type=int, default=4)
    ap.add_argument("--gateway-workers", type=int, default=4)
    ap.add_argument("--mock-workers", type=int, default=2)
    args = ap.parse_args()
    rates = [float(r) for r in args.rates.split(",")]

    m = mock(MOCK_PORT, workers=args.mock_workers, ttft_ms=0, tpot_ms=0, median_tokens=20,
             sigma=0, chunk_tokens=1).start()
    g = gateway(gw_config(), GW_PORT, workers=args.gateway_workers).start()
    results = []
    try:
        direct = f"http://127.0.0.1:{MOCK_PORT}/v1/chat/completions"
        via = f"http://127.0.0.1:{GW_PORT}/v1/chat/completions"
        load(via, 500, 3, args.loadgen_procs, False)  # warm up connections and the p95 estimator
        scenarios = [(False, r) for r in rates] + [(True, rates[0])]
        for stream, rate in scenarios:
            for target, url in (("direct", direct), ("gateway", via)):
                res = load(url, rate, args.duration, args.loadgen_procs, stream)
                res.update(target=target, stream=stream)
                results.append(res)
                print(f"{'stream' if stream else 'unary':6s} {target:8s} {rate:6.0f} rps -> "
                      f"{res['achieved_rps']:7.1f} rps, errors {res['errors']}, "
                      f"latency {res['latency_ms']}", flush=True)
                time.sleep(1)
    finally:
        g.stop()
        m.stop()

    added = []
    for stream, rate in scenarios:
        d = next(r for r in results if r["target"] == "direct" and r["stream"] == stream
                 and r["target_rps"] == rate)
        v = next(r for r in results if r["target"] == "gateway" and r["stream"] == stream
                 and r["target_rps"] == rate)
        added.append({"stream": stream, "target_rps": rate,
                      **{f"added_{k}_ms": round(v["latency_ms"][k] - d["latency_ms"][k], 3)
                         for k in ("p50", "p90", "p99")}})
    params = {"duration_s": args.duration, "loadgen_procs": args.loadgen_procs,
              "gateway_workers": args.gateway_workers, "mock_workers": args.mock_workers,
              "tenants": len(TENANTS), "output_tokens": 20}
    path = save("overhead", {"params": params, "results": results, "added": added})
    print(f"\nwrote {path}")
    for a in added:
        print(a)


if __name__ == "__main__":
    main()
