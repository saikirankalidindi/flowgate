"""Failover when the primary upstream dies or starts failing, mid-load.

Route: primary -> secondary. Steady open-loop streaming load. Two scenarios:
  kill  SIGKILL the primary process at t=5s, start it again at t=12s
  503   the primary answers every request with 503 from t=5s to t=12s

Reports client-visible failures (and whether they broke mid-stream, which no gateway can retry
once bytes reached the client), the error window, and how fast traffic returns to the primary.

    uv run python bench/failover.py
"""

from __future__ import annotations

import argparse
import asyncio
import time

import aiohttp
import numpy as np
from common import REDIS_URL, gateway, http_json, mock, save

PRIMARY, SECONDARY, GW = 9301, 9302, 8401
MOCK = dict(ttft_ms=20, tpot_ms=2, median_tokens=50, sigma=0.3, chunk_tokens=1)
FAULT_AT, RECOVER_AT = 5.0, 12.0


def gw_config() -> dict:
    return {
        "redis_url": REDIS_URL,
        "key_prefix": f"bench-failover-{int(time.time())}",
        "breaker": {"failure_threshold": 5, "cooldown_s": 2},
        "upstreams": {
            "primary": {"base_url": f"http://127.0.0.1:{PRIMARY}", "connect_timeout_s": 1,
                        "read_timeout_s": 5},
            "secondary": {"base_url": f"http://127.0.0.1:{SECONDARY}", "connect_timeout_s": 1,
                          "read_timeout_s": 5},
        },
        "routes": {"m": {"targets": [{"upstream": "primary", "model": "a"},
                                     {"upstream": "secondary", "model": "b"}]}},
        "tenants": {"t": {"api_keys": ["t"]}},
    }


async def one(s: aiohttp.ClientSession, due: float, t0: float) -> dict:
    body = {"model": "m", "stream": True, "max_tokens": 512,
            "messages": [{"role": "user", "content": "hi"}]}
    rec = {"t": round(due - t0, 4), "upstream": None, "outcome": "ok"}
    try:
        async with s.post(f"http://127.0.0.1:{GW}/v1/chat/completions", json=body,
                          headers={"authorization": "Bearer t"}) as r:
            rec["upstream"] = r.headers.get("x-hardcap-upstream")
            if r.status != 200:
                rec["outcome"] = f"http_{r.status}"
                await r.read()
            else:
                done = False
                async for line in r.content:
                    if line.startswith(b'data: {"error"'):
                        rec["outcome"] = "broken_mid_stream"
                    if line.strip() == b"data: [DONE]":
                        done = True
                if not done and rec["outcome"] == "ok":
                    rec["outcome"] = "broken_mid_stream"
    except Exception as e:  # noqa: BLE001
        rec["outcome"] = f"client_{type(e).__name__}"
    rec["latency_ms"] = round((time.perf_counter() - due) * 1000, 2)
    return rec


async def run(scenario: str, rate: float, duration: float) -> dict:
    primary = mock(PRIMARY, seed=1, **MOCK).start()
    secondary = mock(SECONDARY, seed=2, **MOCK).start()
    g = gateway(gw_config(), GW).start()
    events: dict[str, float] = {}
    try:
        async def faults(t0: float):
            nonlocal primary
            await asyncio.sleep(max(0, t0 + FAULT_AT - time.perf_counter()))
            events["fault"] = time.perf_counter() - t0
            if scenario == "kill":
                await asyncio.to_thread(primary.kill)
            else:
                http_json(f"http://127.0.0.1:{PRIMARY}/admin/faults",
                          {"error_rate": 1.0, "error_status": 503})
            await asyncio.sleep(max(0, t0 + RECOVER_AT - time.perf_counter()))
            if scenario == "kill":
                primary = await asyncio.to_thread(lambda: mock(PRIMARY, seed=3, **MOCK).start())
            else:
                http_json(f"http://127.0.0.1:{PRIMARY}/admin/faults", {"error_rate": 0.0})
            events["recover"] = time.perf_counter() - t0

        conn = aiohttp.TCPConnector(limit=0)
        async with aiohttp.ClientSession(connector=conn, timeout=aiohttp.ClientTimeout(total=30)) as s:
            t0 = time.perf_counter() + 0.2
            ft = asyncio.create_task(faults(t0))
            tasks = []
            for i in range(int(rate * duration)):
                due = t0 + i / rate
                await asyncio.sleep(max(0, due - time.perf_counter()))
                tasks.append(asyncio.create_task(one(s, due, t0)))
            recs = await asyncio.gather(*tasks)
            await ft
    finally:
        g.stop()
        primary.stop()
        secondary.stop()

    failed = [r for r in recs if r["outcome"] != "ok"]
    fault, recover = events["fault"], events["recover"]
    after = [r for r in recs if r["t"] >= recover and r["upstream"] == "primary" and r["outcome"] == "ok"]
    buckets = []
    for b in np.arange(0, duration, 0.5):
        grp = [r for r in recs if b <= r["t"] < b + 0.5]
        ok = [r for r in grp if r["outcome"] == "ok"]
        buckets.append({
            "t": float(b), "requests": len(grp), "failed": len(grp) - len(ok),
            "via_primary": sum(r["upstream"] == "primary" for r in ok),
            "via_secondary": sum(r["upstream"] == "secondary" for r in ok),
            "p99_ms": round(float(np.percentile([r["latency_ms"] for r in grp], 99)), 2) if grp else None,
        })
    outcomes: dict[str, int] = {}
    for r in failed:
        outcomes[r["outcome"]] = outcomes.get(r["outcome"], 0) + 1
    return {
        "scenario": scenario, "requests": len(recs), "failed": len(failed),
        "failed_pct": round(100 * len(failed) / len(recs), 3), "failure_kinds": outcomes,
        "fault_at_s": round(fault, 3), "recover_at_s": round(recover, 3),
        "error_window_ms": round(1000 * (max(r["t"] for r in failed) - fault), 1) if failed else 0.0,
        "back_on_primary_after_ms":
            round(1000 * (min(r["t"] for r in after) - recover), 1) if after else None,
        "p99_ms_steady": round(float(np.percentile(
            [r["latency_ms"] for r in recs if r["t"] < fault], 99)), 2),
        "p99_ms_during_fault": round(float(np.percentile(
            [r["latency_ms"] for r in recs if fault <= r["t"] < recover], 99)), 2),
        "timeline": buckets,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=200)
    ap.add_argument("--duration", type=float, default=20)
    args = ap.parse_args()
    out = []
    for sc in ("kill", "503"):
        res = asyncio.run(run(sc, args.rate, args.duration))
        out.append(res)
        print({k: v for k, v in res.items() if k != "timeline"}, flush=True)
    path = save("failover", {"params": {"rate_rps": args.rate, "duration_s": args.duration,
                                        "mock": MOCK, "breaker": {"failure_threshold": 5,
                                                                  "cooldown_s": 2}},
                             "scenarios": out})
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
