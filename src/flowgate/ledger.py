"""Distributed token-budget ledger on Redis.

Every replica talks to the same Redis, and every state change is one Lua script, so admission
decisions are serialized per tenant no matter how many gateway replicas are running.

Two accounting modes:

* ``reserve``: admission atomically reserves ``input + estimated output`` against the window's
  remaining budget. The stream may grow its reservation in chunks; settlement swaps the
  reservation for the real count. The budget can only be overshot by tokens that were never
  reserved, and with ``on_overrun: truncate`` there are none.
* ``posthoc``: admission checks ``used < limit`` and charges at the end. This is the common
  approach and the baseline the benchmarks compare against.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from importlib import resources

import redis.asyncio as aioredis

from flowgate.config import BudgetPolicy, Tenant


def _script(name: str) -> str:
    return resources.files("flowgate").joinpath("lua", name).read_text()


@dataclass
class Admission:
    ok: bool
    tenant: str
    window: int = 0
    reservation_id: str = ""
    # Output tokens granted by the reservation (reserve mode); the stream may extend it.
    granted_out: int = 0
    initial_out: int = 0
    reserved_in: int = 0
    reason: str = ""
    retry_after_s: float = 0.0
    last_refresh: float = 0.0


class BudgetLedger:
    def __init__(self, client: aioredis.Redis, policy: BudgetPolicy, prefix: str = "fg"):
        self.r = client
        self.policy = policy
        self.prefix = prefix
        self._admit = client.register_script(_script("admit.lua"))
        self._extend = client.register_script(_script("extend.lua"))
        self._settle = client.register_script(_script("settle.lua"))

    # Keys share a {tenant} hash tag so the scripts also work on Redis Cluster.
    def _keys(self, tenant: str, window: int) -> list[str]:
        base = f"{self.prefix}:{{{tenant}}}"
        return [f"{base}:rl", f"{base}:b:{window}", f"{base}:rz:{window}", f"{base}:rv:{window}"]

    def _key_ttl(self, tenant: Tenant) -> int:
        return int(tenant.budget.window_s * 2 + self.policy.reservation_ttl_s + 60)

    @staticmethod
    def window_of(tenant: Tenant, now: float | None = None) -> int:
        return int((time.time() if now is None else now) // tenant.budget.window_s)

    async def admit(
        self, tenant: Tenant, est_in: int, est_out: int, est_out_max: int | None = None
    ) -> Admission:
        b, rl = tenant.budget, tenant.rate_limit
        window = self.window_of(tenant)
        rid = uuid.uuid4().hex if self.policy.accounting == "reserve" else ""
        algo = {"sliding_window": "sw", "token_bucket": "tb"}[rl.algorithm] if rl.requests else "none"
        res = await self._admit(
            keys=self._keys(tenant.name, window),
            args=[
                self.policy.accounting,
                rid,
                est_in,
                est_out,
                max(est_out, est_out_max or est_out),
                b.input,
                b.output,
                b.total,
                int(self.policy.reservation_ttl_s * 1000),
                self._key_ttl(tenant),
                algo,
                rl.requests,
                int(rl.per_s * 1000),
                rl.burst or rl.requests,
            ],
        )
        if int(res[0]) == 1:
            return Admission(
                ok=True,
                tenant=tenant.name,
                window=window,
                reservation_id=rid,
                granted_out=int(res[1]),
                initial_out=int(res[1]),
                reserved_in=est_in,
                last_refresh=time.monotonic(),
            )
        reason = res[1].decode() if isinstance(res[1], bytes) else str(res[1])
        if reason == "rate":
            retry = int(res[2]) / 1000
        elif reason == "busy":
            retry = 1.0
        else:  # budget: retry when the window rolls over
            retry = (window + 1) * b.window_s - time.time()
        return Admission(ok=False, tenant=tenant.name, window=window, reason=reason,
                         retry_after_s=max(retry, 0.0))

    async def extend(self, tenant: Tenant, adm: Admission, add_out: int) -> int:
        """Grow (or with add_out=0, just refresh) a reservation. Returns 1, 0 (denied) or -1."""
        k = self._keys(tenant.name, adm.window)
        res = int(await self._extend(
            keys=k[1:],
            args=[adm.reservation_id, add_out, tenant.budget.output, tenant.budget.total,
                  int(self.policy.reservation_ttl_s * 1000)],
        ))
        if res == 1:
            adm.granted_out += add_out
            adm.last_refresh = time.monotonic()
        return res

    async def settle(self, tenant: Tenant, adm: Admission, act_in: int, act_out: int) -> tuple[int, int]:
        k = self._keys(tenant.name, adm.window)
        used_in, used_out = await self._settle(
            keys=k[1:], args=[adm.reservation_id, act_in, act_out, self._key_ttl(tenant)]
        )
        return int(used_in), int(used_out)

    async def usage(self, tenant: Tenant, window: int | None = None) -> dict[str, int]:
        window = self.window_of(tenant) if window is None else window
        raw = await self.r.hgetall(self._keys(tenant.name, window)[1])
        return {k.decode(): int(v) for k, v in raw.items()}
