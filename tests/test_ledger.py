import asyncio

from hardcap.config import BudgetPolicy, RateLimit, Tenant, TenantBudget
from hardcap.ledger import BudgetLedger


def tenant(total=0, output=0, input_=0, rl=None, window_s=3600):
    return Tenant("acme", ["k"], TenantBudget(window_s=window_s, total=total, output=output,
                                             input=input_), rl or RateLimit())


async def test_concurrent_reservations_never_exceed_limit(redis_client, prefix):
    ledger = BudgetLedger(redis_client, BudgetPolicy(accounting="reserve"), prefix)
    t = tenant(total=10_000)
    results = await asyncio.gather(*(ledger.admit(t, 50, 200) for _ in range(500)))
    admitted = [a for a in results if a.ok]
    assert len(admitted) == 10_000 // 250
    u = await ledger.usage(t)
    assert u["rin"] + u["rout"] <= 10_000


async def test_settle_swaps_reservation_for_actual(redis_client, prefix):
    ledger = BudgetLedger(redis_client, BudgetPolicy(accounting="reserve"), prefix)
    t = tenant(total=1000)
    a = await ledger.admit(t, 100, 500)
    assert a.ok and a.granted_out == 500
    busy = await ledger.admit(t, 100, 500)
    assert not busy.ok and busy.reason == "busy" and busy.retry_after_s == 1.0
    await ledger.settle(t, a, 90, 120)
    u = await ledger.usage(t)
    assert (u["in"], u["out"], u["rin"], u["rout"]) == (90, 120, 0, 0)
    assert (await ledger.admit(t, 100, 500)).ok
    spent = await ledger.admit(t, 100, 900)
    assert not spent.ok and spent.reason == "total"  # would not fit even with nothing in flight


async def test_elastic_grant_takes_available_headroom(redis_client, prefix):
    ledger = BudgetLedger(redis_client, BudgetPolicy(accounting="reserve"), prefix)
    t = tenant(total=1000)
    a = await ledger.admit(t, 100, 100, est_out_max=2000)
    assert a.ok and a.granted_out == 900
    assert not (await ledger.admit(t, 1, 1)).ok


async def test_extend_respects_limit(redis_client, prefix):
    ledger = BudgetLedger(redis_client, BudgetPolicy(accounting="reserve"), prefix)
    t = tenant(output=300)
    a = await ledger.admit(t, 10, 200)
    assert await ledger.extend(t, a, 100) == 1
    assert a.granted_out == 300
    assert await ledger.extend(t, a, 1) == 0
    await ledger.settle(t, a, 10, 300)
    assert await ledger.extend(t, a, 1) == -1  # reservation is gone


async def test_expired_reservations_are_reclaimed(redis_client, prefix):
    ledger = BudgetLedger(redis_client, BudgetPolicy(accounting="reserve", reservation_ttl_s=0.2), prefix)
    t = tenant(total=500)
    assert (await ledger.admit(t, 0, 500)).ok  # this "replica" dies and never settles
    assert not (await ledger.admit(t, 0, 500)).ok
    await asyncio.sleep(0.3)
    assert (await ledger.admit(t, 0, 500)).ok


async def test_posthoc_admits_until_used_reaches_limit(redis_client, prefix):
    ledger = BudgetLedger(redis_client, BudgetPolicy(accounting="posthoc"), prefix)
    t = tenant(total=100)
    batch = await asyncio.gather(*(ledger.admit(t, 50, 500) for _ in range(20)))
    assert all(a.ok for a in batch)  # nothing is charged yet, so everyone gets in
    for a in batch:
        await ledger.settle(t, a, 50, 50)
    assert not (await ledger.admit(t, 1, 1)).ok
    assert (await ledger.usage(t))["in"] == 1000  # 10x overshoot


async def test_sliding_window_rate_limit(redis_client, prefix):
    ledger = BudgetLedger(redis_client, BudgetPolicy(), prefix)
    t = tenant(rl=RateLimit(algorithm="sliding_window", requests=5, per_s=10))
    res = [await ledger.admit(t, 1, 1) for _ in range(8)]
    assert sum(a.ok for a in res) == 5
    rejected = [a for a in res if not a.ok]
    assert rejected[0].reason == "rate" and rejected[0].retry_after_s > 0


async def test_token_bucket_rate_limit_refills(redis_client, prefix):
    ledger = BudgetLedger(redis_client, BudgetPolicy(), prefix)
    t = tenant(rl=RateLimit(algorithm="token_bucket", requests=20, per_s=1, burst=3))
    res = [await ledger.admit(t, 1, 1) for _ in range(5)]
    assert sum(a.ok for a in res) == 3
    await asyncio.sleep(0.12)  # 20/s refills ~2 tokens
    assert (await ledger.admit(t, 1, 1)).ok


async def test_rate_rejection_does_not_consume_budget(redis_client, prefix):
    ledger = BudgetLedger(redis_client, BudgetPolicy(), prefix)
    t = tenant(total=100, rl=RateLimit(requests=1, per_s=60))
    a = await ledger.admit(t, 10, 10)
    assert a.ok
    assert not (await ledger.admit(t, 10, 10)).ok
    u = await ledger.usage(t)
    assert u["rin"] + u["rout"] == 20
