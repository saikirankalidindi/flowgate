import os
import uuid

import pytest
import redis.asyncio as aioredis

REDIS_URL = os.environ.get("FLOWGATE_TEST_REDIS", "redis://localhost:6379/15")


@pytest.fixture
async def redis_client():
    r = aioredis.Redis(connection_pool=aioredis.BlockingConnectionPool.from_url(
        REDIS_URL, max_connections=64, timeout=10))
    try:
        await r.ping()
    except Exception:
        pytest.skip(f"redis not reachable at {REDIS_URL}")
    yield r
    await r.aclose()


@pytest.fixture
def prefix():
    return f"fgtest-{uuid.uuid4().hex[:8]}"


@pytest.fixture(autouse=True)
async def _cleanup(request, prefix):
    yield
    if "redis_client" in request.fixturenames or "gateway" in request.fixturenames:
        r = aioredis.from_url(REDIS_URL)
        try:
            keys = [k async for k in r.scan_iter(f"{prefix}:*")]
            if keys:
                await r.delete(*keys)
        except Exception:
            pass
        await r.aclose()
