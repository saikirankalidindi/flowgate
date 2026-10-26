import pytest

from hardcap.breaker import CircuitBreaker
from hardcap.config import BudgetPolicy, ConfigError, parse_config
from hardcap.estimator import OutputEstimator, estimate_input_tokens


def test_p95_estimator_warms_up_then_tracks_tail():
    est = OutputEstimator(BudgetPolicy(estimator="p95", p95_min_samples=20))
    assert est.estimate("r", 1024) == 1024  # cold: fall back to max_tokens
    for i in range(200):
        est.observe("r", i % 100 + 1)
    assert 85 <= est.estimate("r", 1024) <= 100
    assert est.estimate("r", 50) == 50  # never above the request's cap


def test_fixed_and_max_tokens_estimators():
    assert OutputEstimator(BudgetPolicy(estimator="max_tokens")).estimate("r", 700) == 700
    fixed = OutputEstimator(BudgetPolicy(estimator="fixed", fixed_estimate=128))
    assert fixed.estimate("r", 700) == 128
    assert fixed.estimate("r", 64) == 64


def test_input_estimate_counts_messages():
    body = {"messages": [{"role": "user", "content": "x" * 400}]}
    assert estimate_input_tokens(body) == 4 + 100 + 3


def test_breaker_opens_half_opens_and_closes():
    now = [0.0]
    b = CircuitBreaker(failure_threshold=3, cooldown_s=10, clock=lambda: now[0])
    for _ in range(3):
        assert b.allow()
        b.record_failure()
    assert b.state == "open" and not b.allow()
    now[0] = 10.5
    assert b.state == "half_open"
    assert b.allow() and not b.allow()  # exactly one probe
    b.record_failure()
    assert b.state == "open"
    now[0] = 21
    assert b.allow()
    b.record_success()
    assert b.state == "closed" and b.allow()


def test_config_validation():
    with pytest.raises(ConfigError):
        parse_config({"budget": {"accounting": "magic"}})
    with pytest.raises(ConfigError):
        parse_config({"upstreams": {"a": {}},
                      "routes": {"m": {"targets": [{"upstream": "b", "model": "x"}]}}})
    with pytest.raises(ConfigError):
        parse_config({"tenants": {"a": {"api_keys": ["k"]}, "b": {"api_keys": ["k"]}}})
    cfg = parse_config({
        "upstreams": {"a": {"base_url": "http://x"}},
        "routes": {"m": {"targets": [{"upstream": "a", "model": "x"}]}},
        "tenants": {"t": {"api_keys": ["k"], "budget": {"total": 10}}},
    })
    assert cfg.tenant_by_key()["k"].budget.total == 10
