"""Configuration model. Loaded from YAML; every field has a default so small configs stay small."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

ACCOUNTING_MODES = ("posthoc", "reserve")
ESTIMATORS = ("max_tokens", "p95", "fixed")
OVERRUN_POLICIES = ("allow", "truncate")
RATE_ALGORITHMS = ("sliding_window", "token_bucket")
UPSTREAM_KINDS = ("openai", "anthropic")


class ConfigError(ValueError):
    pass


@dataclass
class BudgetPolicy:
    """How token budgets are accounted for. Shared by all tenants."""

    # posthoc: check `used < limit` on admission, charge actual tokens when the response ends.
    #          This is what most gateways do, and it lets concurrent requests overshoot.
    # reserve: atomically reserve an estimate on admission, settle to the real count at the end.
    accounting: str = "reserve"
    # How the output-token reservation is sized.
    estimator: str = "p95"
    fixed_estimate: int = 256
    # Used when the client sends no max_tokens; also the cap passed upstream in that case.
    default_max_tokens: int = 4096
    # What happens when a stream outgrows its reservation and the budget cannot cover more:
    # allow lets it finish (bounded overshoot), truncate ends the stream (zero overshoot).
    on_overrun: str = "truncate"
    # Tokens requested per mid-stream extension.
    extend_chunk: int = 64
    # Reservations from a replica that died are reclaimed after this long.
    reservation_ttl_s: float = 120.0
    # Samples kept per route for the p95 estimator, and the estimate used before it is warm.
    p95_window: int = 1000
    p95_min_samples: int = 50


@dataclass
class TenantBudget:
    window_s: int = 60
    total: int = 0  # 0 = unlimited
    input: int = 0
    output: int = 0


@dataclass
class RateLimit:
    algorithm: str = "sliding_window"
    requests: int = 0  # 0 = unlimited
    per_s: float = 1.0
    burst: int = 0  # token_bucket capacity; defaults to `requests`


@dataclass
class Tenant:
    name: str
    api_keys: list[str]
    budget: TenantBudget = field(default_factory=TenantBudget)
    rate_limit: RateLimit = field(default_factory=RateLimit)


@dataclass
class Upstream:
    name: str
    kind: str = "openai"
    base_url: str = "http://localhost:9001"
    api_key: str | None = None
    connect_timeout_s: float = 2.0
    # Max idle time between bytes from the upstream (covers time to first byte).
    read_timeout_s: float = 30.0
    total_timeout_s: float = 600.0


@dataclass
class Target:
    upstream: str
    model: str


@dataclass
class Route:
    name: str
    targets: list[Target]


@dataclass
class Breaker:
    failure_threshold: int = 5
    cooldown_s: float = 10.0


@dataclass
class Price:
    input: float = 0.0  # USD per 1M tokens
    output: float = 0.0


@dataclass
class Config:
    host: str = "0.0.0.0"
    port: int = 8080
    redis_url: str = "redis://localhost:6379/0"
    # Connections per process; requests wait for a free one rather than failing.
    redis_max_connections: int = 256
    key_prefix: str = "fg"
    budget: BudgetPolicy = field(default_factory=BudgetPolicy)
    breaker: Breaker = field(default_factory=Breaker)
    upstreams: dict[str, Upstream] = field(default_factory=dict)
    routes: dict[str, Route] = field(default_factory=dict)
    tenants: dict[str, Tenant] = field(default_factory=dict)
    pricing: dict[str, Price] = field(default_factory=dict)
    # Forward the tenant name upstream as x-flowgate-tenant (useful for upstream-side attribution).
    forward_tenant_header: bool = True

    def tenant_by_key(self) -> dict[str, Tenant]:
        return {k: t for t in self.tenants.values() for k in t.api_keys}


def _pick(cls: type, raw: dict[str, Any] | None, where: str) -> Any:
    raw = dict(raw or {})
    known = set(cls.__dataclass_fields__)
    unknown = set(raw) - known
    if unknown:
        raise ConfigError(f"{where}: unknown keys {sorted(unknown)}")
    return cls(**raw)


def _choice(value: str, allowed: tuple[str, ...], where: str) -> None:
    if value not in allowed:
        raise ConfigError(f"{where}: {value!r} is not one of {allowed}")


def parse_config(raw: dict[str, Any]) -> Config:
    raw = dict(raw or {})
    listen = raw.pop("listen", {}) or {}
    cfg = Config(
        host=listen.get("host", "0.0.0.0"),
        port=int(listen.get("port", 8080)),
        redis_url=raw.pop("redis_url", "redis://localhost:6379/0"),
        redis_max_connections=int(raw.pop("redis_max_connections", 256)),
        key_prefix=raw.pop("key_prefix", "fg"),
        forward_tenant_header=bool(raw.pop("forward_tenant_header", True)),
    )
    cfg.budget = _pick(BudgetPolicy, raw.pop("budget", None), "budget")
    _choice(cfg.budget.accounting, ACCOUNTING_MODES, "budget.accounting")
    _choice(cfg.budget.estimator, ESTIMATORS, "budget.estimator")
    _choice(cfg.budget.on_overrun, OVERRUN_POLICIES, "budget.on_overrun")
    cfg.breaker = _pick(Breaker, raw.pop("breaker", None), "breaker")

    for name, u in (raw.pop("upstreams", None) or {}).items():
        u = dict(u)
        key_env = u.pop("api_key_env", None)
        up = _pick(Upstream, {"name": name, **u}, f"upstreams.{name}")
        if key_env:
            up.api_key = os.environ.get(key_env)
        _choice(up.kind, UPSTREAM_KINDS, f"upstreams.{name}.kind")
        cfg.upstreams[name] = up

    for name, r in (raw.pop("routes", None) or {}).items():
        targets = [_pick(Target, t, f"routes.{name}.targets") for t in r.get("targets", [])]
        if not targets:
            raise ConfigError(f"routes.{name}: needs at least one target")
        for t in targets:
            if t.upstream not in cfg.upstreams:
                raise ConfigError(f"routes.{name}: unknown upstream {t.upstream!r}")
        cfg.routes[name] = Route(name=name, targets=targets)

    for name, p in (raw.pop("pricing", None) or {}).items():
        cfg.pricing[name] = _pick(Price, p, f"pricing.{name}")

    for name, t in (raw.pop("tenants", None) or {}).items():
        t = dict(t)
        tenant = Tenant(
            name=name,
            api_keys=list(t.pop("api_keys", [])),
            budget=_pick(TenantBudget, t.pop("budget", None), f"tenants.{name}.budget"),
            rate_limit=_pick(RateLimit, t.pop("rate_limit", None), f"tenants.{name}.rate_limit"),
        )
        if t:
            raise ConfigError(f"tenants.{name}: unknown keys {sorted(t)}")
        if not tenant.api_keys:
            raise ConfigError(f"tenants.{name}: needs at least one api key")
        _choice(tenant.rate_limit.algorithm, RATE_ALGORITHMS, f"tenants.{name}.rate_limit")
        if tenant.budget.window_s <= 0:
            raise ConfigError(f"tenants.{name}.budget.window_s must be positive")
        cfg.tenants[name] = tenant

    if raw:
        raise ConfigError(f"unknown top-level keys {sorted(raw)}")
    seen: dict[str, str] = {}
    for t in cfg.tenants.values():
        for k in t.api_keys:
            if k in seen:
                raise ConfigError(f"api key reused by tenants {seen[k]!r} and {t.name!r}")
            seen[k] = t.name
    return cfg


def load_config(path: str | Path) -> Config:
    with open(path) as f:
        return parse_config(yaml.safe_load(f))
