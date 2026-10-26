"""Prometheus metrics. With --workers > 1 the CLI sets PROMETHEUS_MULTIPROC_DIR before import,
so counters and histograms aggregate across worker processes."""

from __future__ import annotations

from prometheus_client import Counter, Histogram

_LAT = (0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)

REQUESTS = Counter("flowgate_requests_total", "Requests by outcome",
                   ["tenant", "route", "status"])
REJECTIONS = Counter("flowgate_rejections_total", "Requests rejected by limits",
                     ["tenant", "reason"])
TOKENS = Counter("flowgate_tokens_total", "Tokens charged, by direction",
                 ["tenant", "route", "model", "direction"])
COST = Counter("flowgate_cost_usd_total", "Cost attributed from the pricing table",
               ["tenant", "route", "model"])
TRUNCATIONS = Counter("flowgate_truncations_total", "Streams ended because the budget ran out",
                      ["tenant", "route"])
RESERVATION_ERROR = Histogram(
    "flowgate_reservation_error_tokens",
    "Actual minus initially reserved output tokens (positive = under-reserved)",
    ["route"], buckets=(-4096, -1024, -256, -64, -16, 0, 16, 64, 256, 1024, 4096))
ADMISSION_LATENCY = Histogram("flowgate_admission_seconds",
                              "Gateway time before the upstream call (auth, limits, reservation)",
                              buckets=_LAT)
UPSTREAM_HEADERS_LATENCY = Histogram("flowgate_upstream_headers_seconds",
                                     "Time until upstream response headers", ["upstream"],
                                     buckets=_LAT)
REQUEST_LATENCY = Histogram("flowgate_request_seconds", "End-to-end request time",
                            ["route", "stream"], buckets=_LAT)
UPSTREAM_ERRORS = Counter("flowgate_upstream_errors_total", "Failed upstream attempts",
                          ["upstream", "kind"])
FALLBACKS = Counter("flowgate_fallbacks_total", "Requests served by a non-primary target",
                    ["route", "upstream"])
BREAKER_TRANSITIONS = Counter("flowgate_breaker_transitions_total",
                              "Circuit breaker state changes", ["upstream", "state"])
