# Changelog

## 0.1.0 — 2026-10-26

First public release.

- OpenAI-compatible `/v1/chat/completions` (streaming and non-streaming) in front of
  OpenAI-compatible upstreams (OpenAI, Groq, vLLM, …) and Anthropic
- Per-tenant token budgets (input, output, total, per window) enforced across replicas with
  reserve → extend → settle accounting in Redis Lua scripts
- Output estimators: `max_tokens`, `fixed`, rolling `p95`; overrun policies `truncate` and `allow`
- Request rate limits: sliding-window counter and token bucket, in the same atomic script
- `budget_busy` vs `budget_exceeded` rejections with matching `Retry-After`
- Crash-safe reservations (TTL, reclaimed on next admission)
- Fallback chains on 429 / 5xx / timeouts / connection errors with per-upstream circuit breakers
- Prometheus metrics with per-tenant cost attribution; Grafana dashboard
- `hardcap mock`: a mock LLM upstream for local testing and benchmarks
- Streaming budgets count reasoning (`reasoning`, `reasoning_content`, Anthropic thinking) and
  tool-call deltas, not just visible `content`, so reasoning models can't stream past a budget
- Overshoot, latency-overhead and failover benchmarks with committed results
- Live provider check (`bench/live_providers.py`), run against Groq
