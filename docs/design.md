# FlowGate design notes

## The problem in one paragraph

A request-count limiter cannot bound LLM spend: one request costs 10 tokens, the next 100K, and
output length is only known when the stream ends. The common fix is to count tokens *after* each
response and reject new requests once `used >= limit`. Under concurrency that check is
meaningless. With C streams in flight, all C were admitted while the tenant still looked under
budget, and all C get charged when they finish. The overshoot grows linearly with concurrency (see
the benchmark: +225% at 128 streams per tenant). Adding gateway replicas makes it worse, because
each replica's view of "used" is even more out of date.

## Reserve, then settle

All per-tenant state lives in Redis, and every transition is a single Lua script, so transitions
for one tenant are serialized across any number of replicas.

```
admit   (one round trip)   rate-limit check
                           reclaim expired reservations
                           check  used + reserved + estimate <= limit   (input, output, total)
                           reserved += estimate; record reservation with a TTL
stream  (every 64 tokens)  extend:  check used + reserved + chunk <= limit; reserved += chunk
                           (or just refresh the TTL on long, slow streams)
settle  (one round trip)   reserved -= this reservation; used += actual
```

**Invariant:** `used + reserved <= limit` holds after every admit and extend, because both check
it atomically before committing. Settlement swaps a reservation `r` for the actual count `a`. The
budget can therefore only be exceeded by tokens that were generated without a reservation
(`a > r`). Where those can come from:

| Policy | Unreserved tokens | Bound |
|---|---|---|
| `estimator: max_tokens` | none: the upstream cannot exceed max_tokens | 0, but holds the full cap per stream |
| `p95`, `on_overrun: allow` | streams that outgrow p95 and are denied an extension | the tail beyond p95 of the streams in flight at exhaustion |
| `p95`, `on_overrun: truncate` | the chunk that crosses the boundary, plus whatever the upstream generates between our close and its noticing | ~1 chunk + TCP pipe per stream |

For non-streaming requests in truncate mode there is nothing to cut mid-way, so admission makes
an *elastic* reservation: at least the estimate, at most `max_tokens`, as much as the headroom
allows. The upstream `max_tokens` is then rewritten to the grant.

## Why the estimator matters

`max_tokens` reservations are always safe and always wasteful. Most completions use a fraction of
their cap, so near the end of a window a tenant's concurrency collapses: the remaining budget is
held by reservations, not by tokens. The benchmark shows the cost: about 4% of every budget is
never usable, and thousands of `budget_busy` retries.

The rolling p95 of real output lengths (per route, per replica, last 1000 completed responses)
reserves about what a response actually uses. That leaves the long tail, the 5% of streams that
outgrow their reservation, to mid-stream extension. Truncated responses are kept out of the
sample, because they would bias the estimate downwards.

## Two kinds of budget rejection

| `error.type` | Meaning | `Retry-After` |
|---|---|---|
| `budget_exceeded` | committed usage alone leaves no room; the window is spent | until the window rolls over |
| `budget_busy` | in-flight reservations hold the rest; it frees up when they settle | 1 s |
| `rate_limit_exceeded` | request-rate limit | until a slot frees |

Without the `busy`/`exceeded` split, reserve-mode clients would either give up too early or retry
a spent budget forever.

## Failure handling

* **Replica crash mid-stream:** its reservations expire after `reservation_ttl_s` (default 120 s)
  and the next admission for that tenant reclaims them. Live streams refresh their TTL every
  `ttl/3`. The crashed stream's real usage is lost: the upstream billed it, and FlowGate never
  learns about it. That is a known gap; see limitations.
* **Client disconnect:** the gateway closes the upstream connection (stopping generation) and
  settles the tokens counted so far.
* **Upstream failure before the first byte:** next target in the route, with per-upstream circuit
  breakers (closed → open after N consecutive failures → one half-open probe after cooldown).
  Breakers are per replica, by design: no coordination on the hot path.
* **Upstream failure mid-stream:** cannot be retried transparently once bytes reached the client.
  The client gets an SSE error event, and the breaker records the failure.
* **Redis unavailable:** admission fails, so requests fail closed (HTTP 500). A budget gateway that
  fails open is a billing incident.

## Limitations (read before trusting the numbers)

* **Input tokens are estimated** (about 4 characters per token) for the reservation and corrected at
  settlement. The mock upstream counts input tokens with the same heuristic, so the benchmark does
  not measure input-estimate error. With a real tokenizer mismatch, a request's input can exceed
  its reservation. Mitigation (planned): a reservation safety margin, or exact tokenizers per
  route.
* **Streaming token counts** before the final usage chunk are approximate (characters / 4 per delta).
  Settlement uses the upstream's reported usage when the stream completes.
* **p95 is per replica.** Replicas warm up independently, and until 50 samples exist they reserve
  `max_tokens`.
* **Fixed windows.** Budgets reset at window boundaries (aligned to the epoch), so a tenant can
  spend a full budget on each side of a boundary.
* **Benchmarks use a mock upstream on the same machine.** That isolates the gateway's behaviour,
  but load generator, gateway, mock and Redis compete for the same CPU cores.
