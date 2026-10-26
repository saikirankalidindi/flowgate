---
title: "Your LLM gateway's token budget is a suggestion"
subtitle: "Counting tokens after the response lets a tenant spend 3.3× its budget. Reserving them first holds it to 0.03%."
tags: [llm, distributed-systems, redis, rate-limiting, open-source]
canonical: https://github.com/saikirankalidindi/flowgate
---

# Your LLM gateway's token budget is a suggestion

If several teams share one LLM bill, someone eventually asks for a hard cap: *team A gets two
million tokens an hour, and not one more.* Most LLM gateways offer that. I wanted to know how hard
the cap really is, so I built a gateway, put a budget on it, and hammered it with concurrent
streams.

With 128 streams in flight per tenant, spread across three gateway replicas, the usual
implementation let a tenant spend **3.3× its budget**. The approach this post describes held
the same tenant to **0.03%** over: 5 tokens on a 20,000-token budget.

![Budget overshoot vs concurrency](https://raw.githubusercontent.com/saikirankalidindi/flowgate/main/docs/img/overshoot.png)

The code, the benchmark and the raw results are open source:
[github.com/saikirankalidindi/flowgate](https://github.com/saikirankalidindi/flowgate).

## Why request limits don't work for LLMs

A classic rate limiter counts requests. That's fine when every request costs about the same. An
LLM request can cost 10 tokens or 100,000, and you don't know which until the response is done:
output length is decided by the model, one token at a time, while the stream is open.

So gateways count tokens instead. The common implementation is:

1. On each request, look up how many tokens the tenant has used this window.
2. If `used < limit`, let it through.
3. When the response finishes, add its real token count to `used`.

Step 3 is the problem. Between admission and settlement, a request is *invisible* to the budget.
Run one request at a time and you overshoot by at most one response. Run 128 at once and all 128
are admitted while the tenant still looks under budget, and all 128 are charged when they finish.
The overshoot grows linearly with concurrency, which is exactly what the blue line shows.

Replicas make it worse, not better. Even with shared state in Redis, nothing reserves the tokens
that in-flight requests are about to use.

## Reserve, then settle

The fix comes from payments, where it is old news: a card terminal *authorizes* an amount before
the sale and *captures* the real amount after. Applied to tokens:

```
admit   used + reserved + estimate ≤ limit ?   → reserved += estimate
stream  every 64 tokens: grow the reservation (or stop if the budget is gone)
settle  reserved -= reservation;  used += actual
```

Each of these is a single Lua script in Redis. Redis runs one script at a time, so for a given
tenant the checks and updates are serialized, no matter how many gateway replicas send them. That
gives one invariant, checked inside every admit and every extension:

> **used + reserved ≤ limit**

Settlement swaps a reservation for the real count, so the only way past the limit is a token that
was generated *without* a reservation. The whole design comes down to deciding what happens to
those tokens.

## The real problem is the estimate

What should you reserve? There are two obvious answers, and both are wrong.

**Reserve `max_tokens`.** It's safe, because the model can't exceed it. But most completions use a
fraction of their cap. With `max_tokens: 1024` and a median response of about 150 tokens, every
in-flight request holds about 6× what it will use. Near the end of a budget, the remaining tokens
are all *held* and none are *used*, so new requests get rejected even though the budget isn't
spent. In the benchmark, about **4% of every budget was never usable**, and at 128 streams each
tenant's clients saw roughly 2,900 "come back later" rejections.

**Reserve a small fixed number.** It's efficient, until a long response blows straight past it.

FlowGate reserves the **rolling p95** of real output lengths for that route (the last 1,000
completed responses) and treats the 5% tail as an *extension* problem. As a stream gets within
32 tokens of its reservation, the gateway asks Redis for 64 more. If the budget can't cover them,
there are two policies:

- **allow**: let the stream finish. Overshoot is bounded by the tail of the streams in flight when
  the budget ran out (+7.9% worst case at 128 streams).
- **truncate** (the default): stop forwarding, send the client a final chunk with
  `finish_reason: "length"`, and close the upstream connection so the provider stops generating.

Truncated responses are kept out of the p95 sample. Otherwise the estimate would drift down, cause
more truncation, drift down further, and spiral.

| 128 streams per tenant, 3 replicas | Worst overshoot | Budget usable | Truncated streams per tenant |
|---|---:|---:|---:|
| Count after the fact | **+234.3%** | 100% | 0 |
| Reserve `max_tokens` | never over | 95.6% | 0 |
| Reserve p95, allow | +7.9% | 98.7% | 0 |
| **Reserve p95, truncate** | **+0.03%** | **98.7%** | 1.2 |

That 0.03% isn't a rounding error, and I didn't want to hide it. It is the tokens a provider
generates between the gateway closing the connection and the provider noticing: a chunk or two
sitting in a TCP buffer. The benchmark only catches them because ground truth is what the *mock
upstream* says it generated, not what the gateway thinks it forwarded. A real provider bills the
former.

## Two kinds of "no"

Reservations created a problem I didn't expect. A tenant with 900 tokens left, all held by
in-flight requests, is in a different position from a tenant whose 900 tokens are *spent*. The
first will have room again in a second; the second has to wait for the window to reset. Return
the same error for both and clients either give up too early or retry a spent budget forever.

So the admission script checks one more thing when it rejects: *would this fit if nothing were in
flight?* If yes, it answers `budget_busy` with `Retry-After: 1`; if no, `budget_exceeded` with the
time until the window resets.

## When a replica dies mid-stream

Reservations are only safe if they can't leak. Each one is stored with an expiry. A live stream
refreshes it, and the next admission for that tenant reclaims any that expired. If a replica
crashes, its tenants get their held budget back after the TTL (two minutes by default). The
benchmark checks this too: across every run, **0 reserved tokens were left behind** once traffic
stopped.

What it can't recover is the *usage* of the streams that died with it. The provider billed those
tokens and the gateway never learned the count. That's in the limitations section of the README,
because it's a real gap.

## What it costs

Two Redis round trips per request, plus a small extension call every 64 tokens. Through the full
path (auth, rate limit, reservation, settlement, Prometheus metrics), measured open-loop on one
laptop:

| Load | Added p50 | Added p99 |
|---|---:|---:|
| 1,000 req/s | +1.1 ms | +1.2 ms |
| 2,500 req/s | +1.6 ms | +3.2 ms |
| 1,000 req/s, streaming | +1.9 ms | +3.0 ms |

For calls that take hundreds of milliseconds to seconds, that's noise. It isn't free at the top
end: five Python workers saturate somewhere between 2,500 and 5,000 req/s on my machine, and the
knee moved between runs. Profiling showed about 65% of the handler's CPU going to the Redis
client's pure-Python protocol code. Switching to its C parser (`hiredis`) moved one worker's limit
from about 1,300 to over 1,600 req/s. If raw throughput were the goal, the hot path would be Go. It
wasn't: the goal was a budget that holds.

## Failover, briefly

A budget gateway also has to route around a failing provider. With a primary → secondary route at
200 streaming req/s:

- **Primary returns 503 for seven seconds:** 0 of 4,000 requests failed, and p99 latency didn't
  move.
- **Primary process killed with SIGKILL:** 26 of 4,000 failed. Every one of them was a stream
  already in flight on the killed process. Once bytes have reached the client, no gateway can
  retry transparently. Every request sent after the kill succeeded.

![Failover timeline](https://raw.githubusercontent.com/saikirankalidindi/flowgate/main/docs/img/failover.png)

## What I'd tell someone building this

1. **Measure overshoot against the provider, not your ledger.** In my runs the ledger and the
   upstream agreed to within 40 tokens, but only checking the upstream told me that.
2. **The estimator matters more than the locking.** Atomic Redis scripts are the easy part.
   Reserving the right amount decides whether you leave 4% of the budget unusable or 1.3%.
3. **Split "busy" from "spent".** It's one extra comparison, and it decides whether clients can
   behave sensibly.
4. **Publish the uncomfortable numbers.** Where it saturates, what a crash loses, which estimate
   the benchmark doesn't test (input tokens, counted with a heuristic that the mock upstream
   shares). A benchmark that only shows wins doesn't tell anyone much.

Everything here reproduces with one command per benchmark. The raw JSON, the machine specs and the
parameters are committed next to the charts:
**[github.com/saikirankalidindi/flowgate](https://github.com/saikirankalidindi/flowgate)**.
