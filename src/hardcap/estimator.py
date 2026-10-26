"""Token estimates used before the real counts are known."""

from __future__ import annotations

from collections import deque
from typing import Any

from hardcap.config import BudgetPolicy


def count_text_tokens(text: str) -> int:
    """Rough token count (~4 characters per token). Good enough for reservations; settlement
    always uses the upstream's reported usage when it has one."""
    return max(1, round(len(text) / 4)) if text else 0


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):  # OpenAI content parts
        return "".join(p.get("text", "") for p in content if isinstance(p, dict))
    return ""


def estimate_input_tokens(body: dict[str, Any]) -> int:
    msgs = body.get("messages") or []
    # ~4 tokens of per-message framing, as in OpenAI's chat format.
    return sum(4 + count_text_tokens(_content_text(m.get("content"))) for m in msgs) + 3


class OutputEstimator:
    """Sizes the output-token reservation per route.

    max_tokens  reserve the client's cap: never overshoots, but holds far more than most
                responses use, which throttles concurrency near the limit.
    fixed       reserve a constant.
    p95         reserve the rolling p95 of recent real output lengths for the route, per replica.
                Falls back to max_tokens until enough samples exist.
    """

    def __init__(self, policy: BudgetPolicy):
        self.policy = policy
        self._samples: dict[str, deque[int]] = {}
        self._p95: dict[str, int] = {}
        self._since: dict[str, int] = {}

    def estimate(self, route: str, max_tokens: int) -> int:
        p = self.policy
        if p.estimator == "max_tokens":
            return max_tokens
        if p.estimator == "fixed":
            return min(p.fixed_estimate, max_tokens)
        p95 = self._p95.get(route)
        return max_tokens if p95 is None else max(1, min(p95, max_tokens))

    def observe(self, route: str, output_tokens: int) -> None:
        if self.policy.estimator != "p95":
            return
        s = self._samples.get(route)
        if s is None:
            s = self._samples[route] = deque(maxlen=self.policy.p95_window)
        s.append(output_tokens)
        n = self._since.get(route, 0) + 1
        # Recompute every 32 samples; sorting 1000 ints is cheap but not free per request.
        if len(s) >= self.policy.p95_min_samples and (n >= 32 or route not in self._p95):
            ordered = sorted(s)
            self._p95[route] = ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]
            n = 0
        self._since[route] = n
