"""Upstream adapters. Clients always speak the OpenAI chat-completions format; adapters translate
to each upstream and normalize responses back, so the gateway core sees one shape.

Supported: ``openai`` (OpenAI and anything OpenAI-compatible, including vLLM) and ``anthropic``
(Messages API, text content only)."""

from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import aiohttp

from hardcap.config import Upstream
from hardcap.estimator import count_text_tokens


@dataclass
class Usage:
    prompt_tokens: int
    completion_tokens: int


@dataclass
class StreamEvent:
    data: bytes | None = None  # OpenAI-format SSE bytes to forward, if any
    tokens: int = 0  # output tokens carried by this event (approximate until usage arrives)
    usage: Usage | None = None
    usage_only: bool = False  # the OpenAI trailing usage chunk; forward only if the client asked
    done: bool = False


def sse(obj: dict[str, Any]) -> bytes:
    return b"data: " + json.dumps(obj, separators=(",", ":")).encode() + b"\n\n"


SSE_DONE = b"data: [DONE]\n\n"


async def _sse_data(resp: aiohttp.ClientResponse) -> AsyncIterator[bytes]:
    async for line in resp.content:
        line = line.strip()
        if line.startswith(b"data:"):
            yield line[5:].strip()


# Delta fields that carry generated (billed) output, not just the visible answer. Reasoning
# models can spend most of their output in `reasoning` before any `content` appears; counting
# only `content` would let them run unreserved straight past the budget.
_DELTA_TEXT_FIELDS = ("content", "reasoning", "reasoning_content")


def _delta_tokens(chunk: dict[str, Any]) -> int:
    n = 0
    for c in chunk.get("choices") or ():
        d = c.get("delta") or {}
        for f in _DELTA_TEXT_FIELDS:
            if isinstance(d.get(f), str) and d[f]:
                n += count_text_tokens(d[f])
        for tc in d.get("tool_calls") or ():
            args = (tc.get("function") or {}).get("arguments")
            if args:
                n += count_text_tokens(args)
    return n


class OpenAIAdapter:
    path = "/v1/chat/completions"

    def build(self, up: Upstream, model: str, body: dict[str, Any]) -> tuple[str, dict, dict]:
        out = dict(body, model=model)
        if out.get("stream"):
            # Ask for the usage chunk so settlement uses exact counts.
            out["stream_options"] = dict(out.get("stream_options") or {}, include_usage=True)
        headers = {"content-type": "application/json"}
        if up.api_key:
            headers["authorization"] = f"Bearer {up.api_key}"
        return up.base_url.rstrip("/") + self.path, headers, out

    def parse(self, data: dict[str, Any]) -> tuple[dict[str, Any], Usage | None]:
        u = data.get("usage") or None
        usage = Usage(int(u.get("prompt_tokens", 0)), int(u.get("completion_tokens", 0))) if u else None
        return data, usage

    async def stream(self, resp: aiohttp.ClientResponse) -> AsyncIterator[StreamEvent]:
        async for payload in _sse_data(resp):
            if payload == b"[DONE]":
                yield StreamEvent(data=SSE_DONE, done=True)
                return
            chunk = json.loads(payload)
            u = chunk.get("usage")
            usage = Usage(int(u.get("prompt_tokens", 0)), int(u.get("completion_tokens", 0))) if u else None
            yield StreamEvent(
                data=b"data: " + payload + b"\n\n",
                tokens=_delta_tokens(chunk),
                usage=usage,
                usage_only=usage is not None and not chunk.get("choices"),
            )


_STOP_REASONS = {"end_turn": "stop", "stop_sequence": "stop", "max_tokens": "length",
                 "tool_use": "tool_calls"}


class AnthropicAdapter:
    path = "/v1/messages"

    def __init__(self, default_max_tokens: int = 4096):
        self.default_max_tokens = default_max_tokens

    def build(self, up: Upstream, model: str, body: dict[str, Any]) -> tuple[str, dict, dict]:
        system, messages = [], []
        for m in body.get("messages") or []:
            content = m.get("content")
            if isinstance(content, list):
                content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
            if m.get("role") == "system":
                system.append(content or "")
            else:
                messages.append({"role": m.get("role", "user"), "content": content or ""})
        out: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "max_tokens": body.get("max_tokens") or body.get("max_completion_tokens")
            or self.default_max_tokens,
        }
        if system:
            out["system"] = "\n\n".join(system)
        for k in ("temperature", "top_p", "stream"):
            if k in body:
                out[k] = body[k]
        if body.get("stop"):
            stop = body["stop"]
            out["stop_sequences"] = [stop] if isinstance(stop, str) else list(stop)
        headers = {"content-type": "application/json", "anthropic-version": "2023-06-01"}
        if up.api_key:
            headers["x-api-key"] = up.api_key
        return up.base_url.rstrip("/") + self.path, headers, out

    def parse(self, data: dict[str, Any]) -> tuple[dict[str, Any], Usage | None]:
        text = "".join(b.get("text", "") for b in data.get("content") or [] if b.get("type") == "text")
        u = data.get("usage") or {}
        usage = Usage(int(u.get("input_tokens", 0)), int(u.get("output_tokens", 0)))
        return {
            "id": data.get("id", ""),
            "object": "chat.completion",
            "created": int(time.time()),
            "model": data.get("model", ""),
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": _STOP_REASONS.get(data.get("stop_reason"), "stop"),
            }],
            "usage": {"prompt_tokens": usage.prompt_tokens,
                      "completion_tokens": usage.completion_tokens,
                      "total_tokens": usage.prompt_tokens + usage.completion_tokens},
        }, usage

    async def stream(self, resp: aiohttp.ClientResponse) -> AsyncIterator[StreamEvent]:
        msg_id, model, prompt_tokens = "", "", 0
        created = int(time.time())

        def chunk(delta: dict, finish: str | None = None) -> dict:
            return {"id": msg_id, "object": "chat.completion.chunk", "created": created,
                    "model": model, "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}

        async for payload in _sse_data(resp):
            ev = json.loads(payload)
            t = ev.get("type")
            if t == "message_start":
                m = ev.get("message") or {}
                msg_id, model = m.get("id", ""), m.get("model", "")
                prompt_tokens = int((m.get("usage") or {}).get("input_tokens", 0))
                yield StreamEvent(data=sse(chunk({"role": "assistant", "content": ""})))
            elif t == "content_block_delta":
                d = ev.get("delta") or {}
                text = d.get("text", "")
                if text:
                    yield StreamEvent(data=sse(chunk({"content": text})), tokens=count_text_tokens(text))
                else:
                    # thinking and tool-input deltas aren't forwarded, but they are billed output
                    other = d.get("thinking") or d.get("partial_json") or ""
                    if other:
                        yield StreamEvent(tokens=count_text_tokens(other))
            elif t == "message_delta":
                out = int((ev.get("usage") or {}).get("output_tokens", 0))
                finish = _STOP_REASONS.get((ev.get("delta") or {}).get("stop_reason"), "stop")
                yield StreamEvent(data=sse(chunk({}, finish)), usage=Usage(prompt_tokens, out))
            elif t == "message_stop":
                yield StreamEvent(data=SSE_DONE, done=True)
                return
            elif t == "error":
                raise UpstreamStreamError(str(ev.get("error")))


class UpstreamStreamError(RuntimeError):
    pass


def adapter_for(up: Upstream, default_max_tokens: int):
    return AnthropicAdapter(default_max_tokens) if up.kind == "anthropic" else OpenAIAdapter()
