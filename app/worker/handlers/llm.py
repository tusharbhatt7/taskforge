"""LLM job handlers.

LLM calls are the canonical modern queue workload: seconds-to-minutes of latency, hard
rate limits, and transient failures. Running them inline in a request handler means the
user waits and a 429 becomes a user-visible error; running them here means the platform's
backoff, rate-limit-aware retries, and dead-letter queue apply for free.
"""

from app.ai.client import AIClient
from app.ai.prompts import (
    CLASSIFY_SYSTEM,
    EXTRACT_SYSTEM,
    SUMMARIZE_SCHEMA,
    SUMMARIZE_SYSTEM,
    classify_schema,
    extract_schema,
)
from app.core.config import get_settings
from app.engine.errors import PermanentError
from app.worker.handlers import JobContext, handler


def _require_text(payload: dict) -> str:
    text = payload.get("text")
    if not isinstance(text, str) or not text.strip():
        # The payload is wrong, not the world — no retry will fix it.
        raise PermanentError("payload must include a non-empty 'text' string")
    limit = get_settings().ai_max_input_chars
    if len(text) > limit:
        raise PermanentError(
            f"'text' is {len(text)} chars, over the {limit} limit. Split it into "
            "multiple jobs — chunking here would silently truncate the input."
        )
    return text


def _require_list(payload: dict, key: str, *, max_items: int) -> list[str]:
    values = payload.get(key)
    if not isinstance(values, list) or not values:
        raise PermanentError(f"payload must include a non-empty '{key}' array")
    if not all(isinstance(v, str) and v.strip() for v in values):
        raise PermanentError(f"every entry in '{key}' must be a non-empty string")
    if len(values) > max_items:
        raise PermanentError(f"'{key}' accepts at most {max_items} entries")
    return values


@handler("llm_summarize")
async def llm_summarize(payload: dict, ctx: JobContext) -> dict:
    text = _require_text(payload)
    max_words = min(int(payload.get("max_words", 120)), 1000)

    completion = await AIClient().complete_json(
        system=SUMMARIZE_SYSTEM,
        user=f"Summarize the following in at most {max_words} words.\n\n---\n{text}",
        schema=SUMMARIZE_SCHEMA,
    )
    return {**completion.data, "usage": completion.usage.as_dict()}


@handler("llm_classify")
async def llm_classify(payload: dict, ctx: JobContext) -> dict:
    text = _require_text(payload)
    labels = _require_list(payload, "labels", max_items=50)

    completion = await AIClient().complete_json(
        system=CLASSIFY_SYSTEM,
        # The schema constrains `label` to this enum, so the result is always one of
        # the caller's labels — no post-hoc validation or fuzzy matching needed.
        user=f"Labels: {', '.join(labels)}\n\nClassify this text:\n\n---\n{text}",
        schema=classify_schema(labels),
    )
    return {**completion.data, "usage": completion.usage.as_dict()}


@handler("llm_extract")
async def llm_extract(payload: dict, ctx: JobContext) -> dict:
    text = _require_text(payload)
    fields = _require_list(payload, "fields", max_items=30)

    completion = await AIClient().complete_json(
        system=EXTRACT_SYSTEM,
        user=f"Fields to extract: {', '.join(fields)}\n\nText:\n\n---\n{text}",
        schema=extract_schema(fields),
    )
    return {**completion.data, "usage": completion.usage.as_dict()}
