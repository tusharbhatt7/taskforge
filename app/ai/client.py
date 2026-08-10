"""Anthropic client wrapper.

Two jobs beyond "call the API":

1. **Translate API failures into the platform's retry vocabulary.** A 429 carries a
   `Retry-After` the queue should honour instead of its own backoff; a 400 can never
   succeed on a retry and should dead-letter immediately. That mapping is the whole
   reason an LLM call is a good fit for this platform.

2. **Account for tokens and cost per job**, so the dashboard can show what a workload
   actually spent rather than just that it succeeded.

SDK-level retries are deliberately disabled (`max_retries=0`): the platform *is* the
retry engine. Letting the SDK silently retry would hide attempts from the job's attempt
timeline and double-count against the rate limit the queue is trying to respect.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any

import anthropic

from app.core.config import get_settings
from app.engine.errors import PermanentError, RetryAfterError

logger = logging.getLogger("taskforge.ai")

# USD per million tokens, keyed by model. Cache reads bill at ~0.1x the input rate and
# cache writes at ~1.25x, which is why they are tracked separately below.
PRICING: dict[str, tuple[float, float]] = {
    "claude-opus-5": (5.00, 25.00),
    "claude-opus-4-8": (5.00, 25.00),
    "claude-sonnet-5": (3.00, 15.00),
    "claude-haiku-4-5": (1.00, 5.00),
}
CACHE_READ_MULTIPLIER = 0.1
CACHE_WRITE_MULTIPLIER = 1.25

# Beta flag for server-side refusal fallbacks. Recommended on Opus-5-class models: a
# safety classifier can decline a request, and a fallback recovers it in the same call.
FALLBACK_BETA = "server-side-fallback-2026-07-01"

_MISSING_KEY = (
    "ANTHROPIC_API_KEY is not configured. Set it to enable AI job types "
    "(llm_summarize, llm_classify, llm_extract) and automatic dead-letter triage."
)


@dataclass
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    model: str = ""

    @property
    def cost_usd(self) -> float:
        rate_in, rate_out = PRICING.get(self.model, (0.0, 0.0))
        per_token_in = rate_in / 1_000_000
        return round(
            self.input_tokens * per_token_in
            + self.cache_read_tokens * per_token_in * CACHE_READ_MULTIPLIER
            + self.cache_write_tokens * per_token_in * CACHE_WRITE_MULTIPLIER
            + self.output_tokens * (rate_out / 1_000_000),
            6,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_read_tokens": self.cache_read_tokens,
            "cache_write_tokens": self.cache_write_tokens,
            "cost_usd": self.cost_usd,
        }


@dataclass
class Completion:
    data: dict[str, Any] = field(default_factory=dict)
    usage: Usage = field(default_factory=Usage)


def is_configured() -> bool:
    return bool(get_settings().anthropic_api_key)


class AIClient:
    """Thin async wrapper returning schema-validated JSON."""

    def __init__(self) -> None:
        settings = get_settings()
        if not settings.anthropic_api_key:
            raise PermanentError(_MISSING_KEY)
        self._client = anthropic.AsyncAnthropic(
            api_key=settings.anthropic_api_key,
            timeout=settings.ai_timeout_seconds,
            max_retries=0,  # the platform owns retries — see module docstring
        )
        self._settings = settings
        # Flipped off permanently if the account can't use the fallback beta.
        self._use_fallbacks = settings.ai_server_side_fallbacks

    async def complete_json(self, *, system: str, user: str, schema: dict,
                            max_tokens: int | None = None) -> Completion:
        """Run one request constrained to `schema` and return the parsed object.

        `output_config.format` makes the model emit schema-conforming JSON, so there is
        no prose to strip and no brittle regex extraction.
        """
        response = await self._request(system=system, user=user, schema=schema,
                                       max_tokens=max_tokens or self._settings.ai_max_tokens)

        # A safety classifier can decline the request: HTTP 200, no usable content.
        # Checking stop_reason before touching content is mandatory — indexing content[0]
        # on a refusal raises an unrelated IndexError that hides the real cause.
        if response.stop_reason == "refusal":
            category = getattr(response.stop_details, "category", None)
            raise PermanentError(
                f"request declined by safety classifiers (category={category}). "
                "Rewording the payload is the only remedy; retrying will not help."
            )
        if response.stop_reason == "max_tokens":
            raise RuntimeError(
                "response hit max_tokens before the JSON was complete — "
                "raise ai_max_tokens or shorten the input"
            )

        text = next((b.text for b in response.content if b.type == "text"), "")
        if not text.strip():
            raise RuntimeError(f"model returned no text content (stop_reason={response.stop_reason})")
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"model returned malformed JSON: {exc}") from exc

        return Completion(data=data, usage=self._usage_of(response))

    async def _request(self, *, system: str, user: str, schema: dict, max_tokens: int):
        kwargs: dict[str, Any] = {
            "model": self._settings.ai_model,
            "max_tokens": max_tokens,
            # A stable system prompt is a cache prefix; volatile per-job content goes in
            # the user turn so it never invalidates the cached portion.
            "system": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": user}],
            "output_config": {"format": {"type": "json_schema", "schema": schema}},
        }
        if self._settings.ai_effort:
            kwargs["output_config"]["effort"] = self._settings.ai_effort

        try:
            if self._use_fallbacks:
                try:
                    return await self._client.beta.messages.create(
                        betas=[FALLBACK_BETA], fallbacks="default", **kwargs)
                except anthropic.BadRequestError as exc:
                    if "fallback" not in str(exc).lower():
                        raise
                    # The account can't use the fallback beta. Degrade once, loudly,
                    # rather than dead-lettering every AI job on this deployment.
                    logger.warning("server-side fallbacks unavailable, disabling: %s", exc)
                    self._use_fallbacks = False
            return await self._client.messages.create(**kwargs)
        except Exception as exc:
            raise self._translate(exc) from exc

    @staticmethod
    def _usage_of(response) -> Usage:
        usage = response.usage
        return Usage(
            input_tokens=usage.input_tokens or 0,
            output_tokens=usage.output_tokens or 0,
            cache_read_tokens=getattr(usage, "cache_read_input_tokens", 0) or 0,
            cache_write_tokens=getattr(usage, "cache_creation_input_tokens", 0) or 0,
            model=response.model,
        )

    @staticmethod
    def _translate(exc: Exception) -> Exception:
        """Map an SDK exception onto the platform's retry semantics.

        Ordered most-specific first: RateLimitError and the 4xx classes are subclasses of
        APIStatusError, so a broad catch would swallow the distinction that decides
        whether a job retries at all.
        """
        if isinstance(exc, anthropic.RateLimitError):
            retry_after = _retry_after_of(exc, default=60.0)
            return RetryAfterError(f"rate limited by the Anthropic API: {exc}", retry_after)

        # 401/403/404 are configuration faults; 400 means the request itself is invalid.
        # None of them change on a retry.
        if isinstance(exc, (anthropic.AuthenticationError, anthropic.PermissionDeniedError,
                            anthropic.NotFoundError, anthropic.BadRequestError)):
            return PermanentError(f"{type(exc).__name__}: {exc}")

        if isinstance(exc, anthropic.APIStatusError):
            # 529 overloaded and 5xx are transient; anything else 4xx is not.
            if exc.status_code == 529 or exc.status_code >= 500:
                return RetryAfterError(f"Anthropic API unavailable ({exc.status_code}): {exc}",
                                       _retry_after_of(exc, default=15.0))
            return PermanentError(f"HTTP {exc.status_code} from Anthropic API: {exc}")

        # Timeouts and connection failures are retryable by nature.
        if isinstance(exc, (anthropic.APITimeoutError, anthropic.APIConnectionError)):
            return RuntimeError(f"{type(exc).__name__}: {exc}")

        return exc


def _retry_after_of(exc: Exception, default: float) -> float:
    """Read the Retry-After header the API sent, falling back to a sane delay."""
    response = getattr(exc, "response", None)
    header = getattr(response, "headers", {}) or {}
    try:
        return float(header.get("retry-after", default))
    except (TypeError, ValueError):
        return default
