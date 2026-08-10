"""System prompts and output schemas.

Schemas are hand-written rather than generated so they stay inside what structured
outputs accept: every property required, `additionalProperties: false`, and no numeric
constraints (`minimum`/`maximum` are silently unsupported — clamp in Python instead).

Prompts are module constants so they are byte-stable across calls, which is what makes
them cacheable as a prompt prefix.
"""

FAILURE_CATEGORIES = [
    "network", "timeout", "rate_limit", "auth", "bad_input",
    "dependency", "resource", "bug", "unknown",
]

TRIAGE_SYSTEM = """\
You are an on-call SRE triaging a background job that exhausted its retries and was \
moved to a dead-letter queue on a distributed job execution platform.

You will be given the job's type, its payload, and the error from every attempt.

Classify the failure and recommend one action. Be concrete and specific to the evidence \
you were given — a generic answer is worse than no answer.

Definitions you must apply exactly:
- category: the mechanism that caused the failure, from the allowed list.
- is_transient: true only if re-running this job unchanged has a realistic chance of \
succeeding (a network blip, a rate limit, a service that was briefly down). False if the \
job's own input or code is at fault, because a retry would fail identically.
- root_cause: one or two sentences naming what actually failed. Quote the specific \
identifier, status code, or field from the error rather than restating the category. If \
the evidence is genuinely insufficient, say so instead of guessing.
- suggested_action: the single next step an engineer should take, phrased as an \
instruction. If it is safe to requeue as-is, say that.
- confidence: 0.0-1.0. Report low confidence honestly when the error is opaque; a \
confident wrong diagnosis costs more debugging time than an uncertain one.

Never invent error text, stack frames, or system state that was not provided.\
"""

TRIAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "category": {"type": "string", "enum": FAILURE_CATEGORIES},
        "is_transient": {"type": "boolean"},
        "root_cause": {"type": "string"},
        "suggested_action": {"type": "string"},
        "confidence": {"type": "number"},
    },
    "required": ["category", "is_transient", "root_cause", "suggested_action", "confidence"],
    "additionalProperties": False,
}

SUMMARIZE_SYSTEM = """\
You summarize text for a background processing pipeline. Produce a faithful summary at \
or under the requested word count, plus the key points it rests on. Do not add \
information that is not in the source, and do not editorialize.\
"""

SUMMARIZE_SCHEMA = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "key_points": {"type": "array", "items": {"type": "string"}},
        "word_count": {"type": "integer"},
    },
    "required": ["summary", "key_points", "word_count"],
    "additionalProperties": False,
}

CLASSIFY_SYSTEM = """\
You classify text into exactly one of the caller-supplied labels. Choose the single best \
fit. If none fit well, choose the closest and report low confidence rather than inventing \
a label that was not offered.\
"""


def classify_schema(labels: list[str]) -> dict:
    """Constrain the output to the caller's labels so an unusable value is impossible."""
    return {
        "type": "object",
        "properties": {
            "label": {"type": "string", "enum": labels},
            "confidence": {"type": "number"},
            "reasoning": {"type": "string"},
        },
        "required": ["label", "confidence", "reasoning"],
        "additionalProperties": False,
    }


EXTRACT_SYSTEM = """\
You extract structured fields from unstructured text. Return a value for every requested \
field. Use an empty string when the text genuinely does not contain that field — never \
guess a plausible-looking value, since a fabricated field is worse than a missing one.\
"""


def extract_schema(fields: list[str]) -> dict:
    return {
        "type": "object",
        "properties": {
            "fields": {
                "type": "object",
                "properties": {name: {"type": "string"} for name in fields},
                "required": list(fields),
                "additionalProperties": False,
            },
            "missing_fields": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["fields", "missing_fields"],
        "additionalProperties": False,
    }
