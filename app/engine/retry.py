"""Retry backoff: exponential with full jitter.

delay = min(cap, base * 2^(attempt-1)) * uniform(0.5, 1.5)

Jitter prevents thundering herds: without it, a burst of jobs failing together
(e.g. a downstream outage) would all retry at the same instant and fail together again.
"""

import random


def backoff_seconds(attempt: int, base: float = 5.0, cap: float = 300.0, jitter: bool = True) -> float:
    if attempt < 1:
        attempt = 1
    delay = min(cap, base * (2 ** (attempt - 1)))
    if jitter:
        delay *= random.uniform(0.5, 1.5)
    return delay
