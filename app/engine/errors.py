"""Handler failure classification.

By default a failed attempt is retried until `max_attempts` is exhausted. That is the
right policy for a network blip and the wrong one for a malformed payload: retrying a
400 Bad Request three times just burns the retry budget to reach the same conclusion.

Handlers raise these to tell the engine what kind of failure it was. Anything else is
treated as retryable, so an unannotated handler keeps the original behaviour.
"""


class PermanentError(Exception):
    """This attempt will fail identically on every retry — dead-letter it now.

    For failures caused by the input rather than the environment: an invalid payload,
    revoked credentials, a request the downstream service rejects on its merits.
    """


class RetryAfterError(Exception):
    """Retryable, but the downstream service told us exactly how long to wait.

    Honouring a server-supplied delay beats our own exponential backoff: a 429 carrying
    `Retry-After: 30` means a retry at 5s is guaranteed to fail *and* spends another
    request against the rate limit.
    """

    def __init__(self, message: str, retry_after: float):
        super().__init__(message)
        self.retry_after = max(0.0, float(retry_after))
