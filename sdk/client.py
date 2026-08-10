"""Tiny Taskforge client.

    from sdk.client import Taskforge

    tf = Taskforge("https://your-app.onrender.com", api_key="tf_live_...")
    job = tf.submit("http_fetch", {"url": "https://example.com"}, max_attempts=5)
    print(tf.wait(job["id"])["result"])
"""

import time
from typing import Any

import httpx


class TaskforgeError(RuntimeError):
    pass


class Taskforge:
    def __init__(self, base_url: str, api_key: str, timeout: float = 15.0):
        self._client = httpx.Client(
            base_url=base_url.rstrip("/") + "/api/v1",
            headers={"X-API-Key": api_key},
            timeout=timeout,
        )

    def submit(self, job_type: str, payload: dict | None = None, *, queue: str = "default",
               priority: int = 0, max_attempts: int = 3, delay_seconds: int | None = None,
               idempotency_key: str | None = None, depends_on: list[str] | None = None,
               callback_url: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {
            "type": job_type, "payload": payload or {}, "queue": queue,
            "priority": priority, "max_attempts": max_attempts,
        }
        if delay_seconds is not None:
            body["delay_seconds"] = delay_seconds
        if idempotency_key:
            body["idempotency_key"] = idempotency_key
        if depends_on:
            body["depends_on"] = depends_on
        if callback_url:
            body["callback_url"] = callback_url
        return self._request("POST", "/jobs", json=body)

    def get(self, job_id: str) -> dict[str, Any]:
        return self._request("GET", f"/jobs/{job_id}")

    def list(self, *, state: str | None = None, queue: str | None = None,
             limit: int = 50) -> dict[str, Any]:
        params = {"limit": limit}
        if state:
            params["state"] = state
        if queue:
            params["queue"] = queue
        return self._request("GET", "/jobs", params=params)

    def cancel(self, job_id: str) -> dict[str, Any]:
        return self._request("POST", f"/jobs/{job_id}/cancel")

    def retry(self, job_id: str) -> dict[str, Any]:
        return self._request("POST", f"/jobs/{job_id}/retry")

    def wait(self, job_id: str, *, timeout: float = 120.0, poll: float = 1.0) -> dict[str, Any]:
        """Block until the job reaches a terminal state."""
        deadline = time.monotonic() + timeout
        while True:
            job = self.get(job_id)
            if job["state"] in ("succeeded", "dead", "canceled"):
                return job
            if time.monotonic() >= deadline:
                raise TimeoutError(f"job {job_id} still {job['state']} after {timeout}s")
            time.sleep(poll)

    def metrics(self) -> dict[str, Any]:
        return self._request("GET", "/metrics/overview")

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "Taskforge":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        response = self._client.request(method, path, **kwargs)
        if response.status_code >= 400:
            try:
                detail = response.json().get("detail", response.text)
            except ValueError:
                detail = response.text
            raise TaskforgeError(f"{response.status_code}: {detail}")
        return response.json()
