"""Minimal GitHub REST client: authenticated, ETag-aware, rate-limit honest.

Design rule: the client *detects* rate limiting and raises a typed error
carrying the wait time; the caller decides how to react. Detection and policy
stay separately testable.

ETags matter here: a 304 Not Modified does not count against the rate limit,
so conditional polling buys frequency without burning quota.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from types import TracebackType
from typing import Any

import httpx

from reporadar.config import Settings
from reporadar.github.events import RawEvent, parse_event


class RateLimitedError(Exception):
    """GitHub reports the quota exhausted; retry after ``retry_after_s``."""

    def __init__(self, retry_after_s: float) -> None:
        super().__init__(f"GitHub rate limit hit; retry after {retry_after_s:.0f}s")
        self.retry_after_s = retry_after_s


@dataclass(frozen=True)
class RateLimit:
    limit: int | None
    remaining: int | None
    reset_epoch: int | None

    @classmethod
    def from_headers(cls, headers: httpx.Headers) -> RateLimit:
        def _int(name: str) -> int | None:
            value = headers.get(name)
            return int(value) if value is not None and value.isdigit() else None

        return cls(
            limit=_int("x-ratelimit-limit"),
            remaining=_int("x-ratelimit-remaining"),
            reset_epoch=_int("x-ratelimit-reset"),
        )


class GitHubClient:
    """Async client for api.github.com. One instance per task; not thread-safe."""

    _TRANSIENT_ATTEMPTS = 3

    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None) -> None:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": settings.user_agent,
        }
        if settings.github_token:
            headers["Authorization"] = f"Bearer {settings.github_token}"
        self._client = client or httpx.AsyncClient(
            base_url=settings.api_base, headers=headers, timeout=30.0
        )
        self._etags: dict[str, str] = {}
        self.last_rate_limit: RateLimit | None = None
        # How often the server is willing to be polled, as it stated on the last
        # response that said so. It is guidance we are given rather than a number
        # to pick, so it is recorded here beside the rate limit and applied by the
        # loops that sleep. Sticky: a response that omits the header is not
        # permission to speed back up.
        self.last_poll_interval_s: float | None = None

    async def __aenter__(self) -> GitHubClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get_json(self, path: str, params: dict[str, Any] | None = None) -> tuple[int, Any]:
        """GET with ETag caching. Returns ``(status, body)``; 304 → ``(304, None)``.

        Raises ``RateLimitedError`` when the quota is exhausted; retries
        transport errors and 5xx with short backoff before giving up.
        """
        cache_key = f"{path}?{sorted((params or {}).items())!r}"
        headers: dict[str, str] = {}
        if cache_key in self._etags:
            headers["If-None-Match"] = self._etags[cache_key]

        for attempt in range(1, self._TRANSIENT_ATTEMPTS + 1):
            try:
                resp = await self._client.get(path, params=params, headers=headers)
            except httpx.TransportError:
                if attempt == self._TRANSIENT_ATTEMPTS:
                    raise
                await asyncio.sleep(2**attempt)
                continue

            self.last_rate_limit = RateLimit.from_headers(resp.headers)
            poll_interval = self._poll_interval_s(resp.headers)
            if poll_interval is not None:
                self.last_poll_interval_s = poll_interval

            if resp.status_code in (403, 429) and self._looks_rate_limited(resp):
                raise RateLimitedError(self._retry_after_s(resp))
            if resp.status_code >= 500:
                if attempt == self._TRANSIENT_ATTEMPTS:
                    resp.raise_for_status()
                await asyncio.sleep(2**attempt)
                continue
            if resp.status_code == 304:
                return 304, None

            resp.raise_for_status()
            etag = resp.headers.get("etag")
            if etag:
                self._etags[cache_key] = etag
            return resp.status_code, resp.json()

        raise AssertionError("unreachable: retry loop exits via return or raise")

    async def list_public_events(self, page: int = 1, per_page: int = 100) -> list[RawEvent]:
        """One page of /events. A 304 (nothing new for our ETag) yields []."""
        status, body = await self.get_json("/events", params={"page": page, "per_page": per_page})
        if status == 304 or body is None:
            return []
        return [parse_event(item) for item in body]

    @staticmethod
    def _poll_interval_s(headers: httpx.Headers) -> float | None:
        """The server's requested minimum seconds between polls, if it stated one."""
        value = headers.get("x-poll-interval")
        return float(value) if value is not None and value.isdigit() else None

    @staticmethod
    def _looks_rate_limited(resp: httpx.Response) -> bool:
        rate = RateLimit.from_headers(resp.headers)
        return resp.headers.get("retry-after") is not None or rate.remaining == 0

    @staticmethod
    def _retry_after_s(resp: httpx.Response) -> float:
        retry_after = resp.headers.get("retry-after")
        if retry_after is not None and retry_after.isdigit():
            return float(retry_after)
        rate = RateLimit.from_headers(resp.headers)
        if rate.reset_epoch is not None:
            return max(0.0, rate.reset_epoch - time.time())
        return 60.0
