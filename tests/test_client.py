from __future__ import annotations

import time
from typing import Any

import httpx
import pytest
import respx

from reporadar.config import Settings
from reporadar.github.client import GitHubClient, RateLimitedError

EVENTS_URL = "https://api.github.com/events"


@respx.mock
async def test_sends_auth_and_user_agent(settings: Settings, event_dict: dict[str, Any]) -> None:
    route = respx.get(EVENTS_URL).mock(return_value=httpx.Response(200, json=[event_dict]))
    async with GitHubClient(settings) as client:
        events = await client.list_public_events()

    assert len(events) == 1
    sent = route.calls.last.request
    assert sent.headers["Authorization"] == "Bearer test-token"
    assert "reporadar" in sent.headers["User-Agent"]


@respx.mock
async def test_etag_round_trip_treats_304_as_empty(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    route = respx.get(EVENTS_URL).mock(
        side_effect=[
            httpx.Response(200, json=[event_dict], headers={"ETag": 'W/"abc123"'}),
            httpx.Response(304),
        ]
    )
    async with GitHubClient(settings) as client:
        first = await client.list_public_events()
        second = await client.list_public_events()

    assert len(first) == 1
    assert second == []  # 304 → nothing new, and it didn't cost rate limit
    assert route.calls.last.request.headers["If-None-Match"] == 'W/"abc123"'


@respx.mock
async def test_exhausted_quota_raises_typed_error_with_reset(settings: Settings) -> None:
    reset = int(time.time()) + 30
    respx.get(EVENTS_URL).mock(
        return_value=httpx.Response(
            403,
            json={"message": "API rate limit exceeded"},
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(reset)},
        )
    )
    async with GitHubClient(settings) as client:
        with pytest.raises(RateLimitedError) as excinfo:
            await client.list_public_events()

    assert 0 < excinfo.value.retry_after_s <= 31


@respx.mock
async def test_plain_403_is_not_mistaken_for_rate_limit(settings: Settings) -> None:
    # A 403 with quota remaining (e.g. access blocked) must surface as an HTTP
    # error, not silently wait for a reset that will never help.
    respx.get(EVENTS_URL).mock(
        return_value=httpx.Response(
            403, json={"message": "forbidden"}, headers={"X-RateLimit-Remaining": "42"}
        )
    )
    async with GitHubClient(settings) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await client.list_public_events()


@respx.mock
async def test_rate_limit_headers_are_tracked(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    respx.get(EVENTS_URL).mock(
        return_value=httpx.Response(
            200,
            json=[event_dict],
            headers={"X-RateLimit-Limit": "5000", "X-RateLimit-Remaining": "4999"},
        )
    )
    async with GitHubClient(settings) as client:
        await client.list_public_events()
        assert client.last_rate_limit is not None
        assert client.last_rate_limit.limit == 5000
        assert client.last_rate_limit.remaining == 4999
