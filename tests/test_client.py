from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx
import pytest
import respx

from conftest import TEST_API_BASE
from reporadar.config import Settings
from reporadar.github.client import GitHubClient, RateLimitedError

EVENTS_URL = f"{TEST_API_BASE}/events"


@pytest.fixture()
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Remove the retry backoff's wall-clock cost, and record what it asked for.

    Patching the clock rather than the client keeps the real retry path under
    test; the recorded delays let a test assert the backoff happened at all.
    """
    recorded: list[float] = []

    async def _record(seconds: float) -> None:
        recorded.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", _record)
    return recorded


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
async def test_each_page_caches_its_own_etag(
    settings: Settings, event_dict: dict[str, Any]
) -> None:
    # One cache entry per (path, params). Keyed on the path alone, page 2 would be
    # sent page 1's ETag: the server answers 304 for a page we have never read, and
    # list_public_events turns 304 into [] — a page dropped in silence.
    route = respx.get(EVENTS_URL).mock(
        side_effect=[
            httpx.Response(200, json=[event_dict], headers={"ETag": 'W/"page-1"'}),
            httpx.Response(200, json=[event_dict], headers={"ETag": 'W/"page-2"'}),
            httpx.Response(304),
        ]
    )
    async with GitHubClient(settings) as client:
        await client.list_public_events(page=1)
        await client.list_public_events(page=2)
        await client.list_public_events(page=1)

    assert "If-None-Match" not in route.calls[1].request.headers  # page 2 is unseen
    assert route.calls[2].request.headers["If-None-Match"] == 'W/"page-1"'  # its own, not page 2's


@respx.mock
async def test_a_transient_server_error_is_retried(
    settings: Settings, event_dict: dict[str, Any], sleeps: list[float]
) -> None:
    # A 500 is GitHub having a moment, not an answer. Retrying keeps the blip away
    # from the caller; poll_stream only absorbs RateLimitedError, so anything else
    # escaping here ends the always-on service.
    respx.get(EVENTS_URL).mock(
        side_effect=[httpx.Response(500), httpx.Response(200, json=[event_dict])]
    )
    async with GitHubClient(settings) as client:
        events = await client.list_public_events()

    assert len(events) == 1  # the caller never learns the blip happened
    assert sleeps == [2.0]  # and it backed off first, rather than retrying hot


@respx.mock
async def test_a_transport_error_is_retried(
    settings: Settings, event_dict: dict[str, Any], sleeps: list[float]
) -> None:
    # Same contract one layer down: a dropped connection is transient too.
    respx.get(EVENTS_URL).mock(
        side_effect=[httpx.ConnectError("connection reset"), httpx.Response(200, json=[event_dict])]
    )
    async with GitHubClient(settings) as client:
        events = await client.list_public_events()

    assert len(events) == 1
    assert sleeps == [2.0]


@respx.mock
async def test_a_secondary_rate_limit_is_detected_from_retry_after(settings: Settings) -> None:
    # GitHub's *secondary* limits answer 403 + Retry-After while the primary quota
    # is untouched. Reading only x-ratelimit-remaining would call this a plain 403
    # and keep hammering — the behaviour that escalates a slowdown into a block.
    respx.get(EVENTS_URL).mock(
        return_value=httpx.Response(
            403,
            json={"message": "You have exceeded a secondary rate limit"},
            headers={"Retry-After": "45", "X-RateLimit-Remaining": "4999"},
        )
    )
    async with GitHubClient(settings) as client:
        with pytest.raises(RateLimitedError) as excinfo:
            await client.list_public_events()

    assert excinfo.value.retry_after_s == 45.0  # honour the server's number


@respx.mock
async def test_a_reset_already_in_the_past_waits_zero_not_negative(settings: Settings) -> None:
    # Our clock and GitHub's need not agree, so the reset can arrive already spent.
    # An unclamped subtraction hands the caller a negative wait, asyncio.sleep()
    # returns instantly, and the "backoff" becomes a hot loop against a limited API.
    respx.get(EVENTS_URL).mock(
        return_value=httpx.Response(
            403,
            json={"message": "API rate limit exceeded"},
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(int(time.time()) - 30)},
        )
    )
    async with GitHubClient(settings) as client:
        with pytest.raises(RateLimitedError) as excinfo:
            await client.list_public_events()

    assert excinfo.value.retry_after_s == 0.0


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


@respx.mock
async def test_poll_interval_is_taken_from_the_server(settings: Settings) -> None:
    # GitHub states how often it is willing to be polled. Reading that off the
    # response instead of hardcoding 60 keeps the number true if GitHub ever
    # changes it — the same reason the rate limit is read rather than assumed.
    respx.get(EVENTS_URL).mock(
        return_value=httpx.Response(200, json=[], headers={"X-Poll-Interval": "60"})
    )
    async with GitHubClient(settings) as client:
        await client.list_public_events()

        assert client.last_poll_interval_s == 60.0


@respx.mock
async def test_a_response_without_the_header_is_not_permission_to_speed_up(
    settings: Settings,
) -> None:
    # Sticky by design: once the server has asked for a cadence, a later response
    # that merely omits the header must not silently restore a faster one.
    respx.get(EVENTS_URL).mock(
        side_effect=[
            httpx.Response(200, json=[], headers={"X-Poll-Interval": "60"}),
            httpx.Response(200, json=[]),  # no header at all
        ]
    )
    async with GitHubClient(settings) as client:
        await client.list_public_events()
        await client.list_public_events(page=2)

        assert client.last_poll_interval_s == 60.0


@respx.mock
async def test_a_nonsense_poll_interval_is_ignored(settings: Settings) -> None:
    # The header is server-controlled input, so it is parsed defensively: a value
    # that is not a plain integer leaves the cadence unchanged rather than raising
    # inside the poll loop or being coerced into something arbitrary.
    respx.get(EVENTS_URL).mock(
        return_value=httpx.Response(200, json=[], headers={"X-Poll-Interval": "soon"})
    )
    async with GitHubClient(settings) as client:
        await client.list_public_events()

        assert client.last_poll_interval_s is None
