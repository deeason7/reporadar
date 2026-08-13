from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import ValidationError

from reporadar.github.events import dedupe, iter_ndjson, parse_event, parse_page


def test_parse_event_from_dict(event_dict: dict[str, Any]) -> None:
    event = parse_event(event_dict)
    assert event.id == "45000000001"
    assert event.type == "PushEvent"
    assert event.actor.login == "octo-tester"
    assert event.repo.name == "octo/widgets"
    assert event.payload["size"] == 1


def test_event_time_is_timezone_aware(event_dict: dict[str, Any]) -> None:
    event = parse_event(event_dict)
    assert event.created_at.tzinfo is not None  # the API's Z suffix parses to an offset


def test_naive_event_time_is_refused(event_dict: dict[str, Any]) -> None:
    # astimezone() reads a naive value as the *local* time of whichever machine is
    # holding it, so an unmarked timestamp would land in a different hour file and a
    # different database row depending on where ingest happened to run — with no error.
    # Refusing beats assuming UTC: a guess would bury the upstream bug instead of
    # surfacing it, and the caller already routes parse failures to a dead-letter path.
    with pytest.raises(ValidationError):
        parse_event({**event_dict, "created_at": "2026-07-07T15:00:00"})  # no Z


def test_event_time_may_carry_any_offset(event_dict: dict[str, Any]) -> None:
    # The rule is awareness, not UTC: an offset names an instant just as well, and
    # 10:00-05:00 is the same moment as the fixture's 15:00Z.
    shifted = parse_event({**event_dict, "created_at": "2026-07-07T10:00:00-05:00"})
    assert shifted.created_at == parse_event(event_dict).created_at


def test_parse_event_from_json_string(event_dict: dict[str, Any]) -> None:
    event = parse_event(json.dumps(event_dict))
    assert event.id == event_dict["id"]


def test_envelope_is_frozen(event_dict: dict[str, Any]) -> None:
    event = parse_event(event_dict)
    with pytest.raises(ValidationError):
        # Static checkers don't know pydantic's frozen; the runtime does — that's the test.
        event.type = "WatchEvent"


def test_iter_ndjson_skips_blank_lines(event_dict: dict[str, Any]) -> None:
    lines = [json.dumps(event_dict), "", "   ", json.dumps({**event_dict, "id": "2"})]
    events = list(iter_ndjson(lines))
    assert [e.id for e in events] == ["45000000001", "2"]


def test_iter_ndjson_raises_on_malformed_line(event_dict: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        list(iter_ndjson([json.dumps(event_dict), "{not json"]))


def test_dedupe_preserves_first_occurrence_order(event_dict: dict[str, Any]) -> None:
    first = parse_event({**event_dict, "id": "a"})
    second = parse_event({**event_dict, "id": "b"})
    assert dedupe([first, second, first, second]) == [first, second]


# The redacted-repo shape below is not invented. It is the envelope of a real item
# the live feed served on 2026-08-12, and an identical one sits in the GH Archive
# hour 2026-07-22-23 at line 38992 (id 12166351577). The measured rate across three
# archive hours is 1 in 488,274 events -- about two per million, which for an
# always-on poller is one every 29 to 36 hours.
REDACTED_REPO_EVENT: dict[str, Any] = {
    "id": "12166351577",
    "type": "ForkEvent",
    "actor": {"id": 9384210, "login": "octo-forker"},
    "repo": {},  # blanked upstream: the repository is no longer publicly visible
    "public": False,
    "created_at": "2026-07-22T23:14:07Z",
    "payload": {"forkee": {"private": True}},
}


def test_parse_page_keeps_the_good_events_when_one_item_is_redacted(
    event_dict: dict[str, Any],
) -> None:
    # The whole point: one bad item used to end the run, because the page was built
    # in a list comprehension and the exception escaped the sweep.
    events, rejected = parse_page([event_dict, REDACTED_REPO_EVENT, {**event_dict, "id": "2"}])

    assert [e.id for e in events] == ["45000000001", "2"]
    assert len(rejected) == 1


def test_parse_page_keeps_the_rejected_item_whole(event_dict: dict[str, Any]) -> None:
    # A reason without the item cannot be replayed and cannot be re-read when the
    # reason turns out to be wrong about itself.
    _, rejected = parse_page([REDACTED_REPO_EVENT])

    assert rejected[0].raw == REDACTED_REPO_EVENT
    assert "repo" in rejected[0].reason  # names the field that failed, for triage


def test_parse_page_is_empty_in_both_halves_for_an_empty_page() -> None:
    assert parse_page([]) == ([], [])


def test_redaction_is_not_corruption(event_dict: dict[str, Any]) -> None:
    # Everything except `repo` is intact and well-formed, which is why this is a
    # model overclaiming rather than a feed misbehaving: GitHub blanks the repo
    # object when a repository goes private and leaves the rest alone. Asserting it
    # here so a future reader does not "fix" this by hardening the parser against
    # garbage -- there is no garbage.
    _, rejected = parse_page([REDACTED_REPO_EVENT])
    raw = rejected[0].raw

    assert raw["id"] == "12166351577"
    assert raw["type"] == "ForkEvent"
    assert raw["actor"]["login"] == "octo-forker"
    assert raw["created_at"].endswith("Z")
    assert raw["payload"]["forkee"]["private"] is True
    assert raw["repo"] == {}  # the one field that is gone
