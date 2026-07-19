from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import ValidationError

from reporadar.github.events import dedupe, iter_ndjson, parse_event


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
