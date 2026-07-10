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
    assert event.created_at.tzinfo is not None  # naive datetimes are forbidden


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
