from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from reporadar.github.events import RawEvent
from reporadar.ingest.wire import (
    SCHEMA_VERSION,
    UnsupportedSchemaVersionError,
    decode_value,
    encode_key,
    encode_value,
)


def _event(event_id: str = "1", repo_id: int = 2) -> RawEvent:
    return RawEvent.model_validate(
        {
            "id": event_id,
            "type": "PushEvent",
            "actor": {"id": 1, "login": "octo-tester"},
            "repo": {"id": repo_id, "name": "octo/widgets"},
            "created_at": "2026-07-07T15:00:00Z",
            "payload": {"ref": "refs/heads/main"},
        }
    )


def test_value_round_trips_event_version_and_capture_time() -> None:
    event = _event()
    captured = datetime(2026, 7, 14, 18, 0, 0, 123456, tzinfo=UTC)

    envelope = decode_value(encode_value(event, captured_at=captured))

    assert envelope.v == SCHEMA_VERSION
    assert envelope.captured_at == captured
    assert envelope.event == event  # payload, ids, and event time all survive the wire


def test_value_is_self_describing_json() -> None:
    # Anything reading the raw bytes (kcat, a DLQ dump) sees the version and both clocks.
    raw = json.loads(encode_value(_event(), captured_at=datetime(2026, 7, 14, tzinfo=UTC)))
    assert raw["v"] == SCHEMA_VERSION
    assert raw["event"]["id"] == "1"
    assert raw["captured_at"]  # present; the exact string form is pydantic's to choose


def test_capture_time_defaults_to_now_utc() -> None:
    before = datetime.now(tz=UTC)
    envelope = decode_value(encode_value(_event()))
    after = datetime.now(tz=UTC)

    assert before <= envelope.captured_at <= after


def test_naive_capture_time_is_refused() -> None:
    with pytest.raises(ValidationError):
        encode_value(_event(), captured_at=datetime(2026, 7, 14, 18, 0, 0))  # no tzinfo


def test_key_is_the_repo_id_as_decimal_bytes() -> None:
    assert encode_key(_event(repo_id=2)) == b"2"
    # Same repo → same key (→ same partition); different repo → different key.
    assert encode_key(_event("a", repo_id=7)) == encode_key(_event("b", repo_id=7))
    assert encode_key(_event(repo_id=7)) != encode_key(_event(repo_id=8))


def test_foreign_version_is_refused_before_shape_validation() -> None:
    # A well-formed v2 envelope with a shape v1 can't know must fail as a *version*
    # problem, not as a confusing shape error.
    data = json.dumps({"v": 2, "frame": {"totally": "different"}}).encode()

    with pytest.raises(UnsupportedSchemaVersionError) as excinfo:
        decode_value(data)

    assert excinfo.value.version == 2


def test_json_that_is_not_an_envelope_is_refused() -> None:
    with pytest.raises(UnsupportedSchemaVersionError):
        decode_value(b"[1, 2, 3]")  # valid JSON, but not an envelope object


def test_corrupt_bytes_raise_json_error() -> None:
    with pytest.raises(json.JSONDecodeError):
        decode_value(b"{not json")


def test_right_version_wrong_shape_is_a_validation_error() -> None:
    # v says 1 but the event is missing — a producer bug, distinct from version skew.
    with pytest.raises(ValidationError):
        decode_value(json.dumps({"v": 1, "captured_at": "2026-07-14T18:00:00Z"}).encode())
