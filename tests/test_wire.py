from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from reporadar.github.events import RawEvent
from reporadar.ingest.wire import (
    DLQ_SCHEMA_VERSION,
    SCHEMA_VERSION,
    UnsupportedSchemaVersionError,
    decode_dead_letter,
    decode_value,
    encode_dead_letter,
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


def test_bytes_that_are_not_text_raise_json_error_too() -> None:
    # json.loads sniffs an encoding from the leading bytes, so a payload starting
    # with a UTF-16 byte-order mark is decoded as UTF-16 and fails as a
    # UnicodeDecodeError rather than a JSONDecodeError. Callers triage on "this
    # did not parse", so the wire contract must expose one failure for both — a
    # message that is not even text is the most corrupt a message can be, and it
    # must not escape as a type nobody is catching.
    with pytest.raises(json.JSONDecodeError):
        decode_value(b"\xff\xfe not text at all \x00\x01")


def test_right_version_wrong_shape_is_a_validation_error() -> None:
    # v says 1 but the event is missing — a producer bug, distinct from version skew.
    with pytest.raises(ValidationError):
        decode_value(json.dumps({"v": 1, "captured_at": "2026-07-14T18:00:00Z"}).encode())


def test_dead_letter_round_trips_reason_and_original_bytes() -> None:
    # The original bytes are exactly what failed to decode, so they may be corrupt
    # or non-UTF-8; base64 must carry them through a JSON round trip losslessly.
    original = b"\xff\xfe not valid json at all"
    record = decode_dead_letter(
        encode_dead_letter(
            reason="corrupt", detail="Expecting value: line 1", value=original, key=b"7"
        )
    )

    assert record.reason == "corrupt"
    assert record.detail == "Expecting value: line 1"
    assert record.value == original  # byte-for-byte, despite the invalid bytes
    assert record.key == b"7"


def test_dead_letter_value_is_self_describing_json() -> None:
    # A DLQ dump (kcat, a triage script) reads the version and reason from the value.
    encoded = encode_dead_letter(reason="invalid_shape", detail="x", value=b"{}", key=b"2")
    raw = json.loads(encoded)
    assert raw["v"] == DLQ_SCHEMA_VERSION
    assert raw["reason"] == "invalid_shape"
    assert isinstance(raw["value"], str)  # base64 text, not raw bytes embedded in JSON


def test_dead_letter_preserves_a_missing_key() -> None:
    # A tombstone has no key; None must round-trip as None, not as empty bytes.
    encoded = encode_dead_letter(reason="corrupt", detail="", value=b"", key=None)
    record = decode_dead_letter(encoded)
    assert record.key is None
    assert record.value == b""


def test_dead_letter_foreign_version_is_refused() -> None:
    foreign = {"v": 2, "reason": "corrupt", "detail": "", "value": "", "key": None}
    with pytest.raises(UnsupportedSchemaVersionError) as excinfo:
        decode_dead_letter(json.dumps(foreign).encode())
    assert excinfo.value.version == 2
