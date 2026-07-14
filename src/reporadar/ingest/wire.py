"""The event wire contract: the bytes that travel in a message.

One module owns what a message's **value** and **key** look like, so the
producer and every consumer import the same definition instead of restating it.

The value is a versioned JSON envelope, ``{"v": 1, "captured_at": ...,
"event": ...}``. ``v`` is checked *before* shape validation so a foreign
version is refused loudly rather than misparsed. ``captured_at`` is ingest time
(the producer's clock) — a distinct field from the event's own ``created_at``
(event time), which makes capture lag measurable per message. The envelope
wraps the event instead of adding sibling fields, so producer metadata can
never collide with a field GitHub adds later.

The key is the repository *id* (stable across renames, unlike the name) as
decimal bytes: partitioned transports order within a partition only, and
keying by repo keeps each repository's events in one partition — per-repo
order preserved for the consumers that aggregate by repo.

Decode failures are deliberately three distinct errors — ``json.JSONDecodeError``
(corrupt bytes), :class:`UnsupportedSchemaVersionError` (not an envelope this
reader speaks), pydantic ``ValidationError`` (right version, wrong shape: a
producer bug) — so dead-letter triage can tell them apart.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from pydantic import AwareDatetime, BaseModel, ConfigDict

from reporadar.github.events import RawEvent

SCHEMA_VERSION = 1


class UnsupportedSchemaVersionError(ValueError):
    """The bytes are JSON, but not an envelope version this reader understands."""

    def __init__(self, version: object) -> None:
        super().__init__(
            f"unsupported wire schema version {version!r}; this reader speaks v{SCHEMA_VERSION}"
        )
        self.version = version


class WireEnvelope(BaseModel):
    """What actually travels in a message value."""

    model_config = ConfigDict(frozen=True)

    v: int
    captured_at: AwareDatetime  # ingest time; event time is event.created_at
    event: RawEvent


def encode_value(event: RawEvent, *, captured_at: datetime | None = None) -> bytes:
    """Serialize one event into a message value (UTF-8 JSON envelope).

    ``captured_at`` defaults to now (UTC); pass it explicitly to stamp a whole
    batch with one capture instant. Naive datetimes are refused.
    """
    stamped = captured_at if captured_at is not None else datetime.now(tz=UTC)
    envelope = WireEnvelope(v=SCHEMA_VERSION, captured_at=stamped, event=event)
    return envelope.model_dump_json().encode("utf-8")


def encode_key(event: RawEvent) -> bytes:
    """The partition key: the repo id as decimal bytes (same repo → same partition)."""
    return str(event.repo.id).encode("ascii")


def decode_value(data: bytes) -> WireEnvelope:
    """Parse and validate a message value, refusing foreign versions loudly."""
    raw: object = json.loads(data)
    version: object = raw.get("v") if isinstance(raw, dict) else None
    if version != SCHEMA_VERSION:
        raise UnsupportedSchemaVersionError(version)
    return WireEnvelope.model_validate(raw)
