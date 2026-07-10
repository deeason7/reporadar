"""Typed envelope for GitHub public events.

Payload schemas differ per event type and evolve over time; the envelope
(id / type / actor / repo / created_at) is stable across all of them, so the
envelope is validated strictly and payloads ride along untyped until a
per-type model earns its keep.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class Actor(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    login: str


class Repo(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: int
    name: str  # "owner/repo"


class RawEvent(BaseModel):
    """One public event, as served by both /events and GH Archive (2015+ format)."""

    model_config = ConfigDict(frozen=True)

    id: str
    type: str
    actor: Actor
    repo: Repo
    created_at: datetime  # event time, always timezone-aware (UTC from the API)
    public: bool = True
    payload: dict[str, Any] = Field(default_factory=dict)


def parse_event(item: dict[str, Any] | str | bytes) -> RawEvent:
    if isinstance(item, str | bytes):
        return RawEvent.model_validate_json(item)
    return RawEvent.model_validate(item)


def iter_ndjson(lines: Iterable[str]) -> Iterator[RawEvent]:
    """Parse NDJSON lines, skipping blanks.

    Malformed lines raise: callers route those to a dead-letter path — silent
    drops are how completeness lies start.
    """
    for line in lines:
        stripped = line.strip()
        if stripped:
            yield RawEvent.model_validate_json(stripped)


def dedupe(events: Iterable[RawEvent]) -> list[RawEvent]:
    """Order-preserving dedupe by event id (pages overlap between poll sweeps)."""
    seen: set[str] = set()
    out: list[RawEvent] = []
    for event in events:
        if event.id not in seen:
            seen.add(event.id)
            out.append(event)
    return out
