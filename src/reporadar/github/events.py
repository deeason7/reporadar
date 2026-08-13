"""Typed envelope for GitHub public events.

Payload schemas differ per event type and evolve over time; the envelope
(id / type / actor / repo / created_at) is stable across all of them, so the
envelope is validated strictly and payloads ride along untyped until a
per-type model earns its keep.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError


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
    # Aware, not plain datetime: astimezone() reads a naive value as the local time of
    # whichever machine holds it, so an unmarked timestamp silently becomes a different
    # instant per deployment — wrong hour file, wrong row, no error. Refusing it here
    # keeps the guess out of the data; the API and the archive both send UTC anyway.
    created_at: AwareDatetime  # event time
    public: bool = True
    payload: dict[str, Any] = Field(default_factory=dict)


def parse_event(item: dict[str, Any] | str | bytes) -> RawEvent:
    if isinstance(item, str | bytes):
        return RawEvent.model_validate_json(item)
    return RawEvent.model_validate(item)


class RejectedItem(BaseModel):
    """A feed item that would not validate, kept whole alongside why.

    The raw item travels, not a summary of it. A rejection that records only a
    reason cannot be replayed, cannot be re-examined when the reason turns out to
    be wrong about itself, and cannot answer the question that actually gets asked
    later — *what did they send us?*
    """

    model_config = ConfigDict(frozen=True)

    raw: dict[str, Any]
    reason: str


def parse_page(body: Iterable[Any]) -> tuple[list[RawEvent], list[RejectedItem]]:
    """Split one page of feed items into what validated and what did not.

    The feed does not promise every field this envelope requires. It serves an
    otherwise well-formed event with ``repo`` blanked when a repository stops
    being publicly visible -- id, type, actor, created_at and the full payload all
    intact. That is redaction, not corruption, and the envelope was asserting a
    guarantee the source does not make.

    Rejecting the page over one such item is the behaviour this replaces: the
    whole sweep raised, which ended an always-on run roughly once a day and a half
    at the measured rate of ~2 per million. Rejecting the *item* keeps the run
    alive and keeps the item, which are different requirements and both real --
    dropping it silently is how completeness lies start.
    """
    events: list[RawEvent] = []
    rejected: list[RejectedItem] = []
    for item in body:
        try:
            events.append(parse_event(item))
        except ValidationError as exc:
            # str(exc) rather than the structured errors: this is read by a person
            # triaging a file, and pydantic's rendering already names each failing
            # field and what it got.
            raw = item if isinstance(item, dict) else {"_unparseable": repr(item)}
            rejected.append(RejectedItem(raw=raw, reason=str(exc)))
    return events, rejected


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
