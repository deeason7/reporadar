"""One event must come back as one event, whatever its payload contains.

NDJSON's whole contract is "one record per line", and the two sides of it
disagree about what a line is. The writer ends records with ``"\\n"``. Python's
``str.splitlines()`` — the obvious way to undo that — also breaks on U+2028 LINE
SEPARATOR, U+2029, U+0085, ``\\v``, ``\\f`` and four more. None of those are
record separators here, and one of them shows up in real data.

U+2028 is legal *unescaped* inside a JSON string: JSON forbids only the control
characters below U+0020, and U+2028 is not one of them. ``json.dumps`` hides that
by escaping non-ASCII, but pydantic's ``model_dump_json`` does not, so the byte
sequence lands raw in the hourly files — and GitHub payloads carry free text
(commit messages, issue titles) written by anyone.

Read that file with ``splitlines()`` and one event becomes two fragments, neither
of which parses. That is worse than a dropped event. ``iter_ndjson`` raises on a
malformed line precisely so callers can dead-letter it rather than drop it
silently — "silent drops are how completeness lies start" — but here the record
that failed never existed, and one lost event is reported as two failures. The
instrument built to measure the damage misreports it.

So: the round trip is pinned against a payload that carries the separator, the
reader the rest of the suite uses splits on ``"\\n"`` alone, and the idiom that
breaks it cannot come back without an entry saying why.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from conftest import read_ndjson
from reporadar.analysis.capture import type_counts
from reporadar.github.events import RawEvent, iter_ndjson
from reporadar.ingest.sinks import HourlyNdjsonSink

REPO = Path(__file__).resolve().parents[1]
SCANNED_ROOTS = (REPO / "src", REPO / "tests")

#: Written as an escape on purpose. Spelled literally it is invisible in an
#: editor, in a diff and in a review — which is the entire reason it survives in
#: production data long enough to reach a parser.
LINE_SEPARATOR = "\u2028"

#: The message an event carries through the round trip below.
MESSAGE = f"fix the parser{LINE_SEPARATOR}and the reader"

#: Files allowed to call ``splitlines()``, with the reason each is exempt. Adding
#: a name here is a deliberate act that costs a sentence, which is the point —
#: and `test_every_exemption_is_still_earned` drops the ones that stop being true.
SPLITLINES_IS_ALLOWED: dict[str, str] = {
    "test_ndjson_lines.py": "the control below calls it deliberately, to show what it costs",
}


def _event(event_id: str, message: str) -> RawEvent:
    return RawEvent.model_validate(
        {
            "id": event_id,
            "type": "PushEvent",
            "actor": {"id": 1, "login": "octo-tester"},
            "repo": {"id": 2, "name": "octo/widgets"},
            "created_at": "2026-07-07T15:00:00Z",
            "payload": {"message": message},
        }
    )


async def _written_hour(tmp_path: Path) -> Path:
    """Three events through the real sink, the middle one carrying the separator."""
    sink = HourlyNdjsonSink(tmp_path)
    await sink([_event("1", "plain"), _event("2", MESSAGE), _event("3", "also plain")])
    return sink.path_for("2026-07-07-15")


def splitlines_calls_in(source: str) -> list[int]:
    """Line numbers of every ``.splitlines()`` call in one module.

    Parsed, not grepped: this module discusses ``splitlines()`` at length in prose
    and names it in a docstring above, so a text search would report the very file
    that defines the rule and nothing else would ever be checked.
    """
    return [
        node.lineno
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "splitlines"
    ]


async def test_a_line_separator_in_a_payload_survives_the_round_trip(tmp_path: Path) -> None:
    path = await _written_hour(tmp_path)

    events = read_ndjson(path)

    assert [event.id for event in events] == ["1", "2", "3"]
    # Not just "three events arrived" — the character itself has to still be there.
    # A reader that stripped it would pass the count and corrupt the payload.
    assert events[1].payload["message"] == MESSAGE


async def test_the_writer_still_writes_the_separator_raw(tmp_path: Path) -> None:
    # The anti-vacuity guard. Everything above is only a test while the hazard is
    # real: if pydantic ever starts escaping non-ASCII, the round trip passes
    # because there is nothing left to survive, and this file would go on claiming
    # to defend against something that had quietly moved. Then this fails, and the
    # failure is the news.
    path = await _written_hour(tmp_path)

    text = path.read_text(encoding="utf-8")

    assert LINE_SEPARATOR in text, "model_dump_json no longer emits U+2028 raw; re-read this file"
    assert text.count("\n") == 3  # three records, three record separators


async def test_splitlines_turns_one_event_into_two_failures(tmp_path: Path) -> None:
    # The negative control, and the reason the helper exists. Without this the rule
    # above is a preference; with it, the cost of breaking it is on the record.
    path = await _written_hour(tmp_path)
    text = path.read_text(encoding="utf-8")

    assert len(text.split("\n")) - 1 == 3  # trailing newline leaves one empty tail
    assert len(text.splitlines()) == 4  # the middle event has been cut in half

    with pytest.raises(ValueError):  # pydantic's ValidationError is a ValueError
        list(iter_ndjson(text.splitlines()))


async def test_the_production_reader_is_unaffected(tmp_path: Path) -> None:
    # DuckDB scans for the newline byte and nothing else, so the capture-rate
    # queries never had this problem. Worth pinning rather than assuming: it is the
    # difference between "one reader disagrees with another" and "the numbers the
    # project publishes are wrong", and those call for different responses.
    path = await _written_hour(tmp_path)

    assert type_counts(path) == [("PushEvent", 3)]


def test_no_module_reads_records_with_splitlines() -> None:
    scanned = 0
    offenders: list[str] = []
    for root in SCANNED_ROOTS:
        for path in sorted(root.rglob("*.py")):
            scanned += 1
            if path.name in SPLITLINES_IS_ALLOWED:
                continue
            offenders += [
                f"{path.relative_to(REPO)}:{line}"
                for line in splitlines_calls_in(path.read_text(encoding="utf-8"))
            ]

    # A sweep that reaches nothing reports nothing wrong, which is indistinguishable
    # from a clean repository until the day it matters.
    assert scanned > 20, f"the sweep only reached {scanned} modules; the roots have moved"
    assert not offenders, (
        "splitlines() splits on U+2028 and seven other characters that are not record "
        f"separators; read through conftest.read_ndjson instead: {offenders}"
    )


def test_the_sweep_reports_a_planted_call() -> None:
    assert splitlines_calls_in("events = parse(path.read_text().splitlines())") == [1]


def test_the_sweep_ignores_the_name_in_prose() -> None:
    # It is spelled out in comments and docstrings all over this file. A checker
    # that fired on those would be turned off within the week.
    assert (
        splitlines_calls_in('"""Never call splitlines() here."""\n# splitlines() is wrong\n') == []
    )


def test_every_exemption_is_still_earned() -> None:
    # An allowlist entry for a file that no longer calls splitlines() is stale, and
    # a stale entry is a hole waiting for the next real call to fall into.
    for name, reason in SPLITLINES_IS_ALLOWED.items():
        found = [path for root in SCANNED_ROOTS for path in root.rglob(name)]
        assert found, f"{name} is exempted but no longer exists; drop the entry"
        assert splitlines_calls_in(found[0].read_text(encoding="utf-8")), (
            f"{name} no longer calls splitlines(), so the exemption is stale: {reason}"
        )
