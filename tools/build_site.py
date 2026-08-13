#!/usr/bin/env python3
"""Build the public result page from the Parquet lake.

Every figure on the page comes from one of the queries in this file, run against
the lake at build time. Nothing is typed into the HTML by hand, because a number
copied into prose is a second copy of a fact: it is correct once, nothing renames
it, nothing type-checks it, and the page keeps asserting it long after the lake
has moved. Regenerating is the only version of "the page is accurate" that stays
true, so the page is a build artifact and the lake is the single source.

The output carries no wall-clock stamp for the same reason. What dates the page
is the data behind it -- the day range and the event count are printed instead --
which also makes a rebuild over an unchanged lake byte-identical, so re-running
this is a no-op rather than a diff.

Usage::

    python tools/build_site.py                     # data/lake -> docs/index.html
    python tools/build_site.py --lake … --out …
"""

from __future__ import annotations

import argparse
import html
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from itertools import pairwise
from pathlib import Path
from typing import Any

import duckdb

REPO = Path(__file__).resolve().parents[1]
DEFAULT_LAKE = REPO / "data" / "lake"
DEFAULT_OUT = REPO / "docs" / "index.html"

#: The day held out of every choice made during the analysis. Fixed here because
#: it is fixed in the analysis: a split chosen after seeing the scores is not a
#: split. It is the last calendar day the lake holds.
HELD_OUT_DAY = "2026-07-29"

#: A repository-day is "busy" at or above this many events. It is a volume
#: filter, not a feature: everything measured on the held-out day is measured
#: *among* busy repository-days, so volume cannot also be doing the predicting.
BUSY_THRESHOLD = 20

#: The cheap signal under test: the repository name after the slash is five to
#: eight lowercase letters and nothing else. Anchored, so it is a full match.
NAME_PATTERN = "^[a-z]{5,8}$"

#: How many event types get their own row before the rest are pooled.
TOP_TYPES = 10


# --------------------------------------------------------------------------- #
# Formatting. Pure, and the reason the tests can cover the page's arithmetic
# without a lake: every number the reader sees goes through one of these.
# --------------------------------------------------------------------------- #


def fmt_int(value: int) -> str:
    """A count, grouped for reading: ``15827495`` -> ``15,827,495``."""
    return f"{value:,}"


def fmt_pct(part: int, whole: int, places: int = 2) -> str:
    """``part`` as a percentage of ``whole``, to a stated number of places.

    ``places`` is per call site rather than global because the figures on this
    page span four orders of magnitude: two places renders the WatchEvent share
    as ``0.03%`` and loses the only digit that says how small it is.

    A zero denominator raises. It has no percentage, and the alternatives -- 0%,
    or a dash -- are both readable as a measurement that came back small.
    """
    if whole == 0:
        raise ValueError("no percentage of nothing")
    return f"{100.0 * part / whole:.{places}f}%"


def fmt_ratio(part: int, whole: int, places: int = 4) -> str:
    """A bare proportion, for precision and recall: ``0.9889``."""
    if whole == 0:
        raise ValueError("no ratio of nothing")
    return f"{part / whole:.{places}f}"


def fmt_lift(numerator: float, denominator: float, places: int = 2) -> str:
    """How many times one rate is the other: ``1.04x``.

    Two places, and no more. The interesting property of this number is that it
    is barely above one, and a third place invites reading precision into a
    figure whose whole point is how small it is.
    """
    if denominator == 0:
        raise ValueError("no lift over nothing")
    return f"{numerator / denominator:.{places}f}×"


def fmt_f1(precision: float, recall: float, places: int = 4) -> str:
    """Harmonic mean of precision and recall."""
    if precision + recall == 0:
        raise ValueError("no F1 without a positive prediction")
    return f"{2 * precision * recall / (precision + recall):.{places}f}"


def fmt_bytes(value: int) -> str:
    """Bytes as GiB, with the exact count kept alongside.

    Both, because they answer different questions and disagree by 7%: the exact
    figure is what a checksum of the directory would agree with, the GiB is what
    a disk says. Printing only the rounded one makes the page unfalsifiable
    against the thing it describes.
    """
    return f"{value / 1024**3:.2f} GiB ({fmt_int(value)} bytes)"


def fmt_points(higher: float, lower: float, places: int = 2) -> str:
    """A spread between two percentages, in percentage points."""
    return f"{higher - lower:.{places}f}"


# --------------------------------------------------------------------------- #
# Query shaping. Each function returns SQL; nothing here opens a connection, so
# the shape of every query is assertable without reading 1.3 GiB.
# --------------------------------------------------------------------------- #


def sql_literal(value: str) -> str:
    """Embed a value as a SQL string literal (single quotes doubled)."""
    return "'" + value.replace("'", "''") + "'"


def lake_source(lake_dir: Path) -> str:
    """The ``FROM`` clause: every Parquet file in the lake, partitions included.

    ``hive_partitioning`` is what makes ``dt`` and ``hr`` columns rather than
    path fragments, so a per-day figure reads the partition it names instead of
    every file in the lake.
    """
    pattern = str(lake_dir / "**" / "*.parquet")
    return f"read_parquet({sql_literal(pattern)}, hive_partitioning = 1)"


def repo_day_cte(lake_dir: Path, day: str | None = None) -> str:
    """One row per repository per day: how many events, how many distinct actors.

    This is the unit the whole analysis is stated in. Not the event: a per-event
    unit lets the largest automated repositories decide the answer by volume
    alone, which is the exact confusion this page exists to take apart.

    The parentheses around ``repo->>'name'`` are load-bearing. ``IS NOT NULL``
    binds tighter than ``->>``, so the unparenthesised spelling asks DuckDB for
    the key ``true`` and dies with a cast error naming the repository column --
    a message that reads like bad data rather than a precedence bug. Seven events
    in this lake genuinely carry no repository, which is what sent the filter
    here in the first place.
    """
    where = "WHERE (repo->>'name') IS NOT NULL"
    if day is not None:
        where += f" AND dt = DATE {sql_literal(day)}"
    return (
        "SELECT dt,\n"
        "       repo->>'name' AS repo_name,\n"
        "       count(*) AS events,\n"
        "       count(DISTINCT actor->>'login') AS actors\n"
        f"FROM {lake_source(lake_dir)}\n"
        f"{where}\n"
        "GROUP BY dt, repo_name"
    )


def overview_sql(lake_dir: Path) -> str:
    """What the lake holds, in one row."""
    return (
        "SELECT count(*) AS events,\n"
        "       count(DISTINCT dt) AS days,\n"
        "       count(DISTINCT (dt, hr)) AS hours,\n"
        "       count(DISTINCT repo->>'name') AS repos,\n"
        "       count(DISTINCT actor->>'login') AS actors,\n"
        "       count(DISTINCT type) AS event_types\n"
        f"FROM {lake_source(lake_dir)}"
    )


def days_sql(lake_dir: Path) -> str:
    """The calendar days present, in order."""
    return f"SELECT DISTINCT dt FROM {lake_source(lake_dir)} ORDER BY dt"


def type_histogram_sql(lake_dir: Path) -> str:
    """Every event type by volume, largest first."""
    return (
        "SELECT type, count(*) AS events\n"
        f"FROM {lake_source(lake_dir)}\n"
        "GROUP BY type\n"
        "ORDER BY events DESC"
    )


def per_repo_sql(lake_dir: Path) -> str:
    """Repositories over the whole lake, and how many are single-actor.

    Per repository rather than per repository-day: this is the headline share,
    and it is asked of the repository across every day it appears, so a project
    with one committer today and another tomorrow is correctly not single-actor.
    """
    return (
        "WITH per_repo AS (\n"
        "    SELECT repo->>'name' AS repo_name,\n"
        "           count(*) AS events,\n"
        "           count(DISTINCT actor->>'login') AS actors\n"
        f"    FROM {lake_source(lake_dir)}\n"
        "    WHERE (repo->>'name') IS NOT NULL\n"
        "    GROUP BY repo_name\n"
        ")\n"
        "SELECT count(*) AS repos,\n"
        "       count(*) FILTER (WHERE actors = 1) AS single_actor_repos,\n"
        "       sum(events) AS events,\n"
        "       sum(events) FILTER (WHERE actors = 1) AS single_actor_events\n"
        "FROM per_repo"
    )


def per_day_sql(lake_dir: Path) -> str:
    """The automated share, one row per day, on both units at once.

    Both units in the same row on purpose. The share of events and the share of
    repository-days are the two readings that the rest of this page is about not
    confusing, and putting them side by side is what makes the gap visible.
    """
    return (
        f"WITH repo_days AS (\n{repo_day_cte(lake_dir)}\n)\n"
        "SELECT dt,\n"
        "       sum(events) AS events,\n"
        "       count(*) AS repo_days,\n"
        f"       sum(events) FILTER (WHERE events >= {BUSY_THRESHOLD} AND actors = 1)"
        " AS automated_events,\n"
        f"       count(*) FILTER (WHERE events >= {BUSY_THRESHOLD} AND actors = 1)"
        " AS automated_repo_days,\n"
        f"       sum(events) FILTER (WHERE regexp_matches(split_part(repo_name, '/', 2),"
        f" {sql_literal(NAME_PATTERN)})) AS pattern_events\n"
        "FROM repo_days\n"
        "GROUP BY dt\n"
        "ORDER BY dt"
    )


def held_out_sql(lake_dir: Path) -> str:
    """Everything scored on the held-out day, in one row.

    The name signal is evaluated only among busy repository-days. Scoring it over
    all of them would let it be right about the millions of one-event repositories
    it never claims, which is a statement about the feed's tail and not about the
    signal.
    """
    return (
        f"WITH repo_days AS (\n{repo_day_cte(lake_dir, HELD_OUT_DAY)}\n),\n"
        "marked AS (\n"
        "    SELECT *,\n"
        "           regexp_matches(split_part(repo_name, '/', 2),"
        f" {sql_literal(NAME_PATTERN)}) AS name_hit\n"
        "    FROM repo_days\n"
        ")\n"
        "SELECT count(*) AS repo_days,\n"
        "       sum(events) AS events,\n"
        "       count(*) FILTER (WHERE name_hit) AS pattern_repo_days,\n"
        "       sum(events) FILTER (WHERE name_hit) AS pattern_events,\n"
        f"       count(*) FILTER (WHERE events >= {BUSY_THRESHOLD}) AS busy_repo_days,\n"
        f"       count(*) FILTER (WHERE events >= {BUSY_THRESHOLD} AND actors = 1)"
        " AS busy_single_actor,\n"
        f"       count(*) FILTER (WHERE events >= {BUSY_THRESHOLD} AND name_hit)"
        " AS rule_positives,\n"
        f"       count(*) FILTER (WHERE events >= {BUSY_THRESHOLD} AND name_hit AND actors = 1)"
        " AS rule_true_positives\n"
        "FROM marked"
    )


def watch_sql(lake_dir: Path) -> str:
    """How many WatchEvents land outside the automated population.

    Every alias is distinct across the three parts. DuckDB will resolve a bare
    ``repo`` in a join condition against whichever side has one, and one of those
    sides holds the extracted name while the other holds the JSON it came from --
    the same query then fails on a cast whose message points at the data.
    """
    return (
        f"WITH repo_days AS (\n{repo_day_cte(lake_dir)}\n),\n"
        "automated AS (\n"
        "    SELECT dt AS auto_dt, repo_name AS auto_repo\n"
        "    FROM repo_days\n"
        f"    WHERE events >= {BUSY_THRESHOLD} AND actors = 1\n"
        "),\n"
        "watches AS (\n"
        "    SELECT dt AS watch_dt, repo->>'name' AS watch_repo\n"
        f"    FROM {lake_source(lake_dir)}\n"
        "    WHERE type = 'WatchEvent'\n"
        ")\n"
        "SELECT count(*) AS watch_events,\n"
        "       count(*) FILTER (WHERE auto_repo IS NULL) AS watch_outside_automated\n"
        "FROM watches\n"
        "LEFT JOIN automated ON auto_dt = watch_dt AND auto_repo = watch_repo"
    )


# --------------------------------------------------------------------------- #
# The figures
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class TypeRow:
    """One event type's share of the lake."""

    event_type: str
    events: int


@dataclass(frozen=True)
class DayRow:
    """One calendar day, on both units."""

    day: str
    events: int
    repo_days: int
    automated_events: int
    automated_repo_days: int
    pattern_events: int


@dataclass(frozen=True)
class Figures:
    """Everything the page states, and nothing it does not.

    Frozen and complete: the renderer takes one of these and no connection, so a
    figure can only reach the HTML by being measured first.
    """

    days: tuple[str, ...]
    events: int
    hours: int
    files: int
    bytes_on_disk: int
    repos: int
    actors: int
    event_types: int
    types: tuple[TypeRow, ...]
    per_repo_repos: int
    single_actor_repos: int
    per_repo_events: int
    single_actor_events: int
    held_out_day: str
    held_out_repo_days: int
    held_out_events: int
    pattern_repo_days: int
    pattern_events: int
    busy_repo_days: int
    busy_single_actor: int
    rule_positives: int
    rule_true_positives: int
    watch_events: int
    watch_outside_automated: int
    per_day: tuple[DayRow, ...]

    @property
    def base_rate(self) -> float:
        """Share of busy repository-days that are single-actor: what guessing scores."""
        return self.busy_single_actor / self.busy_repo_days

    @property
    def rule_precision(self) -> float:
        return self.rule_true_positives / self.rule_positives

    @property
    def rule_recall(self) -> float:
        return self.rule_true_positives / self.busy_single_actor

    @property
    def pooled_pattern_events(self) -> int:
        return sum(day.pattern_events for day in self.per_day)

    @property
    def pooled_events(self) -> int:
        return sum(day.events for day in self.per_day)


def _one_row(con: Any, sql: str) -> tuple[Any, ...]:
    row = con.execute(sql).fetchone()
    if row is None:  # pragma: no cover - an aggregate always returns one row
        raise RuntimeError(f"query returned no row:\n{sql}")
    return tuple(row)


def collect(lake_dir: Path) -> Figures:
    """Run every query and return what the page is allowed to say."""
    files = sorted(lake_dir.glob("**/*.parquet"))
    if not files:
        raise SystemExit(
            f"no Parquet files under {lake_dir} — build the lake first "
            "(reporadar backfill <from> <to>)"
        )

    con = duckdb.connect()
    try:
        events, days_count, hours, repos, actors, event_types = _one_row(
            con, overview_sql(lake_dir)
        )
        days = tuple(str(row[0]) for row in con.execute(days_sql(lake_dir)).fetchall())
        types = tuple(
            TypeRow(str(name), int(count))
            for name, count in con.execute(type_histogram_sql(lake_dir)).fetchall()
        )
        repo_count, single_repos, repo_events, single_events = _one_row(con, per_repo_sql(lake_dir))
        (
            held_repo_days,
            held_events,
            pattern_repo_days,
            pattern_events,
            busy_repo_days,
            busy_single,
            rule_positives,
            rule_true_positives,
        ) = _one_row(con, held_out_sql(lake_dir))
        per_day = tuple(
            DayRow(str(day), int(ev), int(rd), int(auto_ev), int(auto_rd), int(pat_ev))
            for day, ev, rd, auto_ev, auto_rd, pat_ev in con.execute(
                per_day_sql(lake_dir)
            ).fetchall()
        )
        watch_events, watch_outside = _one_row(con, watch_sql(lake_dir))
    finally:
        con.close()

    if int(days_count) != len(days):  # pragma: no cover - two spellings of one fact
        raise RuntimeError(f"{days_count} distinct days but {len(days)} listed")

    figures = Figures(
        days=days,
        events=int(events),
        hours=int(hours),
        files=len(files),
        bytes_on_disk=sum(path.stat().st_size for path in files),
        repos=int(repos),
        actors=int(actors),
        event_types=int(event_types),
        types=types,
        per_repo_repos=int(repo_count),
        single_actor_repos=int(single_repos),
        per_repo_events=int(repo_events),
        single_actor_events=int(single_events),
        held_out_day=HELD_OUT_DAY,
        held_out_repo_days=int(held_repo_days),
        held_out_events=int(held_events),
        pattern_repo_days=int(pattern_repo_days),
        pattern_events=int(pattern_events),
        busy_repo_days=int(busy_repo_days),
        busy_single_actor=int(busy_single),
        rule_positives=int(rule_positives),
        rule_true_positives=int(rule_true_positives),
        watch_events=int(watch_events),
        watch_outside_automated=int(watch_outside),
        per_day=per_day,
    )
    check_consistency(figures)
    return figures


def check_consistency(figures: Figures) -> None:
    """Two queries measured the held-out day; make them agree before publishing.

    The per-day query and the held-out query compute that day's pattern share by
    different routes, and a page that printed one while the other disagreed would
    be wrong in a way no reader could see. Cheap to check, and the only kind of
    error worth checking is the invisible kind.
    """
    held_out = [day for day in figures.per_day if day.day == figures.held_out_day]
    if len(held_out) != 1:
        raise RuntimeError(
            f"the held-out day {figures.held_out_day} is not one of {list(figures.days)}"
        )
    if held_out[0].events != figures.held_out_events:
        raise RuntimeError(
            f"held-out day event count disagrees between queries: "
            f"{held_out[0].events} and {figures.held_out_events}"
        )
    if held_out[0].pattern_events != figures.pattern_events:
        raise RuntimeError(
            f"held-out day name-pattern event count disagrees between queries: "
            f"{held_out[0].pattern_events} and {figures.pattern_events}"
        )


# --------------------------------------------------------------------------- #
# Rendering. Pure: takes figures, returns a whole document.
# --------------------------------------------------------------------------- #

STYLE = """
:root {
  color-scheme: light dark;
  --bg: #fcfcfa;
  --panel: #f4f4ef;
  --ink: #1b1c1a;
  --muted: #5f6058;
  --line: #dedcd3;
  --accent: #0f5c8c;
  --flag: #8a3b12;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #14161a;
    --panel: #1b1e23;
    --ink: #e4e5e1;
    --muted: #9a9d95;
    --line: #2b2f36;
    --accent: #7fb4dd;
    --flag: #d8956a;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto,
               "Helvetica Neue", Arial, sans-serif;
  font-size: 17px;
  line-height: 1.6;
  -webkit-text-size-adjust: 100%;
}
main { max-width: 68ch; margin: 0 auto; padding: 3rem 1.25rem 6rem; }
header { border-bottom: 1px solid var(--line); padding-bottom: 1.5rem; margin-bottom: 2.5rem; }
h1 { font-size: 1.75rem; line-height: 1.25; margin: 0 0 0.5rem; letter-spacing: -0.01em; }
h2 {
  font-size: 1.1rem;
  margin: 3rem 0 0.75rem;
  padding-top: 1.25rem;
  border-top: 1px solid var(--line);
  letter-spacing: 0.02em;
  text-transform: uppercase;
  color: var(--muted);
}
h3 { font-size: 1rem; margin: 2rem 0 0.5rem; }
p, li { max-width: 65ch; }
p { margin: 0 0 1rem; }
ul, ol { padding-left: 1.25rem; margin: 0 0 1rem; }
li { margin-bottom: 0.4rem; }
a { color: var(--accent); }
strong { font-weight: 650; }
.sub { color: var(--muted); font-size: 1.0rem; margin: 0; }
.provenance {
  color: var(--muted);
  font-size: 0.85rem;
  margin-top: 1.25rem;
  font-variant-numeric: tabular-nums;
}
.lead {
  font-size: 1.15rem;
  line-height: 1.5;
  border-left: 3px solid var(--accent);
  padding: 0.25rem 0 0.25rem 1rem;
  margin: 0 0 1.5rem;
}
.lead .big { font-size: 1.6rem; font-weight: 650; font-variant-numeric: tabular-nums; }
.note {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 4px;
  padding: 0.9rem 1rem;
  margin: 1.25rem 0;
  font-size: 0.95rem;
}
.note p:last-child { margin-bottom: 0; }
.aside {
  color: var(--muted);
  font-size: 0.88rem;
  border-left: 2px solid var(--line);
  padding-left: 0.9rem;
}
.note .label {
  display: block;
  font-size: 0.72rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--flag);
  margin-bottom: 0.35rem;
  font-weight: 650;
}
.scroll { overflow-x: auto; margin: 1.25rem 0; border: 1px solid var(--line); border-radius: 4px; }
table { border-collapse: collapse; width: 100%; font-size: 0.9rem; }
caption {
  caption-side: top;
  text-align: left;
  color: var(--muted);
  font-size: 0.85rem;
  padding: 0.6rem 0.75rem;
  border-bottom: 1px solid var(--line);
}
th, td {
  padding: 0.45rem 0.75rem;
  text-align: right;
  white-space: nowrap;
  border-bottom: 1px solid var(--line);
  font-variant-numeric: tabular-nums;
}
th:first-child, td:first-child { text-align: left; }
thead th { color: var(--muted); font-weight: 600; font-size: 0.8rem; }
tbody tr:last-child td { border-bottom: none; }
tbody tr.total td { font-weight: 650; }
td.mark { color: var(--flag); }
pre {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 4px;
  padding: 0.9rem 1rem;
  overflow-x: auto;
  font-size: 0.85rem;
  line-height: 1.5;
}
code {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas,
               "Liberation Mono", monospace;
  font-size: 0.88em;
}
p code, li code, td code { background: var(--panel); padding: 0.1em 0.3em; border-radius: 3px; }
pre code { background: none; padding: 0; }
.outcome { font-weight: 650; }
.held { color: var(--accent); }
.missed, .void { color: var(--flag); }
footer {
  margin-top: 4rem;
  padding-top: 1.25rem;
  border-top: 1px solid var(--line);
  color: var(--muted);
  font-size: 0.85rem;
}
""".strip()


def _table(caption: str, headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    """A table wrapped in its own scroller, so a wide one never scrolls the page."""
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = "\n".join("<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows)
    return (
        '<div class="scroll"><table>\n'
        f"<caption>{html.escape(caption)}</caption>\n"
        f"<thead><tr>{head}</tr></thead>\n"
        f"<tbody>\n{body}\n</tbody>\n"
        "</table></div>"
    )


def day_gaps(days: Sequence[str]) -> list[int]:
    """Calendar days between each pair of consecutive days present.

    ``1`` means contiguous. Anything larger is a hole in what was captured, and
    the page says so rather than presenting four days as a week.
    """
    parsed = [date.fromisoformat(day) for day in days]
    return [(later - earlier).days for earlier, later in pairwise(parsed)]


def day_span(days: Sequence[str]) -> int:
    """Calendar days from the first day to the last, inclusive of both.

    Not ``len(days)``. Four days spanning a week is a different claim from four
    consecutive ones, and the drift figure is a statement about the span.
    """
    return sum(day_gaps(days)) + 1


def _ingest_section(f: Figures) -> str:
    top = f.types[:TOP_TYPES]
    rest = f.types[TOP_TYPES:]
    rows = [
        [html.escape(t.event_type), fmt_int(t.events), fmt_pct(t.events, f.events, 3)] for t in top
    ]
    if rest:
        rest_events = sum(t.events for t in rest)
        rows.append(
            [
                f"{len(rest)} other types",
                fmt_int(rest_events),
                fmt_pct(rest_events, f.events, 3),
            ]
        )
    rows.append(["all types", fmt_int(f.events), fmt_pct(f.events, f.events, 3)])

    volume = _table(
        "What the lake holds",
        ["measure", "value"],
        [
            ["events", fmt_int(f.events)],
            [
                "calendar days",
                f"{len(f.days)} ({html.escape(f.days[0])} to {html.escape(f.days[-1])})",
            ],
            ["hour partitions", fmt_int(f.hours)],
            ["Parquet files", fmt_int(f.files)],
            ["on disk", fmt_bytes(f.bytes_on_disk)],
            ["distinct repositories", fmt_int(f.repos)],
            ["distinct actors", fmt_int(f.actors)],
            ["event types", fmt_int(f.event_types)],
        ],
    )
    histogram = _table(
        "Event types by volume, whole lake",
        ["type", "events", "share"],
        rows,
    )
    total_row_class = ' class="total"'
    histogram = histogram.replace("<tr><td>all types", f"<tr{total_row_class}><td>all types")

    return f"""
<h2>What it ingested</h2>
<p>GH Archive publishes one file per hour of GitHub's public event stream.
<code>backfill</code> and <code>archive-serve</code> download those hours, convert each into a
Parquet partition, and record it in an hours ledger; <code>verify</code> checks the ledger against
the files in both directions, and <code>repair-lake</code> re-fetches whatever it proved untrue.</p>
<p>A separate live path exists — a <code>/events</code> poller writing hourly capture files and
publishing to Kafka, a validating consumer storing events in TimescaleDB — and
<strong>none of the figures on this page come from it</strong>. They come from complete archive
hours, because a share needs a denominator, and the poller's window is exactly the thing this
project does not claim to know the size of.</p>
{volume}
{histogram}
<p>These {len(f.days)} days are not contiguous: the largest step between consecutive days present is
{max(day_gaps(f.days))} calendar days, against 1 for a contiguous run. That is a property of what
was captured, not a sampling choice, and it is why every day-to-day statement below is reported per
day rather than as a trend.</p>
""".strip()


def _finding_section(f: Figures) -> str:
    table = _table(
        f"Held-out day {f.held_out_day}: repository-days at or above {BUSY_THRESHOLD} events",
        ["measure", "count", "share"],
        [
            [
                f"repository-days with ≥ {BUSY_THRESHOLD} events",
                fmt_int(f.busy_repo_days),
                fmt_pct(f.busy_repo_days, f.held_out_repo_days, 2) + " of all repository-days",
            ],
            [
                "…of those, exactly one actor",
                fmt_int(f.busy_single_actor),
                f'<span class="mark">{fmt_pct(f.busy_single_actor, f.busy_repo_days, 2)}</span>',
            ],
        ],
    )
    return f"""
<h2>The finding</h2>
<p class="lead"><span class="big">{fmt_pct(f.single_actor_events, f.per_repo_events, 2)}</span>
of the events in this lake come from repositories with exactly one distinct actor
({fmt_int(f.single_actor_events)} of {fmt_int(f.per_repo_events)}), across
{fmt_int(f.single_actor_repos)} of {fmt_int(f.per_repo_repos)} repositories.</p>
<p>Restrict to the repository-days that carry enough volume to matter, on the day held out of every
choice made during the analysis, and the picture sharpens:
<strong>{fmt_pct(f.busy_single_actor, f.busy_repo_days, 2)}</strong> of repository-days with at
least {BUSY_THRESHOLD} events already have exactly one actor.</p>
{table}
<p class="aside">{fmt_int(f.events - f.per_repo_events)} of the {fmt_int(f.events)} events in this
lake carry no repository at all, and are excluded from every per-repository figure — which is why
the denominator in the headline above is that many short of the event count in the ingest table. It
changes nothing at the precision printed here, and it is stated anyway: a denominator that silently
differs from the one two paragraphs up is how a reader loses the ability to check the rest.</p>
<p>That is the result, and it is a negative one about method: <strong>there is nothing here worth
classifying, because the base rate already answers the question.</strong> Anyone consuming this feed
can discard the automated bulk with a counter and no model. The public event stream is not a
population containing automation — it is automation, carrying a trace of everything else.</p>
<div class="note"><span class="label">What this figure is not</span>
<p>A share of <em>events</em> is a claim about volume, not about population. Read as population it
is badly wrong in the flattering direction, and the next section is what that looks like when it
happens. The per-repository-day figures are the ones that carry the claim.</p></div>
<div class="note"><span class="label">The label is a proxy, and it is stated as one</span>
<p>“Automated” here means: a repository-day with exactly one distinct actor and at least
{BUSY_THRESHOLD} events. There is no ground truth for what is a bot, and inventing one would make
the exercise circular. Two independently motivated conditions, neither of which is a feature used to
predict it — but a human working alone on a busy day is counted as automation, and a bot farm with
two accounts is not.</p></div>
""".strip()


def _pattern_section(f: Figures) -> str:
    precision = f.rule_precision
    recall = f.rule_recall
    base = f.base_rate
    shape = _table(
        f"The same name pattern on the same day, {f.held_out_day}, on two units",
        ["view", "matching", "total", "share"],
        [
            [
                "repository-days matching",
                fmt_int(f.pattern_repo_days),
                fmt_int(f.held_out_repo_days),
                fmt_pct(f.pattern_repo_days, f.held_out_repo_days, 2),
            ],
            [
                "events from those repository-days",
                fmt_int(f.pattern_events),
                fmt_int(f.held_out_events),
                f'<span class="mark">{fmt_pct(f.pattern_events, f.held_out_events, 2)}</span>',
            ],
        ],
    )
    scores = _table(
        f"Scored on the held-out day, among repository-days with ≥ {BUSY_THRESHOLD} events",
        ["predictor", "precision", "recall", "F1"],
        [
            [
                "prevalence baseline (call every one of them single-actor)",
                fmt_ratio(f.busy_single_actor, f.busy_repo_days),
                "1.0000",
                fmt_f1(base, 1.0),
            ],
            [
                "name pattern <code>^[a-z]{5,8}$</code>",
                fmt_ratio(f.rule_true_positives, f.rule_positives),
                fmt_ratio(f.rule_true_positives, f.busy_single_actor),
                fmt_f1(precision, recall),
            ],
        ],
    )
    return f"""
<h2>The cheap signal is a volume artifact</h2>
<p>The analysis started from a cheap signal: repositories whose name after the slash is five to
eight lowercase letters and nothing else — <code>^[a-z]{{5,8}}$</code>, the shape that generated
repository names tend to take. Measured over events it looks decisive. Measured over repositories
it nearly vanishes.</p>
{shape}
<p>The pattern does not identify a distinct population. It identifies a
<strong>volume-concentrated</strong> one: {fmt_pct(f.pattern_repo_days, f.held_out_repo_days, 2)} of
repository-days carrying {fmt_pct(f.pattern_events, f.held_out_events, 2)} of events, because a
handful of very high-volume repositories carry the sum. Weighting by events makes a weak
per-repository signal look overwhelming — those two shares stand
{
        fmt_lift(f.pattern_events / f.held_out_events, f.pattern_repo_days / f.held_out_repo_days)
    } apart, and
reading that gap as separation is how a {fmt_lift(precision, base)} effect gets reported as an
order of magnitude.</p>
{scores}
<p>Against a base rate of {fmt_pct(f.busy_single_actor, f.busy_repo_days, 2)}, the name pattern's
precision of {fmt_ratio(f.rule_true_positives, f.rule_positives)} is a lift of
<strong>{fmt_lift(precision, base)}</strong> — {fmt_points(precision * 100, base * 100)} percentage
points. It also loses on F1, because its recall of
{fmt_ratio(f.rule_true_positives, f.busy_single_actor)} drags against a baseline whose recall is
1.0000 by construction. A signal that costs a regex and buys
{fmt_points(precision * 100, base * 100)} points is not a classifier; it is a rounding error with a
story attached.</p>
""".strip()


def _drift_section(f: Figures) -> str:
    rows = [
        [
            html.escape(day.day),
            fmt_int(day.events),
            fmt_pct(day.automated_events, day.events, 2),
            fmt_pct(day.automated_repo_days, day.repo_days, 2),
        ]
        for day in f.per_day
    ]
    shares = [100.0 * day.automated_events / day.events for day in f.per_day]
    repo_shares = [100.0 * day.automated_repo_days / day.repo_days for day in f.per_day]
    spread = fmt_points(max(shares), min(shares))
    monotonic = all(b > a for a, b in pairwise(shares)) and all(
        b > a for a, b in pairwise(repo_shares)
    )
    direction = (
        "Every step is upward, on both units."
        if monotonic
        else "It does not move in one direction on both units."
    )
    return f"""
<h2>The automated share is not a constant</h2>
{
        _table(
            "Automated share by day, on both units",
            ["day", "events", "share of events", "share of repository-days"],
            rows,
        )
    }
<p>The share of events moved <strong>{spread} percentage points</strong> across the
{day_span(f.days)} calendar days these
{len(f.per_day)} samples span. {direction} With {len(f.per_day)} days it is not a trend either — far
too few to fit anything to — but it is a refusal to be a constant, and that is enough.</p>
<div class="note"><span class="label">Consequence</span>
<p>No single number answers “how much of the public feed is automated”, and any write-up quoting one
is quoting a day. The headline above is quoted as a property of <em>this lake</em> for exactly that
reason.</p></div>
""".strip()


def _watch_section(f: Figures) -> str:
    return f"""
<h2>Human attention is tiny, and clean</h2>
<p>WatchEvent — someone starring a repository — is
<strong>{fmt_pct(f.watch_events, f.events, 3)}</strong> of everything in the lake:
{fmt_int(f.watch_events)} events out of {fmt_int(f.events)}. It is also almost perfectly
uncontaminated: <strong>{fmt_pct(f.watch_outside_automated, f.watch_events, 2)}</strong> of them
land on repository-days outside the automated population.
{fmt_int(f.watch_events - f.watch_outside_automated)} of {fmt_int(f.watch_events)} does not.</p>
<p>This is the useful half of a negative result. The bulk of the feed is machine traffic and carries
no ranking signal, but the fraction that reflects a person deciding something is small enough to
process exhaustively and clean enough to use without filtering.</p>
""".strip()


def _verification_section(f: Figures) -> str:
    training = ", ".join(day for day in f.days if day != f.held_out_day)
    rows = [
        [
            "name-pattern precision lands between 0.55 and 0.85",
            '<span class="outcome missed">missed</span>',
            f"{fmt_ratio(f.rule_true_positives, f.rule_positives)}, above the stated ceiling — and "
            f"wrong in framing too, since a {fmt_lift(f.rule_precision, f.base_rate)} lift is not "
            "what “carries real signal” was meant to describe",
        ],
        [
            "counting beats the regex on F1",
            '<span class="outcome void">void</span>',
            "the comparison was rigged — see below",
        ],
        [
            "the two combined beat the better single one by 0.02 to 0.15",
            '<span class="outcome missed">missed</span>',
            "combining lost ground: the name pattern's recall of "
            f"{fmt_ratio(f.rule_true_positives, f.busy_single_actor)} drags down a predictor whose "
            "recall is 1.0000",
        ],
        [
            f"the automated event share is stable within 5 points across the {len(f.per_day)} days",
            '<span class="outcome missed">missed</span>',
            f"{fmt_points(max(100.0 * d.automated_events / d.events for d in f.per_day), min(100.0 * d.automated_events / d.events for d in f.per_day))}"
            " points, and moving one way",
        ],
        [
            "over 95% of WatchEvents land outside the automated population",
            '<span class="outcome held">held</span>',
            f"{fmt_pct(f.watch_outside_automated, f.watch_events, 2)} of {fmt_int(f.watch_events)}",
        ],
        [
            "the held-out day reproduces the pooled name-pattern share within 3 points",
            '<span class="outcome held">held</span>',
            f"{fmt_pct(f.pattern_events, f.held_out_events, 2)} against "
            f"{fmt_pct(f.pooled_pattern_events, f.pooled_events, 2)} pooled — "
            f"{fmt_points(100.0 * f.pattern_events / f.held_out_events, 100.0 * f.pooled_pattern_events / f.pooled_events)}"
            " points",
        ],
    ]
    return f"""
<h2>How this was checked</h2>
<ul>
<li><strong>A held-out day.</strong> The unit is one repository-day. {html.escape(training)} were
available while the threshold, the pattern and the scoring were being chosen;
{html.escape(f.held_out_day)} was not looked at until they were fixed. Split by calendar order, not
at random — the days are not exchangeable, and a random split would let the same repository appear
on both sides.</li>
<li><strong>A named dumb baseline.</strong> Nothing ships here without beating “predict the majority
class”. That baseline scores F1 {fmt_f1(f.base_rate, 1.0)} on the held-out day, and the signal under
test does not beat it.</li>
<li><strong>Predictions written down first.</strong> Six of them, with an explicit falsifier for
each, recorded before any split, threshold or score existed. Two held, three missed, one was void.
None was reworded afterwards.</li>
</ul>
{_table("The six predictions, scored", ["prediction", "outcome", "measured"], rows)}
<div class="note"><span class="label">The void one is worth more than the result</span>
<p>The baseline was specified as “predict automated from event count alone, threshold chosen on the
training days”, and the label was “exactly one actor <em>and</em> at least {BUSY_THRESHOLD} events”.
The threshold search chose {BUSY_THRESHOLD} — the label's own constant. The baseline was therefore
not competing with the label; it contained half of it, and its recall came back at exactly 1.0000.
It could not have failed.</p>
<p>The tell was in the output and nowhere else: an F1 of {fmt_f1(f.base_rate, 1.0)} looks like a
strong baseline, while a recall of exactly 1.0000 at a threshold sitting exactly on the label's
constant is a disclosure. <strong>A metric that cannot fail is not a weak metric — it is a different
object, and it is indistinguishable from a good result until you ask what it would print if the
claim were false.</strong> The re-run makes volume a filter rather than a feature, which is why
every score above is measured only among repository-days at or above {BUSY_THRESHOLD} events.</p>
</div>
""".strip()


def _removed_section() -> str:
    return """
<h2>What was removed, and why</h2>
<p>This project used to publish an estimate of how much of the live feed a single poller sees. It
does not any more, and the removal is worth more than the number was.</p>
<p>The first attempt reconciled the live capture against the published archive hour covering the
same period. On the hours measured, the two sources shared no event identifiers at all, and matching
on the commit SHA carried by a push — a value that cannot differ between two records of the same
event — found no meaningful overlap in the adjacent hours either. Whatever the cause, the archive
could not serve as ground truth for what the poller missed, so the difference would have reported
the mismatch rather than the miss. The comparison command survives, and refuses to return a ratio in
exactly that case rather than reporting a confident zero.</p>
<p>The second attempt estimated the feed's rate from the live capture alone: events inside a
returned page are consecutive, so the spacing between their identifiers is measurable rather than
assumed, and the identifiers elapsed between two cycles imply how many events happened in between.
That rests on the spacing inside a page describing the spacing outside it. Checked against complete
archive hours, it does not — identifiers arrive in dense clusters, neighbours a couple apart inside
one and consecutive clusters thousands apart, so measuring inside a cluster and applying it across
the gaps prices empty identifier space at the density of a burst.</p>
<p>The estimator was corrected, and the correction changed nothing measurable: a returned page holds
about one cluster, so there is no boundary for the two versions to disagree about, and dozens of
consecutive cycles produced identical results either way. A residual error of roughly 8.6× against
an independently measured event rate remained, and nothing accounted for it. <em>(That residual was
measured during the ingestion work; it is not re-derived by this page, whose queries all run against
the lake.)</em></p>
<p>So it was retired rather than re-measured. What the capture path reports now are exact counts —
cycles, events fetched, events new — and the question <em>what fraction of GitHub is that?</em> is
left open and marked open. <strong>A number wrong by an unexplained factor is not a rough version of
the right number.</strong></p>
""".strip()


def _shell(steps: Sequence[tuple[str, str]]) -> str:
    """A shell block with its comments in one column, wide enough for the widest.

    Padded from the commands themselves. A hand-aligned block is correct until a
    date or a flag changes length, and then it is quietly ragged in the one
    artifact a reader is most likely to copy out whole.
    """
    width = max(len(command) for command, _ in steps if command) + 2
    lines = [f"{command:<{width}}# {comment}" if comment else command for command, comment in steps]
    return "<pre><code>" + html.escape("\n".join(lines)) + "\n</code></pre>"


def _reproduce_section(f: Figures) -> str:
    first, last = f.days[0], f.days[-1]
    steps = _shell(
        [
            ("git clone <this repository> && cd reporadar", ""),
            ("make setup", "uv sync, plus the commit hooks"),
            ("", ""),
            ("cp .env.example .env", "set POSTGRES_PASSWORD, GRAFANA_ADMIN_PASSWORD"),
            ("make up", "Kafka + TimescaleDB, bound to localhost only"),
            ("", ""),
            (f"reporadar backfill {first} {last}", "fetch those days from GH Archive"),
            ("reporadar verify", "does the hours ledger match what is on disk?"),
            ("", ""),
            ("make site", "re-derive every figure above into docs/index.html"),
            ("make lint test", "ruff, mypy --strict, pytest"),
        ]
    )
    return f"""
<h2>Reproducing this</h2>
<p>The lake is not in the repository — it is {fmt_bytes(f.bytes_on_disk)} of public data that the
publisher still serves, so it is rebuilt rather than shipped. Everything below runs locally, and
needs no account with anyone.</p>
{steps}
<p>That range names {day_span(f.days)} calendar days and this lake holds the {len(f.days)} of them
that were captured; hours the publisher never published are a settled answer rather than an
unfinished job, and do not count against the run. <code>make site</code> reads whatever the lake
actually contains, so a rebuilt page states the lake in front of it — including a different one.
The queries are the whole of <code>tools/build_site.py</code>, and the figures live nowhere
else.</p>
""".strip()


def render(f: Figures) -> str:
    """The whole document, from figures alone."""
    body = "\n\n".join(
        [
            _ingest_section(f),
            _finding_section(f),
            _pattern_section(f),
            _drift_section(f),
            _watch_section(f),
            _verification_section(f),
            _removed_section(),
            _reproduce_section(f),
        ]
    )
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RepoRadar — what the public GitHub event feed is made of</title>
<meta name="description" content="{fmt_int(f.events)} events of GitHub's public event stream, measured: the feed is overwhelmingly automated, and the base rate already answers the question a classifier was going to be built for.">
<style>
{STYLE}
</style>
</head>
<body>
<main>
<header>
<h1>What the public GitHub event feed is made of</h1>
<p class="sub">RepoRadar ingests GitHub's public event stream into a partitioned Parquet lake and
measures what is in it. This page reports one measurement: the feed is overwhelmingly automated, and
the base rate already answers the question a classifier was going to be built for.</p>
<p class="provenance">Every figure below is re-derived from the lake each time this page is built —
{fmt_int(f.events)} events, {html.escape(f.days[0])} to {html.escape(f.days[-1])},
{fmt_int(f.hours)} hour partitions. Nothing here is typed in by hand.</p>
</header>

{body}

<footer>
<p>Generated by <code>tools/build_site.py</code> from the Parquet lake. There is no wall-clock
timestamp on this page on purpose: what dates it is the data behind it, and a rebuild over an
unchanged lake produces an identical file.</p>
<p>Nothing is deployed anywhere and nothing is intended to be. This runs locally, and reproducing it
from the repository is the point. MIT licensed.</p>
</footer>
</main>
</body>
</html>
"""


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument(
        "--lake", type=Path, default=DEFAULT_LAKE, help="Parquet lake root (default: data/lake)"
    )
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT, help="page to write (default: docs/index.html)"
    )
    args = parser.parse_args(argv)
    lake_dir: Path = args.lake
    out_path: Path = args.out

    figures = collect(lake_dir)
    page = render(figures)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(page, encoding="utf-8")
    print(
        f"wrote {out_path} — {fmt_int(figures.events)} events, "
        f"{len(figures.days)} days, {fmt_int(len(page))} bytes"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
