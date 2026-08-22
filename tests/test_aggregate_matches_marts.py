"""The two derivations of a repository-day must agree, and here is the test that says so.

``aggregate.py`` computes ecosystem and repository-day totals from the lake. So do
the transformation models in ``dbt/models/``. Two derivations of one quantity is
normally a defect; it is deliberate here, because they run in different places for
different reasons — one on a hosted runner with nothing but the package installed,
the other against a real warehouse — and it is only *safe* while they agree.

🔴 **This file exists because that safety was asserted and not checked.**
``aggregate.py``'s docstring said *"A test asserts the agreement rather than
trusting this paragraph"*, and it was repeated elsewhere. No such test existed. The sentence was written in the same breath as the design it describes,
which is exactly when a claim about verification is least likely to be verified.
⇒ 🔑 **"A test asserts this" is itself a claim, and it is the one kind of claim
that makes every other claim beside it look checked.**

The models are read from ``dbt/models/`` and executed, rather than reimplemented
here. A test that restated the SQL would be a third derivation, and three copies
agreeing with each other proves only that one author was consistent.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import duckdb
import pytest

from reporadar.ingest.aggregate import BREAKOUT_EVENT_TYPES, aggregate_day
from test_aggregate import DAY, _event, _read, _stage_hour  # noqa: F401

REPO_ROOT = Path(__file__).resolve().parent.parent
MODELS = REPO_ROOT / "dbt" / "models"

#: The one clause that cannot be executed here, and the reason it is removed.
#:
#: ``stg_events`` converts the archive's naive timestamp with ``at time zone
#: 'UTC'``. DuckDB routes that through ``pytz``, which the package does not
#: declare — the aggregate carries the timestamp naive for exactly that reason.
#: So the two pipelines genuinely differ in how they *represent* event time, and
#: that difference is intentional and documented rather than a disagreement this
#: test should hide.
#:
#: It is stripped so the rest of the model can run, and the substitution count is
#: asserted, so that a model rewritten around this clause fails here instead of
#: quietly testing a query nobody wrote.
TZ_CLAUSE = re.compile(r"\s*at time zone 'UTC'", re.IGNORECASE)


#: SQL line comments. Stripped before any substitution below, because these models
#: explain themselves at length and quote the very clauses this file rewrites --
#: `stg_events` names `at time zone 'UTC'` twice, once in prose and once in SQL.
#: The first attempt matched both and the count guard refused, which is the guard
#: working: it would otherwise have rewritten a comment and left the SQL alone.
#: ⇒ 🔑 In a codebase where comments carry the reasoning, prose is a substring of
#:   the code, and any rewrite that does not exclude it is editing the wrong copy.
SQL_COMMENT = re.compile(r"--[^\n]*")


def _model(name: str) -> str:
    """One model, with its commentary removed so substitutions hit SQL only."""
    return SQL_COMMENT.sub("", (MODELS / name).read_text(encoding="utf-8"))


def _stg_events_sql(lake_glob: str) -> str:
    """The real staging model, pointed at a lake path instead of a dbt source."""
    sql = _model("staging/stg_events.sql")

    sql, refs = re.subn(
        r"\{\{\s*source\('lake',\s*'events'\)\s*\}\}",
        f"read_parquet('{lake_glob}', hive_partitioning=false)",
        sql,
    )
    assert refs == 1, f"expected exactly one lake source in stg_events, found {refs}"

    sql, tz = TZ_CLAUSE.subn("", sql)
    assert tz == 1, (
        f"expected exactly one `at time zone 'UTC'` in stg_events, found {tz}. "
        "The model changed around the one clause this test removes — re-read it "
        "before trusting anything below."
    )
    return sql


def _mart_sql(name: str, lake_glob: str) -> str:
    """One mart, with its staging reference inlined as a CTE."""
    sql = _model(f"marts/{name}.sql")
    sql, refs = re.subn(r"\{\{\s*ref\('stg_events'\)\s*\}\}", "stg_events", sql)
    assert refs == 1, f"expected exactly one stg_events ref in {name}, found {refs}"
    return f"WITH stg_events AS (\n{_stg_events_sql(lake_glob)}\n)\n{sql}"


@pytest.fixture()
def both_pipelines(tmp_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]], Path]:
    """Run the aggregate over a fixture and keep the lake so dbt can read it too.

    Built adversarially on purpose — a repeated event id, a renamed repository, an
    event with no repository, and rows either side of the floor. A fixture where
    the two pipelines cannot disagree would pass no matter what either one did.
    """
    archive = tmp_path / "archive"
    _stage_hour(
        archive,
        0,
        *(_event(f"a-{n}", 0, repo_id=100, repo_name="zzz/old", minute=n) for n in range(25)),
        *(_event(f"b-{n}", 0, repo_id=200, repo_name="quiet/repo", minute=n) for n in range(3)),
        _event("orphan", 0, repo_id=None, minute=40),
        _event("dupe", 0, repo_id=100, minute=41),
    )
    _stage_hour(
        archive,
        1,
        *(_event(f"c-{n}", 1, repo_id=100, repo_name="aaa/new", minute=n) for n in range(6)),
        _event("dupe", 1, repo_id=100, minute=41),  # the same id, an hour later
    )

    report = aggregate_day(
        DAY,
        archive_dir=archive,
        lake_dir=tmp_path / "lake",
        aggregate_dir=tmp_path / "agg",
        base_url="https://archive.gharchive.invalid",
        keep_lake=True,  # the marts read the same lake this aggregate was built from
    )
    lake_glob = str(tmp_path / "lake" / f"dt={DAY:%Y-%m-%d}" / "hr=*" / "events.parquet")
    return _read(report.ecosystem_path)[0], _read(report.repo_path), Path(lake_glob)


def _run(sql: str) -> list[dict[str, Any]]:
    con = duckdb.connect()
    try:
        cur = con.execute(sql)
        columns = [d[0] for d in cur.description or []]
        return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
    finally:
        con.close()


#: The columns the two pipelines must agree on exactly. Timestamps are excluded
#: with a reason, not by omission — see TZ_CLAUSE.
COUNT_COLUMNS = ("events", "actors", *BREAKOUT_EVENT_TYPES.keys())


def test_the_ecosystem_totals_agree_with_the_mart(
    both_pipelines: tuple[dict[str, Any], list[dict[str, Any]], Path],
) -> None:
    eco, _, lake_glob = both_pipelines
    mart = _run(_mart_sql("ecosystem_daily", str(lake_glob)))

    assert len(mart) == 1, f"the mart produced {len(mart)} rows for one day"
    for column in (*COUNT_COLUMNS, "repos", "hours_present", "events_without_repo"):
        assert eco[column] == mart[0][column], (
            f"ecosystem.{column}: aggregate={eco[column]} mart={mart[0][column]} — "
            "the two derivations have diverged, and the aggregate is the one that "
            "is permanent"
        )


def test_every_repository_day_above_the_floor_agrees_with_the_mart(
    both_pipelines: tuple[dict[str, Any], list[dict[str, Any]], Path],
) -> None:
    _, rows, lake_glob = both_pipelines
    mart = {r["repo_id"]: r for r in _run(_mart_sql("repo_daily", str(lake_glob)))}

    assert rows, "the fixture produced no repository-days above the floor"
    for row in rows:
        counterpart = mart.get(row["repo_id"])
        assert counterpart is not None, f"repo {row['repo_id']} is missing from the mart"
        for column in COUNT_COLUMNS:
            assert row[column] == counterpart[column], (
                f"repo {row['repo_id']}.{column}: aggregate={row[column]} "
                f"mart={counterpart[column]}"
            )
        # The rename rule is the one non-obvious agreement: both must return the
        # name from the day's latest event, not the alphabetically largest.
        assert row["repo_name"] == counterpart["repo_name"] == "aaa/new"


def test_the_floor_is_the_only_reason_a_row_is_absent(
    both_pipelines: tuple[dict[str, Any], list[dict[str, Any]], Path],
) -> None:
    """The aggregate keeps a subset of the mart's rows. That subset must be
    explained entirely by the floor — if the two disagreed about *which*
    repository-days exist, every count above could still match while the history
    quietly lost rows the marts can see."""
    _, rows, lake_glob = both_pipelines
    mart = _run(_mart_sql("repo_daily", str(lake_glob)))
    floor = rows[0]["min_events"]

    expected = {r["repo_id"] for r in mart if r["events"] >= floor}
    assert {r["repo_id"] for r in rows} == expected

    dropped = {r["repo_id"] for r in mart} - expected
    assert dropped, "the fixture never exercises the floor, so this proves nothing"


def test_the_comparison_can_actually_fail() -> None:
    """The control. Every assertion above compares two dictionaries built by
    similar-looking code; if both sides came from the same query, or the column
    list were empty, all three tests would pass over any pair of pipelines."""
    assert COUNT_COLUMNS, "the compared column list is empty"
    a = {"events": 10}
    b = {"events": 11}
    assert a["events"] != b["events"], "control: two different values compared equal"

    # The substitution guard must refuse a model it does not recognise. Fed a
    # model with the clause already removed, `_stg_events_sql` must raise rather
    # than proceed -- otherwise a future edit could silently leave the test
    # running a query nobody wrote.
    raw = _model("staging/stg_events.sql")
    assert TZ_CLAUSE.search(raw), "the clause the guard counts is no longer present"
    assert len(TZ_CLAUSE.findall(raw)) == 1, (
        "comment stripping failed: the clause still appears more than once"
    )
