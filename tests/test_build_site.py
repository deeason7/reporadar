"""The page generator's arithmetic and its SQL, checked without a lake.

Two halves of ``tools/build_site.py`` can be wrong without anything noticing.

The **formatting** decides what a reader sees. A percentage rendered to two
places turns the WatchEvent share into ``0.03%`` and deletes the only digit that
says how small it is; a lift of 1.0358 rendered to one place becomes ``1.0×`` and
deletes the finding. Neither failure raises, and both look like a page that
built cleanly.

The **query shaping** decides what the figures are. Those queries cannot be run
here — they read 1.3 GiB that is not in the repository, and a test that needs the
data is a test that stops running the moment someone clones. So the SQL is built
as text and the text is asserted, which is enough to hold the two properties that
have actually broken: that the lake path survives quoting, and that
``(repo->>'name') IS NOT NULL`` keeps its parentheses.

That second one is not style. ``IS NOT NULL`` binds tighter than ``->>``, so
dropping the parentheses asks DuckDB for the key ``true``, and it fails with a
cast error quoting a repository name — a message that reads like malformed data
and sends the next person after the lake instead of the query.

Nothing here touches DuckDB, the filesystem, or the network.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from build_site import (
    BUSY_THRESHOLD,
    NAME_PATTERN,
    TOP_TYPES,
    DayRow,
    Figures,
    TypeRow,
    check_consistency,
    day_gaps,
    day_span,
    fmt_bytes,
    fmt_f1,
    fmt_int,
    fmt_lift,
    fmt_pct,
    fmt_points,
    fmt_ratio,
    held_out_sql,
    lake_source,
    main,
    per_day_sql,
    per_repo_sql,
    render,
    repo_day_cte,
    sql_literal,
    type_histogram_sql,
    watch_sql,
)

LAKE = Path("/data/lake")


def figures(**overrides: object) -> Figures:
    """A small, invented lake with arithmetic that can be checked by hand.

    Every count here is round on purpose: 48 of 50 is a precision of 0.96 exactly
    and 90 of 100 a base rate of 0.90, so an assertion on the rendered string is
    an assertion about the formatting and not about float noise. The numbers
    deliberately do not resemble the real ones — a fixture that echoes production
    lets a hard-coded figure pass as a computed one.
    """
    base: dict[str, object] = {
        "days": ("2026-01-01", "2026-01-03"),
        "events": 1_000,
        "hours": 48,
        "files": 2,
        "bytes_on_disk": 2 * 1024**3,
        "repos": 100,
        "actors": 90,
        "event_types": 3,
        "types": (
            TypeRow("PushEvent", 900),
            TypeRow("WatchEvent", 90),
            TypeRow("ForkEvent", 10),
        ),
        "per_repo_repos": 100,
        "single_actor_repos": 80,
        "per_repo_events": 1_000,
        "single_actor_events": 750,
        "held_out_day": "2026-01-03",
        "held_out_repo_days": 200,
        "held_out_events": 600,
        "pattern_repo_days": 20,
        "pattern_events": 300,
        "busy_repo_days": 100,
        "busy_single_actor": 90,
        "rule_positives": 50,
        "rule_true_positives": 48,
        "watch_events": 90,
        "watch_outside_automated": 89,
        "per_day": (
            DayRow("2026-01-01", 400, 150, 200, 10, 100),
            DayRow("2026-01-03", 600, 200, 400, 20, 300),
        ),
    }
    base.update(overrides)
    return Figures(**base)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Formatting
# --------------------------------------------------------------------------- #


def test_counts_are_grouped_for_reading() -> None:
    assert fmt_int(15_827_495) == "15,827,495"
    assert fmt_int(0) == "0"


def test_a_percentage_keeps_the_places_it_was_asked_for() -> None:
    assert fmt_pct(1, 3, 2) == "33.33%"
    assert fmt_pct(2, 3, 2) == "66.67%"
    assert fmt_pct(1, 1, 3) == "100.000%"


def test_a_small_share_survives_at_the_precision_it_needs() -> None:
    # The real WatchEvent share. Two places renders it "0.03%", which is a
    # different claim: it loses the digit that distinguishes it from 0.025%.
    assert fmt_pct(5_258, 15_827_495, 3) == "0.033%"
    assert fmt_pct(5_258, 15_827_495, 2) == "0.03%"


def test_rounding_does_not_flatter_the_direction_of_a_claim() -> None:
    # A share just under a boundary must not be printed as though it reached it.
    assert fmt_pct(9_949, 10_000, 2) == "99.49%"
    assert fmt_pct(9_949, 10_000, 1) == "99.5%"  # honest at one place, hence two
    assert fmt_ratio(2_419, 10_000) == "0.2419"


def test_a_percentage_of_nothing_is_refused() -> None:
    # Not 0%, and not a dash. Both read as "measured, and it came back small".
    with pytest.raises(ValueError):
        fmt_pct(0, 0)
    with pytest.raises(ValueError):
        fmt_ratio(0, 0)
    with pytest.raises(ValueError):
        fmt_lift(1.0, 0.0)


def test_lift_is_printed_to_two_places_and_no_more() -> None:
    # The real one: precision 0.9888924 against a base rate of 0.9547273.
    assert fmt_lift(0.9888924, 0.9547273) == "1.04×"
    # A third place invites reading precision into a figure whose whole point is
    # how close to 1.00 it is.
    assert fmt_lift(1.0, 1.0) == "1.00×"


def test_f1_is_the_harmonic_mean() -> None:
    assert fmt_f1(1.0, 1.0) == "1.0000"
    assert fmt_f1(0.5, 0.5) == "0.5000"
    # The prevalence baseline on the held-out day: precision 0.954727, recall 1.
    assert fmt_f1(0.9547273, 1.0) == "0.9768"
    with pytest.raises(ValueError):
        fmt_f1(0.0, 0.0)


def test_bytes_keep_both_readings() -> None:
    # The rounded figure is what a disk says; the exact one is what a checksum of
    # the directory would agree with. They disagree by 7%, so the page prints both.
    assert fmt_bytes(2 * 1024**3) == "2.00 GiB (2,147,483,648 bytes)"


def test_a_spread_is_reported_in_points() -> None:
    assert fmt_points(74.5953964, 68.9621581) == "5.63"


# --------------------------------------------------------------------------- #
# Query shaping
# --------------------------------------------------------------------------- #


def test_a_literal_survives_a_quote_in_the_path() -> None:
    assert sql_literal("plain") == "'plain'"
    assert sql_literal("it's") == "'it''s'"
    assert lake_source(Path("/tmp/o'brien")) == (
        "read_parquet('/tmp/o''brien/**/*.parquet', hive_partitioning = 1)"
    )


def test_the_lake_is_read_with_its_partitions() -> None:
    source = lake_source(LAKE)
    assert "/data/lake/**/*.parquet" in source
    # Without this, `dt` and `hr` are path fragments rather than columns, and
    # every per-day figure silently reads the whole lake.
    assert "hive_partitioning = 1" in source


def test_the_null_repository_filter_keeps_its_parentheses() -> None:
    # The precedence trap, and the reason this test exists at all: `IS NOT NULL`
    # binds tighter than `->>`, so the unparenthesised spelling asks DuckDB for
    # the key `true` and dies with a cast error naming a repository — a message
    # that sends the next reader after the data instead of the query.
    for sql in (repo_day_cte(LAKE), per_repo_sql(LAKE)):
        assert "(repo->>'name') IS NOT NULL" in sql
        assert not re.search(r"[^)]repo->>'name' IS NOT NULL", sql)


def test_a_repo_day_is_one_repository_on_one_day() -> None:
    sql = repo_day_cte(LAKE)
    assert "count(DISTINCT actor->>'login') AS actors" in sql
    assert "GROUP BY dt, repo_name" in sql


def test_a_day_filter_is_added_only_when_a_day_is_named() -> None:
    assert "dt = DATE" not in repo_day_cte(LAKE)
    assert "dt = DATE '2026-07-29'" in repo_day_cte(LAKE, "2026-07-29")
    # And it is escaped like anything else reaching the query as text.
    assert "dt = DATE 'o''clock'" in repo_day_cte(LAKE, "o'clock")


def test_the_scored_query_carries_the_threshold_and_the_pattern() -> None:
    sql = held_out_sql(LAKE)
    assert f"events >= {BUSY_THRESHOLD}" in sql
    assert sql_literal(NAME_PATTERN) in sql
    # The signal is scored only among busy repository-days. Scoring it over all
    # of them would credit it for the millions of one-event repositories it never
    # claims, which is a fact about the feed's tail and not about the signal.
    assert "FILTER (WHERE name_hit)" in sql
    assert f"FILTER (WHERE events >= {BUSY_THRESHOLD} AND name_hit AND actors = 1)" in sql


def test_the_per_day_query_reports_both_units_side_by_side() -> None:
    sql = per_day_sql(LAKE)
    assert "GROUP BY dt" in sql
    assert "ORDER BY dt" in sql
    assert "AS automated_events" in sql
    assert "AS automated_repo_days" in sql


def test_the_watch_join_gives_every_side_its_own_alias() -> None:
    # DuckDB resolves a bare column name in a join condition against whichever
    # side has one. Here one side holds the extracted repository name and the
    # other the JSON it came from, so a shared alias fails on a cast whose
    # message points at the data rather than at the query.
    sql = watch_sql(LAKE)
    assert "ON auto_dt = watch_dt AND auto_repo = watch_repo" in sql
    assert "LEFT JOIN" in sql  # an inner join would report only the contaminated ones
    for alias in ("auto_dt", "auto_repo", "watch_dt", "watch_repo"):
        assert sql.count(alias) >= 2


def test_the_histogram_is_ordered_by_volume() -> None:
    assert "ORDER BY events DESC" in type_histogram_sql(LAKE)


# --------------------------------------------------------------------------- #
# Dates
# --------------------------------------------------------------------------- #


def test_a_gap_in_what_was_captured_is_visible() -> None:
    assert day_gaps(["2026-07-22", "2026-07-27", "2026-07-28", "2026-07-29"]) == [5, 1, 1]
    assert day_gaps(["2026-01-01", "2026-01-02"]) == [1]
    assert day_gaps(["2026-01-01"]) == []


def test_a_span_is_not_a_count_of_days() -> None:
    # Four days spanning a week is a different claim from four consecutive ones,
    # and the drift figure is a statement about the span.
    days = ["2026-07-22", "2026-07-27", "2026-07-28", "2026-07-29"]
    assert day_span(days) == 8
    assert len(days) == 4


# --------------------------------------------------------------------------- #
# Consistency
# --------------------------------------------------------------------------- #


def test_two_readings_of_the_held_out_day_must_agree() -> None:
    check_consistency(figures())  # the fixture is self-consistent


def test_a_disagreement_between_the_two_readings_is_refused() -> None:
    # The negative control. Both queries measure the held-out day, and a page
    # that printed one while the other disagreed would be wrong in the only way
    # a reader cannot see.
    drifted = figures(
        per_day=(
            DayRow("2026-01-01", 400, 150, 200, 10, 100),
            DayRow("2026-01-03", 599, 200, 400, 20, 300),
        )
    )
    with pytest.raises(RuntimeError, match="event count disagrees"):
        check_consistency(drifted)

    mismatched_pattern = figures(
        per_day=(
            DayRow("2026-01-01", 400, 150, 200, 10, 100),
            DayRow("2026-01-03", 600, 200, 400, 20, 299),
        )
    )
    with pytest.raises(RuntimeError, match="name-pattern event count disagrees"):
        check_consistency(mismatched_pattern)


def test_a_held_out_day_missing_from_the_daily_rows_is_refused() -> None:
    with pytest.raises(RuntimeError, match="is not one of"):
        check_consistency(figures(held_out_day="2026-01-02"))


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #


def test_the_page_is_a_whole_document_with_a_real_title() -> None:
    page = render(figures())
    assert page.startswith("<!doctype html>")
    title = re.search(r"<title>(.+?)</title>", page)
    assert title is not None
    assert "GitHub" in title.group(1)
    assert page.rstrip().endswith("</html>")


def test_the_page_needs_nothing_from_the_network() -> None:
    page = render(figures())
    # No stylesheet, no script, no font, no image, no import — the page has to
    # render from the file alone, offline, years from now.
    assert "http" not in page
    assert "<script" not in page
    assert "<link" not in page
    assert "@import" not in page
    assert "url(" not in page
    assert "<img" not in page


def test_the_page_answers_to_both_colour_schemes() -> None:
    assert "prefers-color-scheme: dark" in render(figures())


def test_every_table_can_scroll_on_a_narrow_screen() -> None:
    page = render(figures())
    tables = page.count("<table>")
    assert tables >= 4
    # A table wide enough to overflow must scroll inside its own box; letting the
    # page scroll sideways instead breaks every other line on the page too.
    assert page.count('<div class="scroll"><table>') == tables


def test_the_figures_reach_the_page() -> None:
    page = render(figures())
    assert "1,000" in page  # events
    assert "75.00%" in page  # single-actor share of events, 750 of 1,000
    assert "90.00%" in page  # base rate among busy repository-days, 90 of 100
    assert "0.9600" in page  # name-pattern precision, 48 of 50
    assert "1.07×" in page  # lift of 0.96 over 0.90


def test_the_page_is_computed_rather_than_written_down() -> None:
    # The failure this guards against is a figure that was correct when it was
    # typed. Change the lake and the page must change with it; if it does not,
    # the number lives in the HTML and no rebuild will ever fix it.
    before = render(figures())
    after = render(figures(single_actor_events=500))
    assert "75.00%" in before and "75.00%" not in after
    assert "50.00%" in after


def test_the_smallest_types_are_pooled_rather_than_dropped() -> None:
    many = tuple(TypeRow(f"Type{i}Event", 100 - i) for i in range(TOP_TYPES + 3))
    page = render(figures(types=many, events=sum(t.events for t in many)))
    assert "3 other types" in page
    assert "Type0Event" in page
    # Dropping the tail silently would leave the shares summing to less than 100
    # with nothing on the page saying so.
    assert "100.000%" in page


def test_a_share_of_events_is_labelled_as_a_claim_about_volume() -> None:
    # The whole point of the page. If this sentence goes, the headline figure is
    # readable as a statement about how many repositories are automated, which is
    # the misreading the analysis exists to take apart.
    page = render(figures())
    assert "claim about volume, not about population" in page


def test_the_proxy_label_is_stated_on_the_page() -> None:
    page = render(figures())
    assert "no ground truth" in page
    assert (
        f"at least\n{BUSY_THRESHOLD} events" in page or f"at least {BUSY_THRESHOLD} events" in page
    )


def test_a_flat_series_is_not_described_as_rising() -> None:
    flat = figures(
        per_day=(
            DayRow("2026-01-01", 400, 150, 200, 10, 100),
            DayRow("2026-01-03", 600, 200, 300, 10, 300),
        )
    )
    assert "Every step is upward" in render(figures())
    assert "Every step is upward" not in render(flat)
    assert "does not move in one direction" in render(flat)


# --------------------------------------------------------------------------- #
# The claims the page makes about direction
# --------------------------------------------------------------------------- #

UPWARD = "Every step is upward, on both units."
NOT_ONE_DIRECTION = "It does not move in one direction on both units."


def test_both_units_rising_is_reported_as_upward() -> None:
    # The positive case, so the two tests below are refusals rather than a claim
    # that never fires. In the fixture both shares rise: 50% -> 66.67% of events,
    # 6.67% -> 10% of repository-days.
    page = render(figures())
    assert UPWARD in page
    assert NOT_ONE_DIRECTION not in page


def test_a_flat_step_is_not_an_upward_step() -> None:
    # `>` and `>=` differ on exactly one input — the equal pair — and a lake with
    # two identical days is the likeliest thing to produce one. The page states
    # this as a fact about the data, so the difference between the two spellings
    # is the difference between a true sentence and a false one.
    flat_events = figures(
        per_day=(
            DayRow("2026-01-01", 400, 150, 200, 10, 100),
            DayRow("2026-01-03", 600, 200, 300, 20, 300),  # 50% both days
        )
    )
    page = render(flat_events)
    assert NOT_ONE_DIRECTION in page
    assert UPWARD not in page

    flat_repo_days = figures(
        per_day=(
            DayRow("2026-01-01", 400, 150, 200, 10, 100),
            DayRow("2026-01-03", 600, 300, 400, 20, 300),  # 1-in-15 both days
        )
    )
    page = render(flat_repo_days)
    assert NOT_ONE_DIRECTION in page  # flat on the second unit is equally not upward
    assert UPWARD not in page


def test_one_unit_rising_is_not_both_units_rising() -> None:
    # "on both units" is the load-bearing half of the sentence. With events rising
    # and repository-days falling, an `or` here would publish "every step is
    # upward" over data that steps both ways — the exact overclaim the section
    # exists to refuse.
    diverging = figures(
        per_day=(
            DayRow("2026-01-01", 400, 150, 200, 10, 100),
            DayRow("2026-01-03", 600, 300, 400, 10, 300),  # events up, repo-days down
        )
    )
    page = render(diverging)
    assert NOT_ONE_DIRECTION in page
    assert UPWARD not in page


def test_the_training_days_are_the_days_that_were_not_held_out() -> None:
    # The page names which days the rule was fitted on. Inverting this filter
    # would print the held-out day as its own training set — a claim that the
    # evaluation was done on the data it was fitted to, which is the one thing
    # the held-out day exists to prevent.
    three_days = figures(
        days=("2026-01-01", "2026-01-02", "2026-01-03"),
        held_out_day="2026-01-03",
        per_day=(
            DayRow("2026-01-01", 200, 75, 100, 5, 50),
            DayRow("2026-01-02", 200, 75, 100, 5, 50),
            DayRow("2026-01-03", 600, 200, 400, 20, 300),
        ),
    )
    page = render(three_days)
    assert "2026-01-01, 2026-01-02 were" in page
    assert "2026-01-03 were" not in page  # the held-out day is not its own training set


def test_the_written_size_is_reported_in_bytes_not_characters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """``len(str)`` counts characters; the file is written as UTF-8.

    The page is full of em-dashes and typographic quotes, each costing three
    bytes, so the two numbers diverge by a few hundred — and the gap GROWS with
    the prose. It is a slope, not a constant, which is why one spot-check of the
    difference could never have shown it.
    """
    monkeypatch.setattr("build_site.collect", lambda lake_dir: figures())
    out = tmp_path / "index.html"

    assert main(["--lake", str(tmp_path / "lake"), "--out", str(out)]) == 0

    printed = re.search(r"([\d,]+) bytes", capsys.readouterr().out)
    assert printed is not None, "the command no longer reports a size at all"
    reported = int(printed.group(1).replace(",", ""))
    assert reported == out.stat().st_size  # what the filesystem actually holds
    assert reported > len(out.read_text(encoding="utf-8"))  # ...and it is not the character count
