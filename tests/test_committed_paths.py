"""Every path the scheduled job commits must actually be committable.

This file exists because of a defect that no other check in the repository could
have seen. The daily job writes its run record to ``aggregates/runs.log``, and a
``*.log`` rule three lines long in ``.gitignore`` — written years earlier, about
something else entirely — silently excluded it. Nothing would have failed: the
job would have run, written the file, staged nothing, printed "nothing to
commit", exited 0, and reported success every day. The run record is also what
keeps a public repository's scheduled workflow from being disabled after 60 days
of no activity, so the observable outcome would have been a green job that
quietly switched itself off, mid-absence, with no error anywhere.

⇒ 🔑 **An ignore rule is the only kind of configuration whose failure mode is
silence by design.** Every other misconfiguration produces an error somewhere.

So these tests ask **git** what it will do, never ``.gitignore`` what it says.
Parsing the file would re-implement precedence, negation and anchoring — the
three things that made the original bug possible — and would agree with the bug.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

#: Everything the scheduled job writes and expects to survive a commit.
COMMITTED_BY_THE_SCHEDULE = (
    "aggregates/ecosystem/dt=2026-08-21/ecosystem_daily.parquet",
    "aggregates/repo/dt=2026-08-21/repo_daily.parquet",
    "aggregates/runs.log",
)

#: Things that must STAY ignored. Without this half, "nothing is ignored" would
#: pass every test above — a check that only ever asserts one direction cannot
#: tell a correct rule from a deleted one.
MUST_STAY_IGNORED = (
    ".env",
    "data/lake/dt=2026-08-21/hr=0/events.parquet",
    "data/raw/gharchive/2026-08-21-0.json.gz",
    "dbt/target/compiled/x.sql",
    "src/reporadar/__pycache__/cli.cpython-312.pyc",
    ".DS_Store",
)


def _is_ignored(path: str) -> bool:
    """Ask git, not the file. ``check-ignore`` exits 0 when the path is ignored."""
    result = subprocess.run(
        ["git", "check-ignore", "-q", path],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
    )
    if result.returncode not in (0, 1):
        raise RuntimeError(f"git check-ignore failed on {path}: {result.stderr!r}")
    return result.returncode == 0


@pytest.mark.parametrize("path", COMMITTED_BY_THE_SCHEDULE)
def test_the_schedule_can_commit_what_it_writes(path: str) -> None:
    assert not _is_ignored(path), (
        f"{path} is ignored, so the daily job would stage nothing and commit "
        "nothing while reporting success. This is the exact defect this file "
        "was written for — check .gitignore for a broad rule that reaches it."
    )


@pytest.mark.parametrize("path", MUST_STAY_IGNORED)
def test_what_must_never_be_committed_is_still_ignored(path: str) -> None:
    """The negative half. A `.gitignore` emptied by accident would otherwise make
    every test above pass, and the first casualty would be `.env`."""
    assert _is_ignored(path), f"{path} is NOT ignored and must be"


def test_the_check_itself_can_fail() -> None:
    """The positive control. If `git check-ignore` were silently not working —
    wrong directory, missing binary, exit code misread — every assertion above
    would pass by returning False for everything. This is the project's own
    recurring defect: an instrument reporting clean because it never looked."""
    assert _is_ignored(".env"), "control: a definitely-ignored path read as not ignored"
    assert not _is_ignored("pyproject.toml"), "control: a tracked file read as ignored"


def test_the_data_rule_is_anchored_so_it_cannot_reach_a_future_directory() -> None:
    """Unanchored, `data/` matches a directory of that name at any depth — so a
    `docs/data/` added later would be ignored by a rule written about the local
    lake, and nobody would find out until something was missing from a clone."""
    assert _is_ignored("data/lake/x.parquet"), "the local lake must stay ignored"
    assert not _is_ignored("docs/data/x.parquet"), (
        "a nested data/ directory is caught by the unanchored rule — anchor it with "
        "a leading slash so it only matches the one directory it was written for"
    )
