"""Are the published marts still the ones the lake would produce?

The marts are built by a command somebody types. The lake is converged by a loop
that runs on its own. Nothing connected the two, so the marts could be built from
a lake three days stale and every dashboard panel would look exactly as healthy
as it does now — the numbers would simply be smaller than the truth, with nothing
in them saying so.

**No new state is introduced to fix that, because the fact already exists twice.**
The lake's partition files say which hours are there. ``ecosystem_daily.hours_present``
says how many hours each day's row was computed from — it is carried for the
dashboard's benefit, and it happens to be exactly the marts' own record of what
they saw. Comparing the two answers "have the marts kept up" without anybody
writing down when a build last ran. A ``last_built_at`` column would be a second
home for a fact the data already carries, and it would answer a *weaker* question:
a build that ran five minutes ago against a lake that has since moved is recent
and wrong at the same time.

That is ``verify``'s rule applied one level up — a completion record is only
meaningful relative to the store it describes.

**The files are the denominator, and not the hours ledger, which was the obvious
choice and is the wrong one.** The question worth answering is "would rebuilding
change what is published", and only the files can answer it: a build reads the
lake, so an hour the ledger claims and the lake does not hold is an hour no
rebuild can ever supply. Measured on a working database before this was written —
the ledger claimed 35 hours across three days while the lake held three files, so
a ledger-based check reported 32 hours behind and told the operator to run a build
that would have changed nothing, for ever. *A check nobody can satisfy is worse
than no check, because the first real staleness it buries is the one nobody looks
at any more.* The ledger disagreeing with the files is a real fault and it belongs
to ``verify``, which reports it in far more detail and fails on it.

So the three instruments each own exactly one comparison, and they compose:
``verify`` checks the ledger against the files, this checks the files against the
marts, and the dashboard panel — which can reach neither the ledger's counterpart
nor the files, being a database connection — checks the ledger against the marts.
**When ``verify`` passes, the panel and this command necessarily agree.**

The asymmetry carries over from ``verify`` too:

- the marts holding **fewer** hours than the lake does is a number that lies,
  quietly and in the safe-looking direction: a day understated by the hours added
  since the last build. This is what fails the check.
- the marts holding **more** is a disagreement rather than a falsehood. It means
  the marts were built from partitions that have since been removed, so the
  published figures are real numbers about hours no longer on disk. Nothing is
  understated, so it is reported and does not fail.

Nothing here builds anything. A checker that rebuilt what it found would make
"are the marts current" unanswerable by the thing that is supposed to answer it,
and it would put a dbt invocation inside a process whose whole point is to be
cheap enough to run on every pass.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Final

from reporadar.ingest.ledger import Connection
from reporadar.ingest.verify import lake_files

logger = logging.getLogger(__name__)

#: The exit code that means "stale", as opposed to any other non-zero exit, which
#: means the check itself did not complete. Two codes rather than one because the
#: caller acts on this: a wrapper that rebuilds on *any* failure would rebuild
#: because the database was unreachable, and would report the rebuild's own failure
#: as the answer. **"Rebuild" has to mean "it is stale", never "something went
#: wrong"** — the same rule, pointed the other way, that says a green check has to
#: mean the thing succeeded rather than that it failed differently than expected.
#:
#: Three, and not two, because two is already spoken for twice over. It is the
#: conventional code for a usage error, so ``marts-status --typo`` exits 2 through
#: the CLI framework before this module runs at all; and the runner that invokes
#: the command exits 2 when it cannot spawn it. Both of those mean *the check did
#: not run*, which is the exact opposite of what a wrapper does with this value —
#: it would have rebuilt the published aggregates because somebody misspelled a
#: flag. Measured rather than assumed: a wrapper branching on 2 was watched
#: rebuilding in both cases.
#:
#: The general rule this is an instance of: 0, 1 and 2 are claimed by convention
#: (success, error, usage), so a code carrying application meaning starts at 3.
STALE_EXIT_CODE: Final = 3

#: Whether the marts have ever been built. ``to_regclass`` answers with NULL
#: instead of raising when the name is not there, which is the difference between
#: "no marts yet" being a state this can report and being an exception that has to
#: be caught by class — and catching it by class would drag the driver's exception
#: hierarchy into a module that otherwise talks to a two-method protocol.
MARTS_EXIST: Final = "SELECT to_regclass('marts.ecosystem_daily') IS NOT NULL"

#: The marts' own account of what they were built from. One row per day, and the
#: only thing this module reads out of the database — the other side of the
#: comparison is the filesystem.
MART_DAYS: Final = """
SELECT day, hours_present::bigint AS hours
FROM marts.ecosystem_daily
ORDER BY day
"""


class Drift(StrEnum):
    """How one day's marts stand against the lake."""

    CURRENT = "current"
    """Built from exactly the hours the lake holds. Nothing to do."""

    UNBUILT = "unbuilt"
    """The lake holds this day and the marts have no row for it at all."""

    BEHIND = "behind"
    """The marts were built from fewer hours than the lake now holds."""

    SURPLUS = "surplus"
    """Built from hours the lake no longer holds. Reported, not a failure."""


#: The kinds that mean a published number understates its day. Kept as data rather
#: than an ``if`` chain for the same reason ``verify`` does it: the exit code and
#: the report must not be able to disagree about which findings are serious.
STALE: Final[frozenset[Drift]] = frozenset({Drift.UNBUILT, Drift.BEHIND})


@dataclass(frozen=True)
class DayDrift:
    """One day, as both records describe it."""

    day: date
    lake_hours: int
    mart_hours: int | None
    """``None`` when the marts have no row for this day."""

    @property
    def kind(self) -> Drift:
        """Which of the four states this day is in."""
        if self.mart_hours is None:
            return Drift.UNBUILT
        if self.mart_hours < self.lake_hours:
            return Drift.BEHIND
        if self.mart_hours > self.lake_hours:
            return Drift.SURPLUS
        return Drift.CURRENT

    @property
    def stale(self) -> bool:
        """Whether this day's published numbers are behind the lake."""
        return self.kind in STALE

    @property
    def detail(self) -> str:
        """The two numbers, in the order that says which way the day is wrong."""
        if self.mart_hours is None:
            return f"lake holds {self.lake_hours} hour(s); the marts have no row for this day"
        if self.kind is Drift.SURPLUS:
            return (
                f"built from {self.mart_hours} hour(s); the lake now holds "
                f"{self.lake_hours} — the extra hours have been removed since"
            )
        return f"lake holds {self.lake_hours} hour(s), the marts were built from {self.mart_hours}"


@dataclass
class FreshnessReport:
    """What the comparison examined and what it found."""

    built: bool = True
    """Whether the marts exist at all. ``False`` makes every lake day unbuilt."""

    days: list[DayDrift] = field(default_factory=list)
    """Every day either record knows about, oldest first."""

    @property
    def drift(self) -> list[DayDrift]:
        """The days the two records describe differently."""
        return [day for day in self.days if day.kind is not Drift.CURRENT]

    @property
    def stale_days(self) -> list[DayDrift]:
        """The days whose published numbers are behind the lake."""
        return [day for day in self.days if day.stale]

    @property
    def hours_behind(self) -> int:
        """Ingested hours the lake holds and the marts have not been built from.

        The number that says how much a rebuild would change, rather than how many
        days are affected — one day short by twelve hours and twelve days short by
        one are the same day count and very different amounts of missing data.
        """
        return sum(day.lake_hours - (day.mart_hours or 0) for day in self.stale_days)

    @property
    def ok(self) -> bool:
        """Whether every day the lake holds is reflected in the marts."""
        return not self.stale_days

    def as_dict(self) -> dict[str, object]:
        """The shape a log line and an exit message want."""
        return {
            "built": self.built,
            "days": len(self.days),
            "current": len(self.days) - len(self.drift),
            "stale": len(self.stale_days),
            "surplus": len(self.drift) - len(self.stale_days),
            "hours_behind": self.hours_behind,
        }


def lake_hours_by_day(lake_dir: Path) -> dict[date, int]:
    """How many hourly partitions the lake holds for each day.

    Counts files rather than reading them, which is what keeps this cheap enough
    to run before every build — a directory walk, no scan of the data. The cost is
    one blind spot, stated rather than hidden: a partition file holding no rows
    would be counted here and could never appear in ``hours_present``, so its day
    would read as permanently behind. A published archive hour is never empty, and
    ``reporadar verify --counts`` is what compares recorded counts against the
    files themselves.
    """
    return Counter(day for day, _hour, _path in lake_files(lake_dir))


async def marts_freshness(connection: Connection, *, lake_dir: Path) -> FreshnessReport:
    """Compare the hours in the lake against the hours the marts were built from.

    Read-only on both sides. The lake is walked, not read; the database answers
    one small query over a table with one row per day.
    """
    in_lake = lake_hours_by_day(lake_dir)

    built: dict[date, int] = {}
    exists = _as_bool((await connection.fetch(MARTS_EXIST))[0][0])
    if exists:
        rows = await connection.fetch(MART_DAYS)
        built = {_as_date(row[0]): _as_int(row[1]) for row in rows}

    # Every day either side knows about. A day only the marts have is the surplus
    # case and must not be dropped — an inner join over the same two sets would
    # silently discard exactly the rows worth reporting.
    days = [
        DayDrift(day, in_lake.get(day, 0), built.get(day))
        for day in sorted(set(in_lake) | set(built))
    ]
    # `built` is empty both when the marts do not exist and when they hold no
    # rows, and those want the same handling everywhere except the sentence
    # printed at the end — so the flag records which one it was and nothing else
    # branches on it.
    report = FreshnessReport(built=exists, days=days)

    logger.info("marts freshness: %s", report.as_dict())
    return report


# A driver hands back whatever the server sent, so these columns are only typed at
# runtime. Narrowing here — naming the type that actually arrived — means a schema
# that has drifted from this module's queries says so in one sentence, rather than
# as a TypeError several frames away from the query that caused it.


def _as_bool(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"expected a boolean column, got {type(value).__name__}")
    return value


def _as_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"expected an integer column, got {type(value).__name__}")
    return value


def _as_date(value: object) -> date:
    if not isinstance(value, date):
        raise TypeError(f"expected a date column, got {type(value).__name__}")
    return value
