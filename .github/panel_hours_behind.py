"""Check that the dashboard's staleness panel agrees with `reporadar marts-status`.

The two nearly answer the same question, in two languages, one in SQL inside a
dashboard and one in Python. Neither can import the other, so the arithmetic
exists twice and nothing stops the two from drifting into different numbers about
the same database. This runs the panel's own query, taken from the file that is
provisioned, and compares it against an expected value.

*Nearly*, and the gap is deliberate. The command measures the marts against the
lake's files, because that is what a build reads and therefore the only thing
that answers "would rebuilding change what is published". A dashboard is a
database connection and cannot see files, so the panel measures them against the
hours ledger instead. The two coincide exactly when the ledger and the files
agree — which is what `reporadar verify` checks — so the caller passes the value
it expects rather than this asserting equality blindly, and CI exercises both the
agreeing case and the diverging one.

A file rather than another heredoc because it is called three times, once per
state, and three copies of a check is three chances for one of them to be edited.

It proves the query, not the grant: this connects as the owner, so a panel that
was correct and unreadable would pass here. The permissions are checked from the
other side, in the `dashboards` job, which rejects any panel naming a table the
dashboard's role is not granted.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys

import asyncpg

PANEL_ID = 8
DASHBOARD = pathlib.Path("grafana/dashboards/pipeline-ops.json")


def panel_sql() -> str:
    """The staleness panel's query, read from the file Grafana is given."""
    for panel in json.loads(DASHBOARD.read_text())["panels"]:
        if panel["id"] == PANEL_ID:
            return str(panel["targets"][0]["rawSql"])
    raise SystemExit(f"no panel with id {PANEL_ID} in {DASHBOARD}")


async def check(expected: int) -> None:
    connection = await asyncpg.connect(os.environ["REPORADAR_POSTGRES_DSN"])
    try:
        value = await connection.fetchval(panel_sql())
    finally:
        await connection.close()
    if int(value) != expected:
        raise SystemExit(f"panel {PANEL_ID} returned {value}, expected {expected}")
    print(f"panel {PANEL_ID} agrees with the command: {value} hour(s) not yet built from")


if __name__ == "__main__":
    asyncio.run(check(int(sys.argv[1])))
