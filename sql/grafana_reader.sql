-- The database role the dashboard connects as, and the only thing standing between
-- a chart and the tables it has no business reading.
--
-- The dashboard is given its own login rather than reusing the application's. The
-- application role owns every table here: it creates, drops and rewrites the
-- aggregates on each build, and it writes the events the consumer validates. A
-- dashboard needs none of that, and handing it those rights means the only thing
-- preventing a panel from deleting a table is that nobody wrote that query.
--
-- What it may read is deliberately short, and what is left out is the point:
--
--   marts.*              the published aggregates — repository and ecosystem grains
--   public.archive_hours which hours were ingested, missing or failed
--
--   NOT restricted.*     per-account rows. This project aggregates person-level
--                        signals before publishing them, and this is where that
--                        stops being a promise and becomes a permission. A panel
--                        charting the busiest accounts is one query away from
--                        existing; it is a `permission denied` away from working.
--   NOT public.events    the validated store keeps raw envelopes including account
--                        logins. An operational count does not need row access, and
--                        a view can be granted later if one is genuinely wanted.
--
-- Idempotent: safe to re-run, and it must be, because it is the fix for the failure
-- mode below and that fix has to be applicable to a database already running.

\set ON_ERROR_STOP on

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
        CREATE ROLE grafana_reader LOGIN;
    END IF;
END
$$;

-- Set every run rather than only at creation, so rotating the password is this
-- script plus a new value in the environment.
ALTER ROLE grafana_reader WITH PASSWORD :'grafana_password';

GRANT CONNECT ON DATABASE :"dbname" TO grafana_reader;
GRANT USAGE ON SCHEMA marts, public TO grafana_reader;

GRANT SELECT ON ALL TABLES IN SCHEMA marts TO grafana_reader;
GRANT SELECT ON public.archive_hours TO grafana_reader;

-- The failure this exists to prevent, and it is a quiet one.
--
-- The aggregates are `table` materialisations: every build DROPs and re-CREATEs
-- them. A grant applies to the tables that existed when it ran, so without the
-- line below the dashboard works, somebody rebuilds the models, and every panel
-- turns into `permission denied` for a table that visibly exists. Default
-- privileges attach to the *creating* role, which is why this names the
-- application role explicitly rather than relying on whoever runs this file.
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA marts
    GRANT SELECT ON TABLES TO grafana_reader;

-- Said out loud rather than left to the absence of a grant. A future schema-wide
-- grant, or a role inheriting from somewhere, would otherwise open this silently —
-- and the whole design rests on this one boundary holding.
REVOKE ALL ON SCHEMA restricted FROM grafana_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA restricted
    REVOKE SELECT ON TABLES FROM grafana_reader;
