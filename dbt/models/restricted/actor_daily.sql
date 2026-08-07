-- One row per account per day: the per-actor grain the campaign-detection work
-- needs, and the only model here that describes people rather than repositories.
--
-- ⚠️ It is written to the `restricted` schema, and that is a governance boundary,
-- not a naming preference. This project's rule is that person-level analysis is
-- aggregated to repository or ecosystem level in anything published — no
-- per-account pages, no "top contributors" leaderboards. A comment saying so would
-- be followed wherever somebody remembered it; a separate schema is followed
-- everywhere, because a dashboard connection granted `marts` cannot read this table
-- at all. The constraint becomes something the database enforces rather than
-- something a reviewer has to notice.
--
-- Why it exists despite that: actor-level features are what distinguish a genuine
-- surge of interest from a coordinated one — how many distinct repositories an
-- account touched in a day, how concentrated its activity is, whether a burst of
-- stars came from accounts that do nothing else. Those questions cannot be asked at
-- repository grain, because at repository grain the campaign and the launch look
-- the same. The aggregate that answers them is legitimate; publishing a page about
-- the individual is what is not, and the two are separated here by where the rows
-- live.
--
-- No `where actor_id is not null` filter, unlike `repo_daily`. The staging model
-- carries a hard `not_null` on `actor_id` — zero nulls in the 5,090,496 events
-- measured — so a filter here would be a line that can never do anything, which is
-- worse than absent: it reads as a handled case and quietly asserts that nulls are
-- expected. If that test ever fails, the build stops before reaching this model,
-- which is the behaviour worth having.

select
    archive_day                                                     as day,
    actor_id,

    -- `arg_max` rather than `max`, for the same reason `repo_daily` uses it on
    -- repository names: accounts get renamed, and `max` returns whichever login
    -- sorts highest — a name nobody ever used. This returns the login carried by
    -- that day's most recent event from the account.
    arg_max(actor_login, created_at)                                as actor_login,

    count(*)                                                        as events,

    -- The discriminating feature, and the reason this grain is worth materialising:
    -- an account acting across many repositories in one day looks very different
    -- from one acting repeatedly on a single repository, and the ratio of these two
    -- columns is where that shows up.
    count(distinct repo_id)                                         as repos,

    count(*) filter (where event_type = 'PushEvent')                as pushes,
    count(*) filter (where event_type = 'WatchEvent')               as stars,
    count(*) filter (where event_type = 'ForkEvent')                as forks,
    count(*) filter (where event_type = 'IssuesEvent')              as issues,
    count(*) filter (where event_type = 'PullRequestEvent')         as pull_requests,
    count(*) filter (where event_type = 'ReleaseEvent')             as releases,

    min(created_at)                                                 as first_event_at,
    max(created_at)                                                 as last_event_at

from {{ ref('stg_events') }}
group by archive_day, actor_id
