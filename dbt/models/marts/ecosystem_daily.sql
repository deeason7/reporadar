-- One row per day for the whole ecosystem: the top-level numbers, and the coverage
-- that says how much of the day they were computed from.
--
-- `hours_present` is not decoration, and it is the reason this model exists at a
-- daily grain rather than as a `sum()` in a dashboard panel. The lake holds
-- whatever hours have been ingested, which on any given day is between 0 and 24 —
-- so a daily total is a sum over an unknown fraction of the day unless the fraction
-- travels beside it. A panel reading "2.1M events on Tuesday" from seven ingested
-- hours is not wrong about the seven hours; it is wrong about Tuesday, and nothing
-- in the number itself says which. Carrying the count of hours makes those two
-- claims separable by construction instead of by a caption somebody remembers.
--
-- Counted from the lake rather than from the hours ledger, deliberately. The ledger
-- knows strictly more — it separates an hour the publisher never released from one
-- simply not ingested yet — but it lives in Postgres, and a model whose subject is
-- "what the lake contains" should be answerable from the lake alone. The richer
-- reconciliation between the two is `reporadar verify`'s question, and it already
-- asks it from both sides.

select
    archive_day                                                     as day,

    count(*)                                                        as events,
    count(distinct repo_id)                                         as repos,
    count(distinct actor_id)                                        as actors,

    count(*) filter (where event_type = 'PushEvent')                as pushes,
    count(*) filter (where event_type = 'WatchEvent')               as stars,
    count(*) filter (where event_type = 'ForkEvent')                as forks,
    count(*) filter (where event_type = 'IssuesEvent')              as issues,
    count(*) filter (where event_type = 'PullRequestEvent')         as pull_requests,
    count(*) filter (where event_type = 'ReleaseEvent')             as releases,

    -- How many of the day's 24 hours the lake actually holds. A day still being
    -- ingested and a day with permanent gaps look identical in every other column.
    count(distinct archive_hour)                                    as hours_present,

    -- The published events carrying an empty repository object. `repo_daily`
    -- excludes them because they cannot belong to a per-repository row; this model
    -- counts them, because they were genuinely published and an ecosystem total
    -- that quietly dropped them would not reconcile against the one that keeps
    -- them. Recording the difference here is what lets a test assert the two marts
    -- agree — `events - events_without_repo` must equal the repository totals for
    -- the same day, and neither number is trustworthy without the other.
    count(*) filter (where repo_id is null)                         as events_without_repo,

    min(created_at)                                                 as first_event_at,
    max(created_at)                                                 as last_event_at

from {{ ref('stg_events') }}
group by archive_day
