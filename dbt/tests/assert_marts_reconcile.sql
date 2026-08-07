-- The three daily models describe the same events at three grains, so they have to
-- agree, and this is the test that says so out loud.
--
-- Each one is individually consistent — every model here has a grain test and an
-- arithmetic test — and all three could still disagree with each other, because
-- consistency within a model says nothing about agreement between models. That is
-- the failure this catches: a filter added to one grain and not another, a join
-- introduced upstream that fans rows out for repositories but not for accounts, a
-- `distinct` quietly dropped.
--
-- The event reconciliation has to subtract `events_without_repo` rather than
-- comparing totals directly. `repo_daily` excludes events published with an empty
-- repository object, because they cannot belong to a per-repository row;
-- `ecosystem_daily` keeps them, because they were genuinely published. So the two
-- are *supposed* to differ, by exactly a number one of them records. An equality
-- that needs a correction term is only trustworthy when the correction is itself a
-- measured column and not a constant somebody typed — three, when it was last
-- measured, and there is no reason for that to stay three.

with repos_rolled_up as (

    select
        day,
        sum(events)     as events,
        count(*)        as repos
    from {{ ref('repo_daily') }}
    group by day

),

actors_rolled_up as (

    select
        day,
        count(*)        as actors
    from {{ ref('actor_daily') }}
    group by day

)

select
    e.day,
    e.events                    as ecosystem_events,
    e.events_without_repo,
    r.events                    as repo_events,
    e.repos                     as ecosystem_repos,
    r.repos                     as repo_rows,
    e.actors                    as ecosystem_actors,
    a.actors                    as actor_rows
from {{ ref('ecosystem_daily') }} as e
full outer join repos_rolled_up  as r on r.day = e.day
full outer join actors_rolled_up as a on a.day = e.day
where
    -- Every event that belongs to a repository is counted once at both grains.
    e.events - e.events_without_repo is distinct from r.events
    -- One repository-day row per repository the ecosystem model counted.
    or e.repos is distinct from r.repos
    -- One account-day row per account the ecosystem model counted.
    or e.actors is distinct from a.actors
    -- A day present at one grain and absent at another. The outer joins are what
    -- make this visible: an inner join would drop exactly the rows that prove it.
    or e.day is null
    or r.day is null
    or a.day is null
