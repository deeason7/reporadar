-- One row per repository per day: the grain every activity question starts from.
--
-- Grouped on the archive partition rather than on `date(created_at)`. The two are
-- the same value — an hour's file is rejected at write time unless every row in it
-- belongs to the hour it is named for — but only one of them lets the reader skip
-- files it does not need. The equality is asserted as a test rather than trusted,
-- because it is the kind of invariant that holds until the day it silently doesn't.
--
-- `arg_max` rather than `max` for the repository name: repositories get renamed,
-- and `max` would return whichever name sorts highest, which is a name nobody chose.
-- This returns the name carried by that day's most recent event — the closest thing
-- to "what it was called at the end of the day" that the events can actually support.

select
    archive_day                                                     as day,
    repo_id,
    arg_max(repo_name, created_at)                                  as repo_name,

    count(*)                                                        as events,
    count(distinct actor_id)                                        as actors,

    count(*) filter (where event_type = 'PushEvent')                as pushes,
    count(*) filter (where event_type = 'WatchEvent')               as stars,
    count(*) filter (where event_type = 'ForkEvent')                as forks,
    count(*) filter (where event_type = 'IssuesEvent')              as issues,
    count(*) filter (where event_type = 'PullRequestEvent')         as pull_requests,
    count(*) filter (where event_type = 'ReleaseEvent')             as releases,

    min(created_at)                                                 as first_event_at,
    max(created_at)                                                 as last_event_at

from {{ ref('stg_events') }}
-- A handful of published events carry an empty repository object (3 in 5,090,496
-- measured, all ForkEvents). They are excluded rather than grouped, because
-- grouping on a null key would invent one repository that every keyless event in
-- the feed belongs to — a row that would look like the busiest repository on
-- GitHub. The exclusion is not silent: the staging tests warn on every occurrence,
-- so this filter can never quietly grow.
where repo_id is not null
group by archive_day, repo_id
