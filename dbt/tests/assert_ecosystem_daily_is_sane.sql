-- The arithmetic a daily ecosystem row cannot violate, plus the bound on coverage.
--
-- Same reasoning as the repository-level consistency test: none of these can fail
-- while the GROUP BY is correct, and all of them fail loudly if a future join fans
-- rows out — the usual way an aggregate starts lying while still looking plausible.
--
-- `hours_present` is bounded at 24 because a UTC day has 24 hours and the lake is
-- partitioned by hour. Exceeding it would mean the partition column has stopped
-- meaning an hour of the day, which is the kind of thing that stays invisible until
-- a total is quietly twice what it should be. Zero is excluded too: a day with no
-- hours produces no rows here at all, so a row claiming zero coverage is a
-- contradiction rather than an empty day.

select
    day,
    events,
    repos,
    actors,
    hours_present,
    events_without_repo
from {{ ref('ecosystem_daily') }}
where actors > events
   or repos > events
   or pushes + stars + forks + issues + pull_requests + releases > events
   or events_without_repo > events
   or hours_present < 1
   or hours_present > 24
   or events <= 0
