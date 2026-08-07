-- Arithmetic that must hold for any correct aggregation, checked because the ways
-- it can break are silent.
--
-- A repository-day cannot have more distinct actors than events, since every actor
-- was counted through at least one event; and the per-type counts are subsets of
-- the total, so their sum cannot exceed it. Neither can fail while the GROUP BY is
-- right. Both fail loudly if a future join fans rows out — which is the single most
-- common way an aggregate model starts lying while still looking plausible.

select
    day,
    repo_id,
    events,
    actors,
    pushes + stars + forks + issues + pull_requests + releases as typed_events
from {{ ref('repo_daily') }}
where actors > events
   or pushes + stars + forks + issues + pull_requests + releases > events
   or events <= 0
