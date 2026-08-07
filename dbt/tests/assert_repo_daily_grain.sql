-- The mart's declared grain is one row per repository per day. Nothing enforces
-- that but the GROUP BY, and a GROUP BY is easy to widen by accident when a column
-- is added. This fails the build if the grain ever stops being what the model says
-- it is — a duplicated key here would silently double every number a dashboard
-- built on it.

select
    day,
    repo_id,
    count(*) as rows_at_this_key
from {{ ref('repo_daily') }}
group by day, repo_id
having count(*) > 1
