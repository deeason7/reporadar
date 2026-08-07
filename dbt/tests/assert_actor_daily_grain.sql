-- The declared grain is one row per account per day, and nothing enforces it but
-- the GROUP BY — which is easy to widen by accident the first time a column is
-- added. A duplicated key here would double every per-account number computed from
-- this model, and the campaign-detection work reads exactly those numbers, so the
-- failure would land as a false signal rather than as an obvious error.

select
    day,
    actor_id,
    count(*) as rows_at_this_key
from {{ ref('actor_daily') }}
group by day, actor_id
having count(*) > 1
