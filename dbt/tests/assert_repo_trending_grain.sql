-- One row per repository per day, and one rank per position within a day.
--
-- The grain half is the same check the other marts carry: a join that fans rows out
-- is the usual way an aggregate starts lying while still looking plausible, and this
-- model joins `repo_daily` to itself, which is exactly where that happens if the
-- prior-day join ever loses part of its key.
--
-- The rank half is not redundant with it. `row_number()` produces distinct values
-- per partition by construction, so a duplicate rank cannot appear while the window
-- is written correctly — but a rank that is unique within the wrong partition is
-- still unique. Counting distinct ranks against distinct rows per day is what
-- notices if `partition by day` is ever dropped, which would rank the whole table
-- as one list and leave every day but the first with no row ranked 1.

with grain as (

    select
        day,
        repo_id,
        count(*) as rows_for_key
    from {{ ref('repo_trending') }}
    group by day, repo_id
    having count(*) > 1

),

ranks as (

    select
        day,
        count(*)                    as rows_in_day,
        count(distinct rank_in_day) as distinct_ranks,
        min(rank_in_day)            as lowest_rank
    from {{ ref('repo_trending') }}
    group by day
    having count(*) <> count(distinct rank_in_day)
        or min(rank_in_day) <> 1

)

select day, repo_id, rows_for_key, null::bigint as rows_in_day, null::bigint as distinct_ranks
from grain

union all

select day, null::bigint as repo_id, null::bigint as rows_for_key, rows_in_day, distinct_ranks
from ranks
