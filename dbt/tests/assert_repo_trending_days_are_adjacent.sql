-- Every trending row compares two calendar-adjacent days that the lake holds in
-- full. This is the test for the model's central hazard, and it is worth being
-- explicit about why it needs to exist when the model already looks correct.
--
-- The model's `adjacent_pairs` join is one line, and the natural way to write that
-- line is `lag(day) over (order by day)`. Both spellings are right on a gapless
-- lake and they diverge silently the moment one hour is missing. This lake has a
-- five-day gap in it — 2026-07-22 sits alone, then 2026-07-27 through 2026-07-29 —
-- so the wrong spelling here would pair 07-27 with 07-22, present five days of
-- accumulated movement as one day's, and rank every repository that grew across
-- the gap at the top of the list. Nothing in the output would look wrong: the
-- numbers are plausible, the ranks are ordered, and only the meaning is broken.
--
-- The gap is real rather than constructed, which is what makes this a regression
-- test and not a hypothetical. If a later backfill fills 2026-07-23 to 07-26 the
-- test keeps holding, because what it asserts is the relationship, not the dates.

with trending_days as (

    select distinct day
    from {{ ref('repo_trending') }}

),

full_days as (

    select day
    from {{ ref('ecosystem_daily') }}
    where hours_present = 24

)

select
    t.day,
    t.day - 1                                       as expected_prior_day,
    this_day.day is null                            as this_day_not_full,
    prior_day.day is null                           as prior_day_not_full
from trending_days as t
left join full_days as this_day  on this_day.day  = t.day
left join full_days as prior_day on prior_day.day = t.day - 1
where this_day.day is null
   or prior_day.day is null
