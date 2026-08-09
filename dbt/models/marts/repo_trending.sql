-- Day-over-day movement in how many distinct accounts touched a repository.
--
-- What this measures is participation, not popularity, and the distinction is the
-- whole reason the model looks like this. A trending list normally ranks by stars,
-- and there are not enough of them here to rank a repository day by day: 5,258 star
-- events across four complete days is about 1,300 a day for the whole feed, so most
-- repositories have no star on most days and a daily ranking built on that column
-- would be sorting mostly ties, while looking entirely healthy doing it.
--
-- Worth stating precisely, because the obvious reading is wrong: the feed is not
-- missing stars. Measured across 96 complete hours, star events are 0.033% of all
-- events but 1.04% of events on repositories where five or more accounts are
-- active - thirty-one times higher. Roughly two thirds of everything published goes
-- to single-account repositories with generated six-letter names, so a share of the
-- total is a statement about that population rather than about the ecosystem. The
-- scarcity that matters here is per repository per day, and it is real.
--
-- Ranking on raw event count instead does not work either, and it fails in a way
-- worth stating precisely, because the output still looks like a plausible list.
-- On 2026-07-29 every one of the twelve busiest repositories by event count had
-- exactly one actor — 5,118 pushes from a single account at the top. Sorting by
-- events sorts by automation. Sorting the same day by distinct actors returns
-- PostHog, tenstorrent, odoo, grafana, ROCm, llvm and pytorch. The two lists share
-- no rows. Distinct actors is not a refinement of the event count here; it selects
-- a different population.
--
-- No verdict is attached to any of this. Whether a rise is organic or purchased is
-- a question about actor quality and coordination that needs its own model and its
-- own labelled data, and until that exists the honest output is the movement and
-- the evidence for it, with nothing in the schema implying a judgement has been
-- made.

with comparable_days as (

    -- Only days the lake holds in full. A day with three of its twenty-four hours
    -- is not a smaller version of a full day, it is a different measurement, and
    -- comparing one against the other reports the ingest schedule as ecosystem
    -- movement. Normalising a partial day by `hours_present` would be worse than
    -- excluding it: it assumes activity is spread evenly across the day, and it is
    -- not — the feed has a strong daily cycle, so scaling three night hours to
    -- twenty-four invents a quiet day and then reports it as a decline.
    select day
    from {{ ref('ecosystem_daily') }}
    where hours_present = 24

),

adjacent_pairs as (

    -- Calendar-adjacent days, joined explicitly rather than taken from `lag()`.
    -- `lag()` over the ordered days returns the previous *row*, which is only the
    -- previous day when the lake has no gaps — and this lake has a five-day one.
    -- The failure that spelling produces is silent and directional: it would pair
    -- 2026-07-27 with 2026-07-22, label five days of accumulated change as one
    -- day's movement, and rank every repository that grew over that gap at the top.
    -- Requiring both days to be present as full days makes the gap drop out of the
    -- model instead of being absorbed into it.
    select
        this_day.day        as day,
        this_day.day - 1    as prior_day
    from comparable_days as this_day
    join comparable_days as previous_day
      on previous_day.day = this_day.day - 1

),

today as (

    select
        pairs.day,
        pairs.prior_day,
        repos.repo_id,
        repos.repo_name,
        repos.actors,
        repos.events
    from adjacent_pairs as pairs
    join {{ ref('repo_daily') }} as repos
      on repos.day = pairs.day

),

compared as (

    select
        today.day,
        today.repo_id,
        today.repo_name,

        today.actors                                    as actors,
        -- A repository with no row on the prior day genuinely had no events that
        -- day, and `coalesce` to zero is correct *because* of the join above: the
        -- prior day is guaranteed to be in the lake as a full day, so an absent row
        -- means measured silence rather than an unobserved day. Without that
        -- guarantee this coalesce would be the model's worst bug — it would turn
        -- "we were not looking" into "nothing happened" and score it as growth.
        coalesce(prior.actors, 0)                       as actors_prior,
        today.actors - coalesce(prior.actors, 0)        as actors_change,

        today.events                                    as events,
        coalesce(prior.events, 0)                       as events_prior,

        -- Reported, not ranked on. Actor counts are small integers — 96% of
        -- repository-days have exactly one actor and the daily maximum observed is
        -- 59 — so a ratio has a tiny denominator almost everywhere and 1 → 3 reads
        -- as +200%. Null rather than infinity when the repository was absent, so a
        -- consumer has to handle the case rather than sort it to the top by
        -- accident.
        case
            when coalesce(prior.actors, 0) = 0 then null
            else (today.actors - prior.actors)::double precision / prior.actors
        end                                             as actors_change_ratio,

        prior.repo_id is null                           as absent_prior_day

    from today
    left join {{ ref('repo_daily') }} as prior
           on prior.repo_id = today.repo_id
          and prior.day     = today.prior_day

)

select
    day,
    repo_id,
    repo_name,
    actors,
    actors_prior,
    actors_change,
    actors_change_ratio,
    events,
    events_prior,
    absent_prior_day,

    -- Ranked within the day on absolute change, with the tie broken by today's
    -- actor count and then by id. The id is not decoration: without a total order
    -- the same data produces different ranks on different builds, and a dashboard
    -- that reshuffles when nothing changed teaches its reader to distrust it.
    row_number() over (
        partition by day
        order by actors_change desc, actors desc, repo_id
    )                                                   as rank_in_day

from compared
-- The floor applies to whichever day is larger so that declines survive it. A model
-- that filtered on today alone would drop a repository falling from twenty actors
-- to one, which is the same movement measured in the other direction and is at
-- least as interesting.
--
-- Five is chosen from the measured distribution rather than picked for roundness.
-- Counted over the two comparable days in the lake, the floor admits 919 rows per
-- day at five, 5,005 at three, and 149 at ten. Three is dominated by the two-actor
-- population — 96% of repository-days have a single actor and most of the rest have
-- two — so it buries the collaborative repositories the model exists to surface.
-- Ten ranks cleanly but discards most of them. Five is where a day's list is large
-- enough to have a distribution and small enough that every row means several
-- distinct people touched the same repository on the same day.
where greatest(actors, actors_prior) >= {{ var('trending_min_actors') }}
