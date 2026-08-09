-- The arithmetic and the definitions a trending row cannot violate.
--
-- Most of these cannot fail while the SELECT is written correctly, which is the
-- point: they are cheap, and each one names a specific way the model could start
-- reporting movement that did not happen.
--
-- The two worth explaining are the last pair. `absent_prior_day` and a zero
-- `actors_prior` are two spellings of the same fact, produced by different
-- expressions — one reads the join's null, the other coalesces it — so they can
-- drift apart if either changes, and the column that would then be wrong is the one
-- a reader trusts to distinguish "new" from "quiet". And `actors_change_ratio` must
-- be null exactly when there is no prior activity to divide by: a ratio that
-- silently became zero, or infinity, in that case would sort to one end of the list
-- and put repositories there for a reason that has nothing to do with movement.

select
    day,
    repo_id,
    actors,
    actors_prior,
    actors_change,
    actors_change_ratio,
    absent_prior_day
from {{ ref('repo_trending') }}
where
    -- The change is the difference. Stated because it is stored rather than derived
    -- at read time, and a stored derivation is one that can go stale.
    actors_change is distinct from actors - actors_prior

    -- A repository-day row exists only because the repository had events that day,
    -- so it had at least one actor. Zero here means the join produced a row the
    -- source could not have.
    or actors < 1
    or actors_prior < 0

    -- The floor the model filters on, asserted from the other side.
    or greatest(actors, actors_prior) < {{ var('trending_min_actors') }}

    -- Events and actors travel together: a repository cannot have more distinct
    -- accounts than events on either day.
    or actors > events
    or actors_prior > events_prior

    -- The two spellings of "the repository was absent" must agree.
    or absent_prior_day <> (actors_prior = 0)

    -- Null ratio exactly when there is nothing to divide by.
    or (actors_prior = 0 and actors_change_ratio is not null)
    or (actors_prior > 0 and actors_change_ratio is null)
