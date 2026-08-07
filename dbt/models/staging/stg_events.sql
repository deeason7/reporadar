-- Typed, deduplicated events: the one place the published envelope becomes columns.
--
-- The archive stores actor, repo and org as nested JSON, which is right for a
-- record of what was published but wrong for every query that follows. This model
-- lifts the four fields everything downstream groups by into typed columns and
-- leaves `payload` as JSON, because no downstream model has earned a typed view of
-- it yet and guessing at one per event type would be a schema nobody asked for.
--
-- The deduplication is by event id and is not ceremonial: an hour can be ingested
-- more than once across a re-run, and the published files are not guaranteed to be
-- free of repeats either. `qualify` picks one row per id rather than `distinct`
-- over every column, because two records of one event can differ in ways we do not
-- want to preserve as two rows.

with published as (

    select
        id                                          as event_id,
        type                                        as event_type,
        cast(actor ->> 'id' as bigint)              as actor_id,
        actor ->> 'login'                           as actor_login,
        cast(repo ->> 'id' as bigint)               as repo_id,
        repo ->> 'name'                             as repo_name,
        org ->> 'login'                             as org_login,
        public                                      as is_public,
        -- The archive stores this without a zone, meaning UTC by convention. A
        -- plain cast to a zoned type would read it as *local* time and move every
        -- event by the reader's offset — and on a machine set to UTC the two
        -- spellings are indistinguishable, so the bug would only ever appear for
        -- somebody else. `at time zone 'UTC'` states the convention instead of
        -- inheriting an ambient one.
        created_at at time zone 'UTC'               as created_at,
        payload,
        dt                                          as archive_day,
        hr                                          as archive_hour
    from {{ source('lake', 'events') }}

)

select *
from published
qualify row_number() over (partition by event_id order by created_at) = 1
