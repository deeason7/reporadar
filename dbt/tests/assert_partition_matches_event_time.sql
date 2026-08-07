-- The lake's central invariant, checked from the reading side.
--
-- An hour's file is rejected at write time unless every row in it belongs to the
-- hour the file is named for, which is what makes it safe for `repo_daily` to
-- group on the partition column instead of on the event timestamp. That check
-- runs when a file is written; this one runs over whatever is on disk now,
-- including files written by an older version of the writer.
--
-- If this ever returns rows, grouping by partition and grouping by event date have
-- stopped being the same question, and the mart is answering the one nobody asked.

-- What this does not catch, measured rather than assumed: an hour's file
-- duplicated at a wrong path. Those rows carry ids that already exist under the
-- correct partition, so the staging deduplication keeps one row per id and the
-- misfiled copies are gone before this runs — 40 in, 0 surviving, when it was
-- tried. That is the deduplication working, not a hole: a duplicated file is
-- indistinguishable from a redelivered one, and collapsing it keeps the counts
-- right. Whether the files on disk are the ones the record says they are is a
-- different question, asked by `reporadar verify`.
--
-- Both sides are compared in UTC explicitly. `created_at` is zoned, and extracting
-- a date or an hour from it uses the session's timezone unless told otherwise —
-- which would make this test pass in UTC and fail everywhere else, reporting the
-- reader's location as a data fault.

select
    archive_day,
    archive_hour,
    event_id,
    created_at
from {{ ref('stg_events') }}
where archive_day <> cast(created_at at time zone 'UTC' as date)
   or archive_hour <> extract(hour from created_at at time zone 'UTC')
