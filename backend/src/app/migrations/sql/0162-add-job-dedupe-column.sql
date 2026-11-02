-- Dedupe jobs: at most one not-started job per key.
--
-- A job submitted with `::jobs/dedupe` or `::jobs/debounce` is marked as
-- dedupe, and the database admits a single `new` row for each (tenant,
-- name, queue, label). The submit no longer deletes the pending row
-- before inserting: the unique index decides, and the insert resolves it
-- with `ON CONFLICT`. `dedupe` leaves the pending row alone and skips the
-- insert; `debounce` refreshes its `scheduled_at` and `params` in place,
-- so the work keeps its id and its event history.
--
-- The predicate is `status = 'new'` and not the live set of the
-- `exclusive` column: a row that was already dispatched, is running or
-- waits for a retry leaves the index, and a new submit of the same key is
-- admitted. The status never changes inside the upsert, so the row that
-- is refreshed stays in the index.
--
-- `dedupe` is false by default, so the rows that were never submitted
-- with the option (including every row that exists when this migration
-- runs) stay out of the index: it starts empty and no cleanup of
-- duplicates is needed. The label stays a label.

ALTER TABLE job
    ADD COLUMN IF NOT EXISTS dedupe boolean NOT NULL DEFAULT false;

CREATE UNIQUE INDEX IF NOT EXISTS job__new_dedupe__uniq
    ON job (tenant, name, queue, label)
    WHERE dedupe
      AND status = 'new';

-- NOTE: on an instance with a large `job` table, apply this migration out of
-- band before deploying, building the index with CONCURRENTLY: this migration
-- runs inside a transaction, and a plain CREATE INDEX locks the table against
-- writes for the whole build (in Penpot every webhook-producing file save
-- inserts a job), while CONCURRENTLY does not. Adding a column with a
-- constant DEFAULT is metadata-only since PostgreSQL 11, so the column is
-- cheap and the index is the only slow part. Run both statements on their
-- own, not inside a transaction block, and in this order (the index needs
-- the column):
--
--   ALTER TABLE job
--       ADD COLUMN IF NOT EXISTS dedupe boolean NOT NULL DEFAULT false;
--
--   CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS job__new_dedupe__uniq
--       ON job (tenant, name, queue, label)
--    WHERE dedupe
--      AND status = 'new';
--
-- A CONCURRENTLY build interrupted midway leaves the index INVALID: drop it
-- and retry. Verify with:
--
--   SELECT indexdef FROM pg_indexes
--    WHERE indexname = 'job__new_dedupe__uniq';
--   SELECT indisvalid, indisunique FROM pg_index
--    WHERE indexrelid = 'job__new_dedupe__uniq'::regclass;
--
-- Both statements are IF NOT EXISTS, so this migration finds the column and
-- the index and does nothing: the schema is already in place. It still takes
-- its locks for the instant the no-op lasts (ACCESS EXCLUSIVE for the ALTER,
-- SHARE for the index), so a long transaction on `job` still delays the
-- deploy.
--
-- Treat the checks above as a gate, not as a curiosity: IF NOT EXISTS matches
-- by name, so an INVALID index left by an interrupted build satisfies this
-- migration too and the dedupe silently stops enforcing. Drop it and rebuild
-- before deploying.
