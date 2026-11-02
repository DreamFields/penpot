-- Consumer generalization: the schema of the whole feature in one step.
--
-- This single migration replaces the five steps this branch used to ship
-- separately (team storage usage, exclusive jobs, the audit cursor, the
-- old pending queue and dedupe jobs). The migration runner executes every
-- pending step inside ONE transaction, so five files bought no isolation:
-- their locks piled up until the same commit, and pre-applying one of
-- them out of band did not avoid the others.
--
-- The migration is additive and idempotent: every statement is
-- IF NOT EXISTS / IF EXISTS, so it is a no-op when the operator applied
-- it out of band first (see the NOTE at the end) and safe to re-run.
--
-- The statements take the locks in the same order the audit-log producer
-- uses (audit_log, then audit_log_pending, then job), so a deploy that
-- still has the previous version running cannot deadlock against it: the
-- producer inserts the audit row, then its pending, then submits the
-- webhook job, and this migration walks the same order. What it takes
-- last (`job`) is what the producer takes last.

-- ---------------------------------------------------------------------
-- Team storage usage: cached per-team media storage bytes.
--
-- A background audit-log consumer marks dirty teams and a bounded job
-- recomputes their totals, so uploads read one narrow row instead of
-- scanning `storage_object`. `dirty_at` set = pending recompute; null =
-- fresh. A missing row means never computed (fail open + mark dirty).
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS team_storage_usage (
  team_id     uuid         PRIMARY KEY REFERENCES team(id) ON DELETE CASCADE,
  total_bytes bigint       NOT NULL DEFAULT 0,
  dirty_at    timestamptz  NULL DEFAULT NULL,
  updated_at  timestamptz  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS team_storage_usage__dirty_at__idx
    ON team_storage_usage (dirty_at ASC, team_id ASC)
 WHERE dirty_at IS NOT NULL;

-- Serves the self-healing branch of the scheduler (rows nobody refreshed
-- for a week), which filters `dirty_at IS NULL`: a partial index keeps the
-- dirty rows out, so each branch of the query is an index range scan of
-- the rows it is going to enqueue instead of a scan of the whole table.
CREATE INDEX IF NOT EXISTS team_storage_usage__updated_at__idx
    ON team_storage_usage (updated_at ASC)
 WHERE dirty_at IS NULL;

-- ---------------------------------------------------------------------
-- Audit log cursors: each consumer keeps a position over `audit_log`
-- instead of a pending queue, and the row is deleted when every enabled
-- consumer has passed it.
--
-- `seq` is the insertion order (a sequence, so it costs no extra WAL);
-- `xid` is the transaction that wrote the row. The pair gives a total
-- order that real commits respect: the consumer reads only up to
-- pg_snapshot_xmin(), the oldest live transaction, so it never steps
-- over a row whose transaction is still open. `xid8` (64 bits) does not
-- wrap, unlike the row `xmin` (32 bits); it exists since PostgreSQL 13.
-- ---------------------------------------------------------------------

CREATE SEQUENCE IF NOT EXISTS audit_log_seq_seq;

ALTER TABLE audit_log
  ADD COLUMN IF NOT EXISTS seq bigint,
  -- Fast default (no table rewrite): the rows that already exist get 0
  -- and sort before every real xid8. They are the history, and the first
  -- deploy replays them (see the cursor seed below).
  ADD COLUMN IF NOT EXISTS xid xid8 NOT NULL DEFAULT '0'::xid8;

ALTER SEQUENCE audit_log_seq_seq OWNED BY audit_log.seq;

-- Reserve the low range for the history: the new rows start above the
-- backfill, so any `ORDER BY seq` stays chronological and the backfill
-- can never collide with a value produced by nextval. The margin covers
-- the rows the previous version may insert between this count and the
-- SET DEFAULT (a few statements away).
--
-- Only the first application sets it: a sequence that already left its
-- initial value (1) is reserved by the operator or in use, and a re-run
-- must leave it alone (moving it back would reuse values above the
-- history and break the reserve).
SELECT setval('audit_log_seq_seq',
              (SELECT count(*) + 1000000 FROM audit_log),
              false)
 WHERE (SELECT last_value FROM audit_log_seq_seq) = 1;

-- From here no new row can be NULL: the previous version keeps inserting
-- during the rollout and must leave the columns filled.
ALTER TABLE audit_log
  ALTER COLUMN seq SET DEFAULT nextval('audit_log_seq_seq'),
  ALTER COLUMN xid SET DEFAULT pg_current_xact_id();

-- Backfill the history with 1..N, in the order the previous version sent
-- it (`created_at, id`, the index it ordered by), so the replay keeps the
-- archive order. A single statement: it takes row locks, never a table
-- lock, so the previous version keeps inserting and the statement can be
-- interrupted and re-run (it filters `seq IS NULL`).
UPDATE audit_log
   SET seq = ordered.rn
  FROM (SELECT id, row_number() OVER (ORDER BY created_at, id) AS rn
          FROM audit_log
         WHERE seq IS NULL) ordered
 WHERE audit_log.id = ordered.id;

ALTER TABLE audit_log
  ALTER COLUMN seq SET NOT NULL;

-- Covers both predicates of the scan: the cursor comparison and the
-- snapshot cut are Index Cond, so the index serves the range and the
-- (xid, seq) order of the scan.
CREATE UNIQUE INDEX IF NOT EXISTS audit_log__xid__seq__idx
    ON audit_log (xid, seq);

-- One row per consumer: its position, updated in the same transaction
-- that sends the chunk. Consumer ids live in app.loggers.audit.consumers
-- and are never reused: 1 = nexus, 2 = admin console, 3 = team storage.
CREATE TABLE IF NOT EXISTS audit_log_consumer_cursor (
  consumer   smallint    NOT NULL,
  xid        xid8        NOT NULL,
  seq        bigint      NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer));

-- The two consumers that already existed start at the beginning of the
-- buffer, so the first deploy **replays** the history instead of dropping
-- it: the old archiver is disabled and the already-archived rows are
-- deleted before deploying (see the NOTE), so the buffer holds only the
-- backlog that was never archived. Team storage (id 3) is new and is not
-- seeded: `ensure-cursor` starts it at the end, and its cache is born
-- lazy. ON CONFLICT keeps the position of a consumer that already ran.
INSERT INTO audit_log_consumer_cursor (consumer, xid, seq)
VALUES (1, '0'::xid8, 0),
       (2, '0'::xid8, 0)
ON CONFLICT (consumer) DO NOTHING;

-- ---------------------------------------------------------------------
-- The old pending queue stays one more release: the previous code
-- version keeps inserting into it during a rolling upgrade, and dropping
-- it while that version runs reverts every audited action (the producer
-- inserts the pending in the same transaction as the event).
--
-- What goes now is its foreign key to `audit_log`: after the rollout
-- nobody drains the queue, so a pending row left behind by the window
-- would make the new GC fail on every pass when it deletes the row it
-- references. Without the FK those leftovers are inert.
-- ---------------------------------------------------------------------

ALTER TABLE IF EXISTS audit_log_pending
    DROP CONSTRAINT IF EXISTS audit_log_pending_audit_log_id_fkey;

-- ---------------------------------------------------------------------
-- Jobs: at most one live exclusive job and at most one not-started
-- dedupe job per (tenant, name, queue, label).
--
-- `exclusive` marks the row of a `::jobs/skip-if-active` submit, and the
-- index admits a single live row (new, scheduled, running or retry) for
-- its key. `dedupe` marks a `::jobs/dedupe`/`::jobs/debounce` submit, and
-- its index admits a single `new` row. The submit no longer counts or
-- deletes before inserting: the partial unique index decides and the
-- insert resolves the conflict with `ON CONFLICT`, so the exclusion holds
-- across backends.
--
-- Both columns default to false, so the rows that never asked for the
-- option stay out of the indexes (the label stays a label) and the
-- indexes start empty: no cleanup of duplicates is needed.
-- ---------------------------------------------------------------------

ALTER TABLE job
    ADD COLUMN IF NOT EXISTS exclusive boolean NOT NULL DEFAULT false;

ALTER TABLE job
    ADD COLUMN IF NOT EXISTS dedupe boolean NOT NULL DEFAULT false;

CREATE UNIQUE INDEX IF NOT EXISTS job__live_exclusive__uniq
    ON job (tenant, name, queue, label)
    WHERE exclusive
      AND status IN ('new', 'scheduled', 'running', 'retry');

CREATE UNIQUE INDEX IF NOT EXISTS job__new_dedupe__uniq
    ON job (tenant, name, queue, label)
    WHERE dedupe
      AND status = 'new';

-- The lost sweep looks for `scheduled` rows by their hand-off time
-- (`modified_at`), not by their due time: the old index served the removed
-- reschedule and only narrows by tenant now. The `job` table is not in
-- production yet, so the rebuild does not need CONCURRENTLY.
DROP INDEX IF EXISTS job__scheduled__idx;

CREATE INDEX IF NOT EXISTS job__scheduled__idx
    ON job (tenant, modified_at)
 WHERE status = 'scheduled';

-- ---------------------------------------------------------------------
-- NOTE: on an instance with a large `job` or `audit_log` table, apply this
-- migration out of band before deploying. The runner executes all pending
-- steps in a single transaction, so the plain indexes below hold a SHARE
-- lock on the table for the whole build and every ALTER keeps its ACCESS
-- EXCLUSIVE until that one commit; on tables that keep taking writes (every
-- file save inserts a job and an audit row) that is minutes of stalled
-- writes. Pre-apply the same statements on their own, outside a transaction
-- block, in this order.
--
-- 0. Before anything: stop the legacy archiving **and its GC** for the
--    whole deploy window, so nothing archives or deletes concurrently. The
--    consumer flags are the same in both versions, so flipping them is not
--    enough: stop the old version's workers/cron (or take those instances
--    out) and check that no legacy consumer job and no `audit-log-gc` job
--    is pending before going on. The old sweeps delete rows the new
--    consumers may not have read yet (the new producer no longer fills
--    `audit_log_pending`, so the old orphan sweep sees every new row as an
--    orphan) and the old consumers re-send rows the new cursors already
--    passed; there is no lock that serializes the two versions.
--
--    Then delete what the old archiver had already archived. On the
--    mark-and-sweep version (the one without the pending queue) that is:
--
--      DELETE FROM audit_log WHERE archived_at IS NOT NULL;
--
--    On the version with the pending queue there is nothing to delete (the
--    last consumer deletes the row) and the duplicate window of the replay
--    is bounded to the rows whose destination already got them. After this
--    step the buffer is only the backlog that was never archived, so the
--    replay seeded below does not re-send anything already sent.
--
-- 1. Team storage: a new table (copy of the block above, both indexes
--    included: the one on `dirty_at` and the partial one on `updated_at`
--    that serves the weekly self-healing sweep). The `REFERENCES team(id)`
--    takes SHARE ROW EXCLUSIVE on `team`, which blocks team writes until
--    the commit: one more reason to apply this step out of band, and
--    before the rest of the migration.
--
-- 2. Audit log: columns first (metadata-only), the reserved low range and
--    the defaults the previous version needs immediately:
--
--      CREATE SEQUENCE IF NOT EXISTS audit_log_seq_seq;
--      ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS seq bigint;
--      ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS xid xid8 NOT NULL DEFAULT '0'::xid8;
--      ALTER SEQUENCE audit_log_seq_seq OWNED BY audit_log.seq;
--      SELECT setval('audit_log_seq_seq',
--                    (SELECT count(*) + 1000000 FROM audit_log), false)
--       WHERE (SELECT last_value FROM audit_log_seq_seq) = 1;
--      ALTER TABLE audit_log ALTER COLUMN seq SET DEFAULT nextval('audit_log_seq_seq');
--      ALTER TABLE audit_log ALTER COLUMN xid SET DEFAULT pg_current_xact_id();
--
--    Then the backfill: run the `UPDATE ... row_number()` statement from
--    the body of this migration as-is (it is one statement, it takes row
--    locks only, and it filters `seq IS NULL`, so re-running it is safe).
--
-- 3. Audit log: `NOT NULL` without a long ACCESS EXCLUSIVE scan. No row
--    can be NULL here: the default was set in step 2.
--
--      ALTER TABLE audit_log ADD CONSTRAINT audit_log__seq__not_null
--          CHECK (seq IS NOT NULL) NOT VALID;
--      ALTER TABLE audit_log VALIDATE CONSTRAINT audit_log__seq__not_null;
--      ALTER TABLE audit_log ALTER COLUMN seq SET NOT NULL;
--      ALTER TABLE audit_log DROP CONSTRAINT audit_log__seq__not_null;
--
-- 4. Audit log: the cursor index (CONCURRENTLY) and the cursor table with
--    the seed of consumers 1 and 2 at (0, 0) (copies of the blocks above,
--    the index with CONCURRENTLY):
--
--      CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS audit_log__xid__seq__idx
--          ON audit_log (xid, seq);
--
-- 5. Old queue: keep the table, drop its FK (copy of the block above).
--
-- 6. Jobs: the two columns and the two unique indexes, the indexes with
--    CONCURRENTLY:
--
--      ALTER TABLE job ADD COLUMN IF NOT EXISTS exclusive boolean NOT NULL DEFAULT false;
--      ALTER TABLE job ADD COLUMN IF NOT EXISTS dedupe    boolean NOT NULL DEFAULT false;
--      CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS job__live_exclusive__uniq
--          ON job (tenant, name, queue, label)
--          WHERE exclusive AND status IN ('new', 'scheduled', 'running', 'retry');
--      CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS job__new_dedupe__uniq
--          ON job (tenant, name, queue, label)
--          WHERE dedupe AND status = 'new';
--
--    The lost sweep index changes shape: it filters by `modified_at`, not
--    by `scheduled_at`. `job` is not in production yet, so a plain rebuild
--    is enough:
--
--      DROP INDEX IF EXISTS job__scheduled__idx;
--      CREATE INDEX IF NOT EXISTS job__scheduled__idx
--          ON job (tenant, modified_at)
--       WHERE status = 'scheduled';
--
-- Lock hygiene: every ALTER waits for ACCESS EXCLUSIVE, so run each one with
-- `SET lock_timeout = '5s'` and retry instead of letting it queue behind a
-- long transaction. An interrupted CONCURRENTLY build leaves the index
-- INVALID, and IF NOT EXISTS matches by name, so this migration would accept
-- it and the dedupe/exclusivity would stop enforcing silently. Treat the
-- checks below as a gate before deploying:
--
--   SELECT indexname, indexdef FROM pg_indexes
--    WHERE indexname IN ('job__live_exclusive__uniq',
--                        'job__new_dedupe__uniq',
--                        'audit_log__xid__seq__idx');
--   SELECT indexrelid::regclass AS index, indisvalid, indisunique
--     FROM pg_index
--    WHERE indexrelid IN ('job__live_exclusive__uniq'::regclass,
--                         'job__new_dedupe__uniq'::regclass,
--                         'audit_log__xid__seq__idx'::regclass);
--   SELECT count(*) FROM audit_log WHERE seq IS NULL OR xid IS NULL;
--
-- Any index with indisvalid = false must be dropped and rebuilt before the
-- deploy, and the last count must be 0.
--
-- The old queue itself is dropped after the rollout, once every instance
-- runs this version (until then the previous code still writes to it):
--
--   DROP TABLE IF EXISTS audit_log_pending;
--
-- A follow-up release ships that DROP; doing it by hand before is safe.
