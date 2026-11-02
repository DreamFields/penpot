# Production infrastructure (services Penpot depends on)

Backend (`app.config`, `PENPOT_*` env vars) is parameterized; deployments choose providers.

## Services

- **PostgreSQL**: durable store: profiles, teams, files, sessions, audit, the jobs work queue, the cron registry and migrations. File data also lives here depending on the file-data backend. One shared DB across all backends.
- **Redis (Valkey-compatible)**: per-backend message bus and cache: collaborative-editing broadcasts and team/profile notifications, the file-summary cache, rate-limit counters and the dispatcher→runner hand-off. `PENPOT_REDIS_URI`. A burst of client-side `RedisCommandTimeoutException` with the exporter healthy is the JVM paused or starved, not Redis: the timeout fires because nothing reads the reply.
- **Object storage**: backends `:s3` and `:fs`. S3 in prod; devenv uses RustFS. Holds uploaded media, file-data when the file-data backend is `storage`, exports. Backend-side details (resolve, dedup, bucket set, object lifecycle, and file-data backends): `mem:backend/storage`.
- **SMTP mailer**: invitations, password resets, email verification (sent via the `:sendmail` worker job).
- **LDAP** (optional auth provider): helpers in `app.auth.*`, gated by `enable-login-with-ldap`.

## Task queue and worker model

Async jobs are enqueued via `jobs/submit` (`app.jobs`), which inserts a row into the shared Postgres `job` table tagged with its tenant and queue. Submission is **fire-and-forget**: RPC handlers never poll and never wait. The completion signal is the job row's status, which the creation commands read back to answer with the state of the job they just created. A job submitted with an owner profile publishes its progress on that profile's msgbus topic after each transaction commits; the binfile commands are the callers that pass the owner. Soft-delete RPCs return immediately after marking the top-level row, leaving the cascade and reaping to workers.

Workers run on backends with `enable-backend-worker` in `PENPOT_FLAGS`. Each worker-enabled backend has a dispatcher that claims jobs and hands them to **its own** Redis list, and one or more runners per queue that execute them and update the Postgres row. The hand-off is purely intra-backend; cross-backend coordination happens at the Postgres row level. The queues are `default`, `webhooks`, `cron`, `binfile` and `storage`: `binfile` carries the heavy work of a user job and `storage` the per-team media recompute, so neither a long export/import nor the recompute backlog blocks the default runner that serves the delete cascades.

## Cross-backend safety

Postgres row locking is the only correctness primitive: job claims, cron firing and the job-handler-internal locks (for example, the file-gc scheduler locks its candidate rows) all rely on it. This makes the work-claim path safe across any number of worker-enabled backends.

Two known race patterns survive multi-backend operation:

- **Cron dedup is best-effort.** The lock on `scheduled_task` is released when the task body finishes. If two backends' cron timers fire for the same scheduled instant with a gap larger than the task body's runtime, both execute it. Penpot's cron entries are idempotent; the exception is `:telemetry`, which would double-report. The audit consumer jobs are safe: the cursor lock serializes two runs of the same consumer, so the second takes the next chunk instead of re-sending.
- **`jobs/submit ::dedupe` / `::debounce`** are not affected by it: the row is inserted as deduped and a partial unique index (`status = 'new'`) admits a single not-started row per tenant/name/queue/label. The insert resolves the conflict with `ON CONFLICT DO NOTHING` (skip) or `DO UPDATE` (refresh the schedule and the params of the pending row), so there is no delete before the insert and no cross-backend duplicate. Neither gives "at most one execution": a row that already left `new` (dispatched, running, retrying) does not block the next submit.
- **`jobs/submit ::skip-if-active true`** works the same way one level up: the row is inserted as exclusive and a partial unique index admits a single live row (`new`/`scheduled`/`running`/`retry`) per tenant/name/queue/label, resolved with `ON CONFLICT DO NOTHING`. What it does not give is "at most one execution": the row can go terminal while its work is still running (a retry, an `aborted` orphan), so the work itself must be claimed on the data it touches.

Penpot in production lives with both: horizontal-scale deployments accept "exactly-once" as "essentially-once for idempotent operations." Devenv parallel instances handle it by running workers only on ws0 (see `mem:devenv/core`).

## Jobs observability

Worker-enabled, writable backends publish unified jobs metrics through the normal Prometheus registry. One submit counter splits by `outcome` (`created`, `skipped`, `refreshed`), and the dispatch, retry and reschedule counters are keyed by job name; terminal outcomes are the `_count` of the total-time histogram (`completed`, `failed`, `cancelled`, `aborted`), which is also the orphan count, so there is no separate terminal or orphan counter. Histograms separate claim wait (how late the claim happened), handler execution and total job age, plus dispatcher batch timing and size. `queue` survives only on the batch size: a batch mixes job names, and every other measurement is keyed by a name that already determines its queue. SQL-backed events are recorded after the outer PostgreSQL transaction commits. There is no gauge and no sampler: the backlog and the age of the oldest pending job are one SQL query when an operator asks. All labels are bounded operational values; job IDs, profile IDs, params and error text are excluded. Read-only backends do not start the workers.

## Draining one instance

Each backend has a per-process availability flag (`app.health`, a global atom). While offline, `/readyz` answers `503 OFFLINE` without querying the database, so the load balancer removes that instance from rotation while requests in flight still finish. Operator surface: `app.srepl.main/set-offline` / `set-online`; a restart comes back online. In-flight worker jobs are safe: jobs claimed as `scheduled` return to `new` after the reschedule window and another instance picks them up.

## Touching production data

Production runs the deployed toolbox, not the devenv one:

- **No psql meta-commands there.** `scripts/psql` and `\set`, `\if`, `\copy` do not exist in production, so a data script must be **plain SQL**, copy-pasteable into any client.
- **Start from the ids, not from an inventory.** The ids come from the error or the logs. Unbounded queries over tables with tens of millions of rows (`storage_object`, `file_media_object`) do not finish in a reasonable time; bounded to an id list they are index lookups.
- **One query per reference column.** Two columns in one `IN (col_a, col_b)` lose the index and scan the whole table; split them.
- The read-only replica cuts long queries: see `mem:prod-infra/grafana`.
- A migration that builds an index runs inside the migration transaction, so it cannot use `CONCURRENTLY`, and a plain `CREATE INDEX` locks the table against writes for the whole build. On a large table (file saves write to `file_change` on every save) build the index out of band, before deploying, with `CREATE INDEX CONCURRENTLY` on its own (never inside a transaction block); the shipped migration uses `IF NOT EXISTS`, finds it and does nothing. A concurrent build interrupted midway leaves the index invalid: drop it and retry (check `pg_indexes` and `pg_index.indisvalid`). The jobs dedupe index (`job__new_dedupe__uniq`, migration `0162`) needs its `dedupe` column first, so its runbook pre-applies the whole migration out of band: the `IF NOT EXISTS` `ALTER TABLE` (metadata-only, constant default) and then the `CONCURRENTLY` index, and the migration finds both and does nothing. It still takes the no-op locks, and `IF NOT EXISTS` matches by name: an `INVALID` index from an interrupted build has to be dropped before deploying, or the dedupe stops enforcing silently.

## See also

- Devenv composition and the ws0-only worker placement: `mem:devenv/core`.
- Storage backend resolution, dedup, bucket behavior, object lifecycle, and file-data lifecycle: `mem:backend/storage`.
- Storage operation metrics (S3 API calls, logical ops, dedup, asset requests) and the logical-bucket vs physical-target distinction: `mem:backend/storage` (Metrics section).
- Grafana (datasource ids and how to use them, main dashboard, panel conventions and gotchas): `mem:prod-infra/grafana`.
