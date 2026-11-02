;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS SUBSIDIARY SL

(ns app.worker.dispatcher
  (:require
   [app.common.data :as d]
   [app.common.json :as json]
   [app.common.logging :as l]
   [app.common.schema :as sm]
   [app.common.time :as ct]
   [app.config :as cf]
   [app.db :as db]
   [app.jobs :as jobs]
   [app.jobs.metrics :as metrics]
   [app.metrics :as mtx]
   [app.redis :as rds]
   [app.worker :as wrk]
   [integrant.core :as ig]
   [promesa.exec :as px])
  (:import
   java.lang.AutoCloseable))

(set! *warn-on-reflection* true)

(def ^:private schema:dispatcher
  [:map
   [::wrk/tenant ::sm/text]
   [::lease {:optional true} ::ct/duration]
   ::mtx/metrics
   ::db/pool
   ::rds/client])

(defmethod ig/expand-key ::wrk/dispatcher
  [k v]
  {k (-> (d/without-nils v)
         (assoc ::timeout (ct/duration "10s"))
         (assoc ::batch-size 100)
         (assoc ::wait-duration (ct/duration "5s"))
         (assoc ::lease (cf/get-jobs-lease)))})

(defmethod ig/assert-key ::wrk/dispatcher
  [_ cfg]
  (assert (sm/check schema:dispatcher cfg)))

(def ^:private sql:select-next-jobs
  "SELECT id, name, queue, scheduled_at from job AS t
    WHERE t.scheduled_at <= ?::timestamptz
      AND (t.status = 'new' OR t.status = 'retry')
      AND t.tenant = ?
    ORDER BY t.priority DESC, t.scheduled_at
    LIMIT ?
      FOR UPDATE
     SKIP LOCKED")

(def ^:private sql:mark-job-scheduled
  "UPDATE job SET status = 'scheduled', modified_at = ?
    WHERE id = ANY(?)")

;; The sweeps are bulk UPDATEs that run before every dispatch batch, so
;; they are the queries where a missing tenant filter does the most
;; damage: without it any instance with a worker requeues another
;; instance's scheduled rows and, worse, aborts its running jobs once
;; their lease expires.

;; A deduped job that already has a newer `new` row with its key cannot
;; go back to `new`: that would put two rows in `job__new_dedupe__uniq`
;; and abort the batch. `abort-superseded` resolves it; the guard in
;; `reschedule-lost` covers the race with a submit that lands between the
;; two statements.
(def ^:private sql:reschedule-lost
  "UPDATE job
      SET status='new', scheduled_at=?::timestamptz
     FROM (SELECT t.id
             FROM job AS t
            WHERE status = 'scheduled'
              AND t.tenant = ?
              AND t.scheduled_at < ?::timestamptz - '5 min'::interval
              AND NOT (t.dedupe AND EXISTS (
                    SELECT 1 FROM job AS d
                     WHERE d.tenant = t.tenant
                       AND d.name = t.name
                       AND d.queue = t.queue
                       AND d.label = t.label
                       AND d.dedupe
                       AND d.status = 'new'))) AS subquery
    WHERE job.id=subquery.id
RETURNING job.id, job.name, job.queue")

(def ^:private sql:abort-superseded
  "UPDATE job
      SET status='aborted', modified_at=?::timestamptz, error=?::jsonb
     FROM (SELECT t.id
             FROM job AS t
            WHERE t.status = 'scheduled'
              AND t.dedupe
              AND t.tenant = ?
              AND t.scheduled_at < ?::timestamptz - '5 min'::interval
              AND EXISTS (SELECT 1 FROM job AS d
                           WHERE d.tenant = t.tenant
                             AND d.name = t.name
                             AND d.queue = t.queue
                             AND d.label = t.label
                             AND d.dedupe
                             AND d.status = 'new')) AS subquery
    WHERE job.id=subquery.id
RETURNING job.id, job.queue, job.name, job.created_at")

(def ^:private sql:mark-orphan
  "UPDATE job
      SET status='aborted', modified_at=?::timestamptz, error=?::jsonb
     FROM (SELECT t.id
             FROM job AS t
            WHERE status = 'running'
              AND t.tenant = ?
              AND t.modified_at < ?::timestamptz) AS subquery
    WHERE job.id=subquery.id
RETURNING job.id, job.queue, job.name, job.created_at")

;; the error travels as text and PostgreSQL casts it: a PGobject parameter
;; on a `::jsonb` placeholder is not what the driver expects here. Same
;; for the orphan `end` event payload below.
(def ^:private orphan-error-json
  (json/encode jobs/orphan-error))

(def ^:private superseded-error-json
  (json/encode jobs/superseded-error))

(def ^:private aborted-outcome-json
  (json/encode {:outcome "aborted"}))

(def ^:private sql:insert-aborted-events
  "INSERT INTO job_event (job_id, kind, payload)
   SELECT unnest(?::uuid[]), 'end', ?::jsonb")

(defn- encode-payload
  [{:keys [id scheduled-at]}]
  (json/encode [(str id) (ct/format-inst scheduled-at)]))

(defn- abort-superseded-jobs
  "Abort the deduped jobs whose message was lost and that already have a
  newer `new` row with the same key. The newer row carries the latest
  params, so it is the one that does the work; sending the lost one back
  to `new` would put two rows in `job__new_dedupe__uniq` and abort the
  batch. The abort owns its `end` events, like the orphan sweep."
  [{:keys [::db/conn ::timestamp ::wrk/tenant] :as cfg}]
  (let [rows (db/exec! conn [sql:abort-superseded timestamp superseded-error-json
                             tenant timestamp]
                       {:return-keys true})]
    (when (seq rows)
      (db/exec-one! conn [sql:insert-aborted-events
                          (db/create-array conn "uuid" (map :id rows))
                          aborted-outcome-json]))
    (db/after-commit
     (fn []
       (doseq [{:keys [id queue name created-at]} rows]
         (metrics/record-total cfg name :aborted
                               (- (inst-ms timestamp) (inst-ms created-at)))
         (l/wrn :hint "marked job as aborted (superseded by a newer submit)"
                :id (str id)
                :tenant tenant
                :queue queue))))))

(defn- reschedule-lost-jobs
  [{:keys [::db/conn ::timestamp ::wrk/tenant] :as cfg}]
  (let [rows (db/exec! conn [sql:reschedule-lost timestamp tenant timestamp]
                       {:return-keys true})]
    (db/after-commit
     (fn []
       (doseq [{:keys [id name queue]} rows]
         (metrics/record-rescheduled cfg name)
         (l/wrn :hint "reschedule"
                :name name
                :id (str id)
                :tenant tenant
                :queue queue))))))

(defn- mark-orphan-jobs
  [{:keys [::db/conn ::timestamp ::lease ::wrk/tenant] :as cfg}]
  (let [cutoff (ct/minus timestamp (or lease (cf/get-jobs-lease)))
        rows    (db/exec! conn [sql:mark-orphan timestamp orphan-error-json
                                tenant cutoff]
                          {:return-keys true})]
    ;; The sweep is the only status change outside `app.jobs`, so it owns
    ;; its `end` events too: one bulk insert in the same transaction (never
    ;; one insert per orphan) keeps the history consistent with the rows.
    ;; No msgbus notification: orphans alert through the error log and the
    ;; `aborted` outcome of `penpot_jobs_total_ms`, whose `_count` is the
    ;; orphan count.
    (when (seq rows)
      (db/exec-one! conn [sql:insert-aborted-events
                          (db/create-array conn "uuid" (map :id rows))
                          aborted-outcome-json]))
    (db/after-commit
     (fn []
       (doseq [{:keys [id queue name created-at]} rows]
         (metrics/record-total cfg name :aborted
                               (- (inst-ms timestamp) (inst-ms created-at)))
         (l/err :hint "marked job as aborted (orphan lease expired)"
                :id (str id)
                :tenant tenant
                :queue queue))))))

(defn- get-jobs
  [{:keys [::db/conn ::timestamp ::batch-size ::wrk/tenant]}]
  (let [result (db/exec! conn [sql:select-next-jobs timestamp tenant batch-size])]
    (not-empty result)))

(defn- mark-as-scheduled
  [{:keys [::db/conn]} items]
  (let [ids (map :id items)
        sql [sql:mark-job-scheduled
             (ct/now)
             (db/create-array conn "uuid" ids)]]
    (db/exec-one! conn sql)))

(defn- push-jobs
  [{:keys [::rds/conn ::wrk/tenant] :as cfg} [queue jobs]]
  ;; Mark first, push last: a crash between mark and push leaves a
  ;; `scheduled` row that reschedule-lost-jobs re-queues, while the
  ;; reverse order leaves a duplicate payload nothing deduplicates.
  (mark-as-scheduled cfg jobs)
  (let [items (mapv encode-payload jobs)
        key   (wrk/queue-key tenant queue)]

    (rds/rpush conn key items)
    (metrics/record-dispatcher-size cfg queue (count jobs))
    (doseq [{:keys [id name queue]} jobs]
      (metrics/record-dispatched cfg name 1)
      (l/trc :hist "schedule"
             :tenant tenant
             :queue queue
             :job-id (str id)))))

(defn- run-batch'
  [cfg]
  (let [cfg    (assoc cfg ::timestamp (ct/now))
        tpoint (ct/tpoint)]
    ;; The lost deduped jobs that already have a newer pending row are
    ;; aborted first: they cannot go back to `new` without breaking the
    ;; dedupe index, and the newer row is the one that does the work.
    (abort-superseded-jobs cfg)

    ;; Reschedule lost in transit jobs (can happen when
    ;; redis server is restarted just after job is pushed)
    (reschedule-lost-jobs cfg)

    ;; Mark as aborted all jobs that are still marked as running but
    ;; their last modification (heartbeat or progress) is older than
    ;; the configured lease
    (mark-orphan-jobs cfg)

    ;; Then, schedule the next jobs in queue
    (let [result (if-let [jobs (get-jobs cfg)]
                   (do
                     (->> (group-by :queue jobs)
                          (run! (partial push-jobs cfg)))
                     nil)

                   ;; If no jobs found on this batch run, we signal the
                   ;; run-loop to wait for some time before start running
                   ;; the next batch iteration
                   ::wait)]
      (db/after-commit
       #(metrics/record-dispatcher-batch cfg :dispatch :completed (inst-ms (tpoint))))
      result)))

(defn- sleep-after-error
  [cfg]
  (px/sleep (or (::timeout cfg) (ct/duration "10s"))))

(defn run-batch
  "Execute a single dispatch batch: reschedule lost jobs, mark orphans
  (lease-based) and claim pending jobs into their Redis queues. Exposed
  as a function for testability; the dispatcher thread loops on it."
  [cfg]
  (let [tpoint (ct/tpoint)
        tenant (::wrk/tenant cfg)]
    (try
      (let [rconn (rds/connect cfg)]
        (try
          (-> cfg
              (assoc ::rds/conn rconn)
              (db/tx-run! run-batch'))
          (finally
            (.close ^AutoCloseable rconn))))
      (catch InterruptedException cause
        (throw cause))

      (catch Exception cause
        (cond
          (rds/exception? cause)
          (do
            (metrics/record-dispatcher-batch cfg :redis :failed (inst-ms (tpoint)))
            (l/wrn :hint "redis exception (will retry in an instant)" :tenant tenant :cause cause)
            (sleep-after-error cfg))

          (db/sql-exception? cause)
          (do
            (metrics/record-dispatcher-batch cfg :database :failed (inst-ms (tpoint)))
            (l/wrn :hint "database exception (will retry in an instant)" :tenant tenant :cause cause)
            (sleep-after-error cfg))

          :else
          (do
            (metrics/record-dispatcher-batch cfg :execution :failed (inst-ms (tpoint)))
            (l/err :hint "unhandled exception (will retry in an instant)" :tenant tenant :cause cause)
            (sleep-after-error cfg)))))))

(defmethod ig/init-key ::wrk/dispatcher
  [_ {:keys [::db/pool ::wait-duration ::wrk/tenant] :as cfg}]
  (letfn [(dispatcher []
            (l/inf :hint "started" :tenant tenant)
            (try
              (loop []
                (let [result (run-batch cfg)]
                  (when (= result ::wait)
                    (px/sleep wait-duration))
                  (recur)))
              (catch InterruptedException _
                (l/trc :hint "interrupted" :tenant tenant))
              (catch Throwable cause
                (l/err :hint "unexpected exception" :tenant tenant :cause cause))
              (finally
                (l/inf :hint "terminated" :tenant tenant))))]

    (if (db/read-only? pool)
      (l/wrn :hint "not started (db is read-only)" :tenant tenant)
      (px/fn->thread dispatcher :name "penpot/worker-dispatcher"))))

(defmethod ig/halt-key! ::wrk/dispatcher
  [_ thread]
  (some-> thread px/interrupt!))
