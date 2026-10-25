## Why

`kafka-distributed-ledger` v1 deliberately keeps the full terminal journal and content-addressed idempotency records. The hot primary database therefore grows with every accepted transaction and applied event, increasing storage, index size, backup/restore time, Galera state transfer, migration time, and scan/maintenance cost. The primary hot set should become bounded by a retention window; total historical storage cannot be bounded because a permanent `tx_id` registry must survive purging.

This is a next-version change, sequenced after v1 is implemented, populated, and operating. It is not folded into v1 because retention needs real volume and must not mix storage lifecycle with ledger semantics. It should be scheduled when a stated trigger is crossed, such as journal row count, primary size, backup duration, or Galera SST duration.

Four guarantees must survive deletion:

- **Permanent global `tx_id` uniqueness.** A purged ID must never be reusable; the registry, not the journal row, becomes the uniqueness authority.
- **Dispute and resolve lookup.** Archived detail remains reachable, or a dispute window is explicitly defined and enforced. A dispute must never silently become an unclassified “not found”.
- **Re-ingestion idempotency.** Dedupe records must outlive the replay horizon: the maximum of the file replay window, Kafka topic retention/offset reset, and any backup restore point that could reintroduce older events.
- **Audit, legal hold, and data minimization.** Retention is configurable and auditable; a legal hold reaches archived data and blocks expiry; archived payloads contain only what the policy requires.

## What Changes

- **Partitioned journal and an external archive tier.** The journal is partitioned by age on both backends; recent data stays in the primary, older data moves outside the primary logical database to archive storage behind a catalog and query path. Partitions make purge cost bounded; an archive is verified by counts and checksums before any drop. A failed or interrupted archive never drops its source.
- **A permanent, narrow `tx_id` registry/tombstone.** `tx_id`, owner, terminal state, amount where disputes need it, and `archived` status survive every purge. This preserves global uniqueness and duplicate/owner detection after the journal row is gone. The registry is still one permanent row per transaction: it reduces row width and primary working-set cost, not total cardinality. Registry growth is an explicit open question (narrow unbounded table vs. hash/range partitioning).
- **Only terminal rows are eligible for retention.** `settled` and `charged_back` rows may leave the hot window; `disputed` rows and any row backing an open hold are pinned until resolved. A dispute window that outlives the settled-row window is invalid unless disputes explicitly use an archive lookup.
- **Dispute lookup is a stated policy.** The recommended policy is a defined dispute window: disputes read the primary or the registry while the row is hot, and archive reads are for audit/reporting only, preserving v1’s one-database-transaction atomicity. If archive reads inside a dispute are chosen, that is an explicit relaxation of the v1 transaction boundary and must handle archive unavailability and the archived-vs-unknown race.
- **Idempotency retention is a tradeoff, not free reclamation.** Replacing content-addressed dedupe rows with the same-key tombstones reclaims payload width, not rows. Reclaiming rows requires a file-level epoch, which changes behavior: replaying a file older than the epoch must be rejected with a defined row/job outcome instead of the current silent no-op. If unbounded idempotent replay is kept, the dedupe table remains an unbounded narrow table and is treated like the registry.
- **Job and progress lifecycle.** `jobs`, `job_files`, `job_events`, `job_progress`, and `job_clients` get a bounded lifecycle. Purging a completed job defines the lifetime of its re-export and completed-manifest replay contract; live-job rules are unchanged.
- **Configurable retention and legal hold.** Windows are configuration, not constants. A legal hold can be applied after archival, blocks archive-tier expiry, and is queryable against archived data. Archived payloads follow stated PII/data-minimization rules.
- **Backend-specific operations.** PostgreSQL declarative partitions and MariaDB/Galera range partitions have different DDL hazards; `ATTACH`/`DROP`/flow control are sequenced per backend, not with one portable recipe. Archive moves and DDL are throttled so replication stays healthy.
- **Verification, monitoring, and restore.** Every archive is verified before drop; archive lag, verification mismatches, partition-drop failures, restore drills, and retention compliance are observable and have a runbook. Restore/query tooling makes archived data usable.
- **Tests:** no ID reuse after purge, dispute lookup before/after archival, post-purge replay behavior, open-hold pinning, archive/restore verification, and both database backends.
- **BREAKING** for operators who treat the primary database as an indefinite historical archive: after the hot window, detail moves behind the archive path or expires per policy. Ledger rules, terminal states, dispute outcomes, the active working set, and the in-memory CLI are unchanged.
- **Out of scope:** changing ledger semantics; deleting active/current data; implementing this in v1; DB sharding; cross-client transfers; the payment gateway; and retention of `clients`, `accounts`, or `id_allocator`, whose permanent identity rows are not covered by this policy.

## Capabilities

### New Capabilities

- `journal-retention` — Retention and archival for the distributed journal and supporting tables: configurable hot/archive windows, time-based partitioning, archive catalog and external tier, the permanent `tx_id` registry, terminal-only eligibility and open-hold pinning, dispute window/archive-read policy, replay-horizon idempotency retention, job/progress lifecycle, legal hold, verified archive/restore, and compliance evidence.

### Modified Capabilities

None yet. The capabilities this change will amend — `distributed-ledger`, `stream-ingestion`, `job-reporting`, `database-portability`, and `distributed-operations` — are introduced by the still-active `kafka-distributed-ledger` change and are not in the main specs until that change archives. Their amendments are deferred to this change’s later artifacts, after v1 is archived.

## Impact

- **Schema:** partitioned journal, external archive layout/catalog, `tx_id` registry with `archived`/amount fields as needed, dedupe epoch or tombstone bookkeeping, retention metadata for job tables.
- **Code:** retention/archive workers, registry-aware duplicate and `NotOwner` lookups, dispute-window enforcement, configurable retention/legal hold, resumable migration/backfill, archive query/restore tooling.
- **Data:** backfill of existing rows on both backends; idempotent, resumable operations; no source drop before verification.
- **Systems:** Galera flow-control and backend-specific DDL procedures, archive storage and catalog dependencies, separate archive backup/durability, replication impact, new metrics/alerts/runbook.
- **Compliance:** retention-duration basis, legal hold, PII/data minimization, auditable purge evidence.
- **Tests:** no-ID-reuse, post-archive dispute behavior, post-purge replay outcome, open-hold pinning, archive/restore correctness, both backends.
- **Dependency:** requires `kafka-distributed-ledger` v1 to be implemented, populated, and operating; this change cannot be applied to an empty schema.
- **Risks:** an incorrect registry or replay-epoch design can allow ID reuse or double application; count-only archive verification can lose detail; a mistimed partition drop during replication lag can cause data loss or cluster-wide stalls; the registry remains unbounded even when the journal hot set is bounded.

**Open questions for design/tasks:** legal retention duration; archive format and storage target; partition granularity; exact registry fields and growth strategy; dispute window/SLA vs. archive reads; file replay window and replay-beyond-epoch behavior; Kafka retention/offset-reset/restore horizon; job re-export lifetime; Galera DDL/flow-control procedure; restore/query tooling; PII/data-minimization scope; and the operational trigger that schedules this change.
