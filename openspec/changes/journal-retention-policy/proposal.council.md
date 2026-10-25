# Council Notes: proposal.md

## Author Summary

The draft captured the v1 full-journal tradeoff and proposed a next-version retention/archival policy: time partitioning, an external archive tier, a permanent `tx_id` tombstone registry, bounded idempotency records, job/progress cleanup, legal hold, verified archive/restore, Galera-safe operations, and cross-backend tests. It framed retention as a change to storage lifecycle only, not ledger semantics.

## Reviewer Challenges

- The “journal stays bounded” claim was false: the tombstone registry is still one permanent row per transaction, and a partitioned table cannot enforce global `tx_id` uniqueness without including the partition key.
- Dedupe reclamation was presented as free, but same-key tombstones do not reduce row count; a file epoch changes replay behavior, and the horizon must include Kafka retention/offset reset and backup restore points.
- The modified-capability list referenced `distributed-ledger`, `database-portability`, and `distributed-operations`, which are only active deltas in the unarchived v1 change, not existing main specs. `stream-ingestion` and `job-reporting` were also affected but missing.
- Open disputed rows and held funds could be archived or dropped, stranding obligations.
- Archive-backed dispute lookup would break v1’s one-database-transaction atomicity and needs an archived-vs-unknown distinction.
- “Reversible” was inaccurate for a dropped partition.
- Archive tables inside the primary DB would not deliver the stated backup/SST benefit.
- The proposal duplicated the `BREAKING` marker and drifted into design/tasks detail.
- Client/account/identity retention was not excluded even though those tables still grow.
- Legal hold did not explicitly reach archived data.
- The future change should not be created as an active change before v1 archives.

## Resolutions

- Accepted: the hot primary set is bounded; total historical storage is not. The registry is the permanent uniqueness authority and the residual narrow unbounded table. Registry growth is an explicit open question.
- Accepted: partitioning fixes purge/backup/scan cost, not uniqueness cardinality.
- Accepted: idempotency reclamation is a tradeoff between same-key tombstones (no row reduction) and a file epoch (replay beyond the horizon must be rejected with a defined outcome). The horizon is the maximum of file replay, Kafka retention/offset reset, and backup restore point.
- Accepted: only terminal `settled`/`charged_back` rows are eligible; `disputed` rows and open holds are pinned.
- Accepted: the recommended dispute policy is a defined dispute window; archive reads are audit/reporting only, preserving the v1 transaction boundary. Any archive read inside a dispute is an explicit relaxation with an archived-vs-unknown marker.
- Accepted: the archive tier must sit outside the primary logical DB and carries its own backup/durability obligation.
- Accepted: verify-before-drop is an invariant; a failed archive never drops its source.
- Accepted: backend-specific DDL/flow-control procedures; no single portable recipe.
- Accepted: one `BREAKING` marker, scoped to primary-DB indefinite history; ledger and CLI behavior unchanged.
- Accepted: `Modified Capabilities` is `None` for now; the v1 capability amendments are listed outside the section and deferred until v1 archives.
- Accepted: client/account/identity retention is out of scope; those tables still grow.
- Accepted: legal hold survives archival, blocks archive expiry, and is queryable.
- Accepted: the proposal is proposal-only and not apply-ready until v1 is archived and this change is scheduled.
- Deferred: retention duration, archive format/storage, partition granularity, registry growth strategy, dispute window/SLA, replay-epoch behavior, job re-export lifetime, Galera procedure, restore/query tooling, PII scope, and the scheduling trigger.

## Remaining Risks

- The registry remains unbounded; total storage is not bounded by this change.
- A wrong registry or replay-epoch design can allow ID reuse or double application.
- Count-only archive verification can lose or corrupt detail.
- A mistimed partition drop during replication lag can cause data loss or cluster-wide stalls.
- The future change cannot be applied until `kafka-distributed-ledger` v1 is implemented, populated, and archived.
