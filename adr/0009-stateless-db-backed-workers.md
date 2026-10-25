# ADR-0009: Stateless DB-Backed Workers

## Status

Accepted

## Date

2026-10-24

## Context and Problem Statement

Kafka redelivers records and reassigns partitions during rebalances. Workers must not double-apply an event, must survive a crash between the database commit and the offset commit, and must not hold authoritative account state in memory.

## Considered Options

- Commit the Kafka offset before the database write. Rejected: a database failure loses the event.
- Use Kafka transactions across Kafka and the database. Rejected: no atomic commit spans both systems without a distributed transaction.
- Keep account state in worker memory and checkpoint it. Rejected: rebalance/recovery would need a state store and replay.
- Use stateless workers with one DB transaction per event and idempotent inserts. Chosen: at-least-once delivery becomes effectively once at the ledger.

## Decision Outcome

Chosen option: "Use stateless workers with one DB transaction per event and idempotent inserts". Each record runs one database transaction that inserts a content-addressed ledger event (dedupe), inserts the job event (progress dedupe), locks the account row, applies the shared ledger core, updates the journal and job progress, and commits. The Kafka offset is committed only after the database commit. A redelivery after a crash is a no-op for the ledger and job progress.

### Consequences

- Good, because any replica can take over a partition after a rebalance.
- Good, because a crash between the database commit and the offset commit is safe.
- Good, because the database, not Kafka, is the source of truth for accounts and journal.
- Bad, because each record costs a database transaction and the database is the throughput ceiling.
- Bad, because idempotency and job-progress records grow and require a future retention policy.
- Bad, because Kafka and the database are not atomically committed; the design relies on idempotency rather than exactly-once.
- Follow-up: define retry classification, DLQ metadata, and the per-event transaction budget.
