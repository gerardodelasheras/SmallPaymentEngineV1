## Why

The repository has one executable shape: a single process that reads one CSV, keeps accounts and transaction entries in worker-local maps, prints final balances, and exits. That is the right single-node reference, but it cannot accept overlapping files from many producers, survive a restart, or scale beyond one machine. The roadmap already names Kafka and database-backed variants, and the `Account` arithmetic and transaction types are reusable, but the stateful processor and the `u16` client identity need to be generalized before a durable variant can share them.

This change adds a separate, correctness-first distributed variant. It preserves per-client order, makes the database the source of truth, and keeps a full transaction journal. It scales consumer parallelism with Kafka partitions and replicas, but write capacity remains capped by a single logical database; Galera provides availability, not sharding. The current CLI remains the single-node reference and is not replaced.

## What Changes

- **Packaging:** add the distributed variant as a separate crate/workspace member, not as a second binary in the existing package. The existing package keeps its single binary and bare `cargo run` behavior; the public submission remains the existing crate, and the distributed crate is delivered separately or excluded from the sanitized submission.
- **Shared core:** extract a pure ledger core for both variants. `Account` arithmetic is reused as-is; the state machine, identifier type, and transaction-index/journal boundary are generalized. **BREAKING** for library consumers: `ClientId`, processor internals, and the shared API change; the CLI's observable behavior and arguments do not.
- **Canonical identity:** the external key is source-scoped: `(source_id, client: u16)` maps to a DB-minted, globally unique `BIGINT` client ID. The mapping is mint-or-get and performed in the partitioner, so the Kafka key is canonical and re-ingesting the same key returns the same ID. The job report uses the canonical ID as its `client` column.
- **Manifest ingestion:** each manifest entry carries a source, file ordinal, and hash. Files are parsed in parallel; staging records carry the parsed transaction plus `(job_id, file_ord, row_ord, source_id, external_client)`. A partitioner owns each canonical client through a single staging partition, buffers until all earlier files are complete using DB-backed watermarks, and emits to `transactions` in `(file_ord, row_ord)` order, keyed by canonical ID. Duplicate ordinals, missing files, and hash mismatches are rejected or quarantined with a terminal job outcome.
- **Stable event identity:** event dedupe is content-addressed (`source_id`, `file_hash`, `row_ord`), so re-ingesting the same file under a later job is a no-op for ledger effects while still counting toward that job’s progress and touched clients.
- **Stateless workers:** workers are Kafka consumer-group members with no durable state. Per record, one DB transaction dedupes the event, locks the account, applies the existing ledger rules, updates the transaction index, and updates job progress; the Kafka offset is committed only after the DB commit. At-least-once delivery plus DB idempotency gives effectively-once effects across redelivery and rebalance.
- **Global transaction index and full journal:** one global `tx_id` index in a single logical database, preserving global uniqueness. The distributed journal is terminal and append-only: chargeback marks a transaction `charged_back` instead of deleting it, and a `charged_back` ID cannot be reused, disputed, or resolved. This is a deliberate divergence from the in-memory journal, which is unchanged.
- **Reused ledger rules:** deposits, withdrawals, disputes, resolves, and chargebacks follow the existing rules, including first-attempt lock on an over-available dispute, one warn-level warning naming the canonical client ID, and held funds remaining held. Duplicate transaction IDs and foreign references keep distinct `DuplicateTransaction` and `NotOwner` outcomes.
- **Job-scoped reporting:** the exporter waits until every transaction partition has processed all events emitted for the job, then writes the accounts CSV for the clients the job touched, including clients initialized by rejected rows. The report uses canonical client IDs. v1 allows one active batch job; a second submission is rejected, and an unrecoverable file failure marks the job failed with no partial export.
- **Database portability:** one repository contract over PostgreSQL and MariaDB/Galera, with schema, migrations, a portable ID allocator, and retries for certification/deadlock failures. Galera is virtually synchronous, certification-based, multi-primary replication used for HA, not data or write sharding; v1 uses one logical database.
- **Operations:** configuration and credential precedence, topic/DB migrations, bounded retries with a dead-letter path, and health/metrics for consumer lag, DLQ depth, and stalled watermarks. Journal and idempotency records are retained without a v1 cleanup policy.
- **No breaking CLI change.** The distributed schema, topics, and crate are new surfaces. Out of scope: cross-client transfers, partial disputes, the payment gateway/live ingestion, in-memory journal changes, DB sharding, gRPC/HTTP/WebSocket variants, and unlocking or refunding stranded held funds.

## Capabilities

### New Capabilities

- `client-identity` — Source-scoped external keys mapped to DB-minted, globally unique, stable `BIGINT` client IDs by the partitioner.
- `stream-ingestion` — Manifest ordinals/hashes, parallel parsing, staging payloads, per-client partitioner ownership, DB file watermarks, and per-client input ordering across overlapping files.
- `distributed-ledger` — Kafka consumer workers, DB-backed atomic application, content-addressed idempotency, the global index, the full terminal journal, the reused ledger/dispute rules, and the canonical-ID warning.
- `job-reporting` — Job completion watermarks, canonical-ID accounts export for touched clients, single-active-job behavior, and terminal job failure without partial output.
- `database-portability` — PostgreSQL/MariaDB-Galera repository contract, schema/migrations, portable ID allocation, conflict retries, and the single-logical-DB scope.
- `distributed-operations` — Configuration and credential precedence, migrations, retry/DLQ behavior, and health/metrics for lag, DLQ depth, and stalled watermarks.

### Modified Capabilities

None. The in-memory CLI and the existing ledger, ingestion, reporting, arithmetic, and process specs keep their behavior. The new capability specs carry their own scenarios; a cross-variant conformance suite runs the shared ledger cases against both binaries to prevent drift. The distributed journal divergence is defined in `distributed-ledger`.

## Impact

- **Packaging:** a new crate/workspace member; the existing package, its single binary, bare `cargo run`, and public submission stay unchanged.
- **Code:** shared-core extraction, a durable repository, a partitioner with bounded buffers, new service binaries for ingest/partition/worker/export/migrate, and the existing binary unchanged. The library API changes; the CLI behavior does not.
- **Dependencies:** `rdkafka` with the native librdkafka library (the implemented choice, because consumer groups, rebalancing, offset commits, idempotent production, and admin operations are required), a SQL client with migrations (`sqlx`), and container-based test support. Fixed-point `i64` and the no-floating-point rule carry over.
- **Systems:** Kafka, PostgreSQL or MariaDB/Galera, a migration runner, topics with a chosen partition count, DLQ wiring, credentials, and monitoring.
- **Data:** clients and external keys, accounts, the global transaction index with terminal journal, content-addressed idempotency records, job/file watermarks, and progress. Journal and idempotency retention is unbounded in v1.
- **Tests:** a distributed acceptance harness, both database backends, Kafka rebalance/redelivery cases, and a cross-variant oracle that runs the same CSV through the CLI and the distributed variant and compares balances.
- **Operations:** consumer lag, DLQ depth, retry exhaustion, stalled watermarks, and failed jobs must be visible; a failed job never emits a partial report.
- **Limits and risks:** the global index and single logical DB cap write throughput; replicas add availability and consumer parallelism, not write capacity. Galera is HA, not sharding. Partitioner buffers and staging growth need bounds/backpressure. Serialized jobs reduce batch concurrency. The full journal and idempotency tables grow without a v1 retention policy. The existing irreversible lock and stranded-held-funds policy are inherited. Rekeying clients when partition counts change must be planned. Shared-core extraction must not regress the CLI.
