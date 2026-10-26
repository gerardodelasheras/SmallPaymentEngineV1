`acceptance-tests/` already exists for the CLI. This plan adds a separate distributed acceptance harness while keeping the existing CLI suite green.

## 1. Planning checkpoint and distributed acceptance harness

- [x] 1.1 Commit the planning checkpoint on `main` with user approval: `proposal.md`, `proposal.council.md`, `specs/`, `design.md`, `adr.md`, the new `adr/0005-0012-*.md` files, and this `tasks.md`. Do not start code while specs-zone changes are uncommitted.
- [x] 1.2 Add a distributed acceptance harness (Python/Behave or a docker-compose-backed runner) that can start Kafka, PostgreSQL, and MariaDB, submit a manifest, wait for a job, and read the exported accounts CSV; keep the CLI suite untouched.
- [x] 1.3 Add the manifest/job page object and step definitions for `client-identity` and `stream-ingestion` scenarios.
- [x] 1.4 Add the distributed ledger step definitions for deposits, withdrawals, disputes, resolves, chargebacks, idempotent redelivery, journal terminal state, and the locking warning.
- [x] 1.5 Add the job/reporting step definitions for completion, touched clients, rejected-row initialization, second-job rejection, failure, and resubmission.
- [x] 1.6 Add the database-portability and distributed-operations step definitions for both backends, conflict retries, configuration, migrations, DLQ, health, and metrics.
- [x] 1.7 Run the distributed acceptance suite and record the red scenarios; confirm there are no undefined steps caused by the new specs.
- [x] 1.8 Run the spec extractor and `gherkin-lint`; confirm the new capabilities compose and the existing CLI specs still compose.

## 2. Packaging and shared ledger core

- [x] 2.1 Add a separate `distributed/` crate/workspace member with entry points for ingest, partition, worker, export, and migrate; keep the existing package as the default member and bare `cargo run` working.
- [x] 2.2 Add a CLI regression test for chargeback followed by reuse of the same transaction ID, pinning the current in-memory delete behaviour before refactoring.
- [x] 2.3 Extract a pure shared ledger core: account mutation, journal lookup/effect, dispute/resolve/chargeback transitions, and identifier/index boundaries.
- [x] 2.4 Adapt the in-memory `TransactionProcessor` to the shared core without changing CLI behaviour or output.
- [x] 2.5 Generalize the identifier boundary so the CLI keeps `u16` and the distributed adapter uses canonical `BIGINT`; update the library exports and document the library API break.
- [x] 2.6 Add a cross-variant conformance test that runs the same generated CSVs through the CLI and the distributed adapter and compares final balances, journal state, and lock state.
- [x] 2.7 Run the CLI unit, integration, fixture, and acceptance suites; confirm the shared-core extraction is green before adding Kafka.

## 3. Database schema and repository contract

- [x] 3.1 Add PostgreSQL migrations for `clients`, `accounts`, `transactions`, `ledger_events`, `jobs`, `job_files`, `job_events`, `job_progress`, `job_clients`, `id_allocator`, and the job lock; include the global `tx_id` unique index and account checks.
- [x] 3.2 Add equivalent MariaDB/Galera migrations with backend-specific types, checks, and indexes.
- [x] 3.3 Implement the repository trait and PostgreSQL adapter for mint-or-get, account row locking, ledger event insert, transaction lookup/upsert, job progress, job clients, and reporting queries.
- [x] 3.4 Implement the MariaDB/Galera adapter, owning `ON DUPLICATE KEY UPDATE`, boolean/timestamp mapping, and dialect-specific migration loading.
- [x] 3.5 Implement the portable `id_allocator` block allocator with atomic mapping, unique IDs, and acceptable gaps.
- [x] 3.6 Implement bounded retry/backoff for PostgreSQL deadlock/serialization and MariaDB/Galera certification conflicts, preserving idempotency.
- [x] 3.7 Add repository contract tests on both backends: concurrent mint-or-get, duplicate `tx_id` rejection, account lock/update atomicity, idempotent ledger events, and job progress.
- [x] 3.8 Add an idempotent `migrate` command that applies migrations and verifies or creates the required topics.

## 4. Identity, manifest ingestion, and partitioner

- [x] 4.1 Implement `mint_or_get` for `(source_id, external_client: u16)` and prove concurrent first sightings converge on one canonical ID.
- [x] 4.2 Define and validate the manifest model: source, unique ordinal, hash, location, duplicate/missing/hash-mismatch handling, and manifest hash identity.
- [x] 4.3 Implement bounded parallel ingestion using the shared CSV parser; stage the parsed transaction plus `(job_id, source_id, external_client, file_ord, row_ord, event_id)`; handle malformed rows without stopping the file.
- [x] 4.4 Produce staging records keyed by source-scoped external key with an idempotent producer; flush before marking a file complete.
- [x] 4.5 Implement the partitioner: consume staging, map external keys to canonical IDs, own each source-scoped client through exactly one staging partition (many clients share a partition; a replica may own several partitions), buffer until earlier files complete, emit in `(file_ord, row_ord)` order keyed by canonical ID, and update emitted progress.
- [x] 4.6 Bound partitioner buffers and apply Kafka backpressure; expose a stalled-watermark signal.
- [x] 4.7 Add restart/resume tests for partial files, re-ingestion idempotency, overlapping-file ordering, and manifest failure -> terminal job failure.
- [x] 4.8 Add topic provisioning/partition-count configuration for staging, transactions, and DLQ; document that changing the transactions partition count rekeys clients.

## 5. Distributed ledger worker

- [x] 5.1 Implement the worker consumer group and one DB transaction per event: ledger-event dedupe, job-event dedupe, account lock, shared-core application, journal update, progress, and job-client tracking; commit the offset only after the DB commit.
- [x] 5.2 Implement the full terminal journal: chargeback updates `charged_back`, never deletes, rejects reuse, and rejects dispute/resolve against a charged-back row.
- [x] 5.3 Implement zeroed account initialization for the first parsed row before validation, including rejected rows, and include those clients in job reporting.
- [x] 5.4 Classify duplicate monetary IDs as `DuplicateTransaction` and foreign references as `NotOwner`; keep unknown/already-disputed references non-locking.
- [x] 5.5 Emit exactly one warn-level warning per over-available lock, naming the canonical client ID; keep later locked rows warning-free.
- [x] 5.6 Add worker health/readiness and metrics for records processed, retries, DLQ sends, and locked accounts.
- [x] 5.7 Add rebalancing, redelivery, crash-between-DB-and-offset, and duplicate-event tests; prove no double-apply.
- [x] 5.8 Add DLQ handling for exhausted retries with job/file/row/error metadata; a poison event fails the job and writes no partial export.

## 6. Job submission, completion, and export

- [x] 6.1 Implement job submission with a singleton job lock; reject a second active job as `JobAlreadyActive`; accept a new job after a terminal state.
- [x] 6.2 Implement the completion wait: `job_progress.processed == emitted` on every partition with emitted events, plus partitioner completion; never write a partial CSV.
- [x] 6.3 Implement the accounts export with the fixed header, canonical `BIGINT` client IDs, touched clients including rejected-row initialization, and sorted/deterministic rows.
- [x] 6.4 Implement terminal job failure for hash mismatch, missing files, and poison events; release the lock and allow a later valid job.
- [x] 6.5 Implement idempotent resubmission of a completed manifest and prove the exported balances do not change or double-count.
- [x] 6.6 Add end-to-end tests for a valid job, a lagging partition, a failed job, a second active job, and a re-submitted manifest.

## 7. Operations, configuration, and test infrastructure

- [x] 7.1 Implement configuration loading with file + environment precedence, required-setting validation, and secret redaction.
- [x] 7.2 Implement the health/readiness and metrics surface for consumer lag, DLQ depth, stalled watermarks, failed jobs, and service readiness.
- [x] 7.3 Add containerized Kafka/PostgreSQL/MariaDB test infrastructure and a docker-compose or testcontainers profile for the distributed suite.
- [x] 7.4 Add optional 3-node Galera conflict/retry integration coverage; keep single-node MariaDB contract tests required.
  - Note: the required gate is the single-node MariaDB contract test plus the injected certification-conflict retry scenario. A 3-node Galera topology remains an optional manual exercise.
- [x] 7.5 Add topic/DB credential configuration, connection pooling, and graceful shutdown for each service.

## 8. Documentation and durable records

- [x] 8.1 Add distributed README/operations documentation: architecture, manifest format, topics, DB backends, migrations, job lifecycle, retries/DLQ, metrics, limitations, and rollback.
- [x] 8.2 Verify `adr/0005-0012-*.md` and the change ADR manifest are consistent; do not edit the existing CLI, `SPEC.md`, or `openspec/specs/` until archive sync.
- [x] 8.3 Document the library API break, the canonical-vs-external ID mapping, the single-logical-DB write ceiling, serialized jobs, and unbounded journal/idempotency retention.

## 9. Full verification

- [x] 9.1 Run `cargo test` for the existing package and the distributed crate.
- [x] 9.2 Run `./tests/run_tests.sh`; the existing CLI fixtures must remain unchanged and green.
- [x] 9.3 Run the existing CLI acceptance suite; it must remain green.
- [x] 9.4 Run the distributed acceptance suite; every scenario passes with zero pending/undefined steps and the report is generated.
- [x] 9.5 Run the cross-variant conformance suite on generated inputs and compare CLI vs distributed balances, journal state, and lock state.
- [x] 9.6 Run PostgreSQL and MariaDB contract tests; run Galera conflict tests where available.
- [x] 9.7 Run `openspec validate kafka-distributed-ledger --type change --strict` (or the supported equivalent) and confirm the change is valid.
- [x] 9.8 Review the effective spec and confirm the CLI specs and the six new capabilities compose without duplicate or missing scenarios.
- [x] 9.9 Confirm specs-zone and code-zone changes are committed separately with user approval; do not auto-commit or archive from a feature branch.
  - Archived from `main` after implementation was merged. The sync/archive changes are committed separately from the code commits.
