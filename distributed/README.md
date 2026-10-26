# Distributed ledger

The distributed variant reuses the shared ledger core and adds Kafka-backed
ingestion, partitioning, durable workers, and job reporting. The in-memory CLI
remains the single-node reference and is unchanged.

## Architecture

```text
manifest.json
    │  ingest (parse, validate, mint canonical IDs)
    ▼
Kafka staging topic  (key = source_id:external_client)
    │  partitioner (own each staging partition, order per client)
    ▼
Kafka transactions topic  (key = canonical BIGINT client_id)
    │  worker (one DB transaction per event, dedupe, lock, journal, progress)
    ▼
PostgreSQL or MariaDB/Galera
    │  export (wait for every partition, write job-scoped CSV)
    ▼
accounts.csv
```

- **Ingest** validates the manifest, mints or gets canonical IDs, and stages
  parsed rows. It never applies ledger rules.
- **Partitioner** consumes staging partitions through a consumer group, maps
  external keys to canonical IDs, orders rows by `(file_ord, row_ord)` per
  client, and emits to the transactions topic keyed by canonical client ID.
- **Worker** consumes transactions partitions, applies the shared ledger core in
  one database transaction per event, and commits the Kafka offset only after
  the database commit.
- **Export** waits until the partitioner emitted every staged row and every
  transaction partition processed its events, then writes the job-scoped CSV.

The distributed services live in the `distributed` workspace member. The root
package stays the default member, so a bare `cargo run` still runs the CLI.

## Build prerequisites

The Kafka client is `rdkafka`, which wraps the native librdkafka library.

```sh
sudo apt-get install librdkafka-dev pkg-config
cargo build -p distributed
```

Runtime deployments need the shared library as well (on Debian,
`librdkafka1`). Container images must install it or link librdkafka statically.

### Version pin

The crate is pinned to `rdkafka` 0.37 with `rdkafka-sys` 4.8.0+2.3.0 in
`Cargo.lock`. Debian trixie provides librdkafka 2.8. `rdkafka` 0.39 requires
librdkafka 2.12 or newer, so it cannot use the Debian package without a newer
native library. The lock pin prevents a routine dependency update from silently
raising the native requirement.

If the deployment image ships librdkafka 2.12 or newer, upgrading to
`rdkafka` 0.39 is possible; update the lock and re-run both backend tests.

## Why rdkafka instead of rskafka

`rskafka` was the initial pure-Rust choice. It is a minimal client and its
README explicitly lists no support for offset tracking, consumer groups, or
transactions. The partitioner and worker need consumer-group ownership,
rebalancing, offset commits after durable work, and (for ingestion) idempotent
production. Those requirements are captured in ADR-0009 and ADR-0013.

| Capability | `rskafka` | `rdkafka` |
|---|---|---|
| Build | Pure Rust | Native librdkafka + C toolchain |
| Produce to a partition | Yes | Yes |
| Consumer groups / rebalancing | No | Yes |
| Committed offsets | No | Yes |
| Idempotent producer | No | Yes |
| Topic create/delete admin | Create only | Yes |
| Kafka transactions | No | Yes (not used by this design) |
| Deployment footprint | Small | Needs librdkafka at runtime |

The native dependency is the price for real consumer-group semantics. A
pure-Rust client would require manual partition assignment, database-stored
offsets, and a manual scaling procedure, which would change the design and
weaken crash recovery.

The full rationale, alternatives, and consequences are in
[`adr/0013-rdkafka-client.md`](../adr/0013-rdkafka-client.md).

## Configuration

Environment variables override the optional JSON file. `--backend` and
`--database-url` flags override both.

| Source | Setting | Purpose |
|---|---|---|
| environment | `DATABASE_URL` | `postgres://...` or `mysql://...`; the scheme selects the backend |
| environment | `KAFKA_BOOTSTRAP` | Broker address, for example `localhost:9092` |
| environment | `DATABASE_PASSWORD` | Optional secret, always redacted by `config show` |
| environment | `DISTRIBUTED_CONFIG` | Path to a JSON configuration file |
| file | `kafka_bootstrap` | Broker address |
| file | `database_url` | Database URL |
| file | `database_password` | Optional secret |

Precedence is environment, then file, then built-in defaults. The backend is
inferred from a `mysql://` URL, so `--backend mariadb` is only needed when the
URL does not make it obvious.

`config show --json` prints the effective configuration with the database
password and any URL password replaced by `***`. It never requires a live
broker, so it is safe to use before deployment.

## Canonical identity

The database owns canonical IDs. Each `(source_id, external_client)` pair maps
to a `BIGINT` minted by the partitioner through the database allocator. Two
sources using the same external value receive different canonical IDs. Mapping
is atomic: concurrent first sightings converge on one row and one ID.

The shared library API is **breaking** for this change: `ClientId` remains the
in-memory `u16` namespace, while `CanonicalClientId` is the distributed `i64`.
The ledger core is generic over both. Code that used the old concrete IDs must
name the correct type.

The worker never mints or remaps identities. Every transactions record is keyed
by the canonical ID, so one client always lands on one transactions partition.

## Manifest format

```json
{
  "source_id": "alpha",
  "files": [
    {
      "source_id": "alpha",
      "ordinal": 1,
      "path": "/data/alpha-1.csv",
      "sha256": "…64 hex characters…"
    }
  ]
}
```

- Ordinals are unique per source and define the cross-file order.
- `sha256` must match the file content; a mismatch fails the job terminally.
- A CSV has the header `type,client,tx,amount` and the row types `deposit`,
  `withdrawal`, `dispute`, `resolve`, and `chargeback`.
- A malformed row is reported and skipped; valid rows in the same file are still
  staged.

Event identity is content-addressed: `(source_id, file_hash, row_ord)`. Re-
ingesting the same content produces the same event ID and is deduped by the
ledger, so balances do not double-count.

## Topics

| Topic | Key | Purpose |
|---|---|---|
| `staging` | `source_id:external_client` | Parsed rows with job/file/row coordinates |
| `transactions` | canonical `BIGINT` client ID | Ordered events for the worker |
| `dlq` | event ID | Exhausted events with job/file/row/error metadata |

Partition counts are chosen at migration time:

```sh
target/debug/distributed migrate --backend postgres \
  --staging-partitions 6 \
  --transactions-partitions 12 \
  --dlq-partitions 1 --json
```

| Topic | Controls | Guidance |
|---|---|---|
| `staging` | Partitioner parallelism | More partitions allow more partitioner replicas; many clients share a partition. |
| `transactions` | Worker parallelism | **Choose before ingesting.** Producers key by canonical client ID. |
| `dlq` | Dead-letter parallelism | 1 is enough for the test scenario; size for operator volume. |

Changing `--transactions-partitions` **rekeys canonical clients**. Stop
ingestion, drain in-flight work, and perform a controlled rekey. `--fresh-topics`
recreates the topics with the supplied counts and is destructive.

## Database backends

Both adapters implement one repository contract. The schema is portable:
fixed-point `i64` money, `BIGINT` identities, one global `tx_id` index, and no
backend-specific SQL in the domain layer.

- **PostgreSQL** uses a blocking transaction lock per event.
- **MariaDB/Galera** uses the same contract. Galera is virtually synchronous,
  multi-primary high availability, not sharding. Write conflicts are retried
  with a bounded budget.

Migrations are idempotent:

```sh
# Apply schema and provision topics (safe to repeat).
target/debug/distributed migrate --backend postgres --json

# Reset the schema only (test/teardown).
target/debug/distributed migrate --backend postgres --fresh --json

# Reset the schema and recreate topics (destructive; suite setup).
target/debug/distributed migrate --backend postgres --fresh --fresh-topics --json
```

## Job lifecycle

1. **Submit**: `ingest` inserts the job and acquires the singleton active-job
   lock. A second active job is rejected (exit 2) without creating a job row.
2. **Stage**: valid rows are produced to `staging`; file completion is recorded
   durably. A crash leaves the file incomplete, so it is never ordered early.
3. **Partition**: each client is owned by one staging partition. A file is
   emitted only when it is complete and every earlier file for that source is
   fully emitted. Progress is recorded idempotently.
4. **Apply**: each event is one database transaction: ledger-event dedupe,
   job-event dedupe, account row lock, shared-core rules, journal update, job
   progress, and touched-client tracking. The offset commits after the commit.
5. **Export**: waits for `processed == emitted` on every partition and for all
   file watermarks to catch up. It then writes the CSV atomically and marks the
   job completed, releasing the lock.
6. **Terminal failure**: a hash mismatch, missing file, or poison event marks the
   job failed, writes no CSV, and releases the lock so a later job can start.

The CSV is `client,available,held,total,locked`, sorted by canonical client ID.
It contains only clients touched by the job, including clients whose first row
was rejected by ledger validation.

## Commands

```sh
# Schema and topics.
target/debug/distributed migrate --backend postgres --json

# Submit and stage a manifest.
target/debug/distributed ingest --manifest manifest.json --job job-1 --json

# Drain a job through the partitioner (test/ops finite mode).
target/debug/distributed partition --job job-1 --drain --expect 100 --json

# Apply events. Omit --drain/--expect for the long-running worker.
target/debug/distributed worker --drain --job job-1 --expect 100 --json

# Write the job-scoped CSV (waits for the job to drain).
target/debug/distributed export --job job-1 --out accounts.csv --json

# Readiness without writing a CSV.
target/debug/distributed export --job job-1 --check --json

# Operator queries.
target/debug/distributed job status --job job-1 --json
target/debug/distributed journal --tx 1 --json
target/debug/distributed dlq list --job job-1 --json
target/debug/distributed identity mint --source alpha --client 7 --json
target/debug/distributed config show --json
target/debug/distributed health --json
target/debug/distributed metrics --json
```

`--drain` and `--partition` are deterministic test/operations modes that assign
partitions directly. Production replicas use consumer groups and commit offsets.

## Retries and dead letters

Transient database failures (serialization, deadlock, Galera certification) are
retried up to five attempts with bounded backoff. An event that exhausts the
budget is published to `dlq` with its event ID, job, source, file, row, attempts,
and error. The worker then marks the job failed, commits the offset only after
the DLQ publish, and exits 3. No partial CSV is written.

`DISTRIBUTED_FAULT=conflict:N` injects transient failures for tests (`N` attempts
or `always`) inside the real retry loop.

## Health and metrics

`health --json` verifies that the database and broker are reachable and reports
`ready`. A missing broker or database setting fails fast with a configuration
error before any job is accepted.

`metrics --json` reports:

- `consumer_lag`: pending events (`emitted - processed`) across jobs.
- `dlq_depth`: records in the dead-letter topic.
- `failed_jobs`: jobs in the failed state.
- `stalled_watermarks`: running jobs whose file watermarks or events have not
  advanced.
- `job_states`: current state per job.

Both commands redact secrets.

## Kafka semantics

- **Producers** use `enable.idempotence=true` and `acks=all`.
- **Consumers** disable auto-commit and commit offsets explicitly.
- **Partitioner** commits a staging offset only while that staging partition has
  no buffered rows, so a crash replays buffered records instead of losing them.
- **Workers** commit the transactions offset only after the database commit.
- **Idempotency** is content-addressed (`event_id`) plus database dedupe, so
  redelivery is safe even when Kafka itself redelivers a record.
- **Shutdown** handles Ctrl-C by draining in-flight work; uncommitted partitions
  are replayed by another replica.

## Limitations

- **One logical database.** The global `tx_id` index and account state live in
  one database. Galera adds availability, not write sharding. V1 is a
  low-volume, correctness-first design.
- **Serialized batch jobs.** At most one active job exists at a time. This
  simplifies export, ordering, and the singleton lock.
- **Write ceiling.** Every event takes an account row lock and one transaction;
  throughput is bounded by the database, not by Kafka.
- **Unbounded retention.** Ledger events, the terminal journal, and idempotency
  rows are never deleted in V1. `journal-retention-policy` is a proposal-only
  follow-up.
- **Deferred features.** Cross-client transfers, a payment gateway, and
  multi-tenant authorization are out of scope.
- **Partition count changes.** Changing the transactions partition count rekeys
  clients and requires a controlled migration.

## Rollback

1. Stop `ingest` and `partition` producers.
2. Let `worker` drain committed offsets, then stop it.
3. Export any job that must be preserved.
4. To abandon the deployment, stop the services and remove the Kafka topics and
   database schema (`migrate --fresh-topics` is destructive). To keep the
   ledger, retain the database and archive the Kafka topics; replay is safe
   because events are content-addressed and deduped.

## Tests

```sh
cargo test -p distributed

# These require the compose database profiles to be running.
cargo test -p distributed --test postgres_migrations
cargo test -p distributed --test mariadb_migrations
cargo test -p distributed --test postgres_repository
cargo test -p distributed --test mariadb_repository
cargo test -p distributed --test migrate
```

The distributed Behave suite lives in `acceptance-tests/distributed/` and runs
against PostgreSQL or MariaDB:

```sh
acceptance-tests/.venv/bin/python acceptance-tests/distributed/run_distributed.py \
  --backend postgres
acceptance-tests/.venv/bin/python acceptance-tests/distributed/run_distributed.py \
  --backend mariadb
```

It uses `--drain` for finite runs, resets the ledger per scenario, and supports
`--crash-after-rows` for the restart/resume scenario.
