# Distributed ledger

The distributed variant reuses the shared ledger core and adds Kafka-backed
ingestion, partitioning, durable workers, and job reporting. The in-memory CLI
remains the single-node reference.

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

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | `postgres://...` or `mysql://...` |
| `KAFKA_BOOTSTRAP` | Broker address, for example `localhost:9092` |

Command-line flags override the environment variables.

## Commands

```sh
# Apply the schema and provision staging, transactions, and DLQ topics.
target/debug/distributed migrate --backend postgres --json

# Reset the schema and recreate the topics (test/teardown only).
target/debug/distributed migrate --backend postgres --fresh --json

# Parse a manifest, mint canonical client IDs, and stage rows.
target/debug/distributed ingest --manifest manifest.json --job job-1 --json

# Consume staging, order per client, and emit to transactions.
# --drain/--expect is a finite test mode; omit them for a long-running service.
target/debug/distributed partition --job job-1 --drain --expect 100 --json
```

`worker` and `export` are implemented by the remaining tasks in
`openspec/changes/kafka-distributed-ledger/tasks.md`.

## Kafka semantics

- **Producers** use `enable.idempotence=true` and `acks=all`.
- **Consumers** disable auto-commit and commit offsets explicitly.
- **Partitioner** commits a staging offset only while that staging partition has
  no buffered rows, so a crash replays buffered records instead of losing them.
- **Workers** commit the transactions offset only after the database commit.
- **Idempotency** is content-addressed (`event_id`) plus database dedupe, so
  redelivery is safe even when Kafka itself redelivers a record.

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

The distributed Behave suite lives in `acceptance-tests/distributed/`.
