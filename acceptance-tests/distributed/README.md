# Distributed test scenario

This compose file provides the infrastructure for the distributed ledger acceptance suite:

- Kafka in single-node KRaft mode, using the smallest official image.
- PostgreSQL 17 Alpine.
- MariaDB 11.4 (the official image has no Alpine variant).

The databases are behind Compose profiles. Run one at a time by default; the
portability tests may start both because PostgreSQL and MariaDB use different
ports and each schema is independent. On a memory-constrained host, keep one
database running.

## Start

Kafka + PostgreSQL:

```sh
sudo docker compose -f acceptance-tests/distributed/compose.yaml --profile postgres up -d
```

Kafka + MariaDB:

```sh
sudo docker compose -f acceptance-tests/distributed/compose.yaml --profile mariadb up -d
```

Wait for health:

```sh
sudo docker compose -f acceptance-tests/distributed/compose.yaml --profile postgres ps
sudo docker compose -f acceptance-tests/distributed/compose.yaml --profile mariadb ps
```

Kafka alone:

```sh
sudo docker compose -f acceptance-tests/distributed/compose.yaml up -d
```

## Stop and reset

```sh
sudo docker compose -f acceptance-tests/distributed/compose.yaml down
sudo docker compose -f acceptance-tests/distributed/compose.yaml down -v
```

`down -v` deletes the Kafka, PostgreSQL, and MariaDB data volumes. Use it between clean test runs.

## Endpoints

| Service | Host endpoint | Credentials |
|---|---|---|
| Kafka | `localhost:9092` | none |
| PostgreSQL | `localhost:5432` | user `ledger`, password `ledger`, database `ledger` |
| MariaDB | `localhost:3306` | user `ledger`, password `ledger`, database `ledger`; root password `root` |

Services running on the host should use `localhost`. Services running inside the compose network should use `kafka:9092`, `postgres:5432`, and `mariadb:3306`.

## Schema migrations

The portable schema lives in `distributed/migrations/`:

```text
distributed/migrations/postgres/0001_init.sql
distributed/migrations/mariadb/0001_init.sql
```

Both files create the same logical model with backend-specific types and
constraints: `clients`, `accounts`, `transactions`, `ledger_events`, `jobs`,
`job_files`, `job_events`, `job_progress`, `job_clients`, `id_allocator`, and
the singleton `job_lock`. The global `transactions.tx_id` primary key enforces
transaction-ID uniqueness; `accounts` enforces non-negative balances and the
`available + held` overflow bound.

Galera is virtually synchronous, full-copy, multi-primary replication used for
availability. It is **not** data or write sharding. V1 uses one logical database.

The migration files are idempotent (`CREATE TABLE IF NOT EXISTS`, conflict-safe
seed rows), so applying them twice is safe. The `migrate` command that invokes
them from the `distributed` binary is implemented by task 3.8; until then, apply
them directly:

```sh
docker compose -f acceptance-tests/distributed/compose.yaml --profile postgres up -d
docker compose -f acceptance-tests/distributed/compose.yaml exec -T postgres \
  psql -U ledger -d ledger -v ON_ERROR_STOP=1 -f -
# paste distributed/migrations/postgres/0001_init.sql

docker compose -f acceptance-tests/distributed/compose.yaml --profile mariadb up -d
docker compose -f acceptance-tests/distributed/compose.yaml exec -T mariadb \
  mariadb -uledger -pledger ledger
# paste distributed/migrations/mariadb/0001_init.sql
```

The migration tests apply the files to the running containers and verify the
tables, the duplicate-ID rejection, and the account checks:

```sh
cargo test -p distributed --test postgres_migrations
cargo test -p distributed --test mariadb_migrations
```

Each test is a no-op when its compose profile is not running.

## Image choices

- Kafka: `apache/kafka-native:4.0.0` (49 MB compressed, official, KRaft). Fall back to the JVM image if the native image misbehaves:

  ```sh
  KAFKA_IMAGE=apache/kafka:3.9.1 sudo docker compose -f acceptance-tests/distributed/compose.yaml up -d
  ```

- PostgreSQL: `postgres:17-alpine`.
- MariaDB: `mariadb:11.4` (LTS; no official Alpine image).

## Resource limits

| Service | Limit |
|---|---:|
| Kafka | 384 MB |
| PostgreSQL | 192 MB |
| MariaDB | 256 MB |

Run one database profile at a time. The host currently has less than 1 GB of available RAM and a full swap file.
