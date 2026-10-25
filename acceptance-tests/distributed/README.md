# Distributed test scenario

This compose file provides the infrastructure for the distributed ledger acceptance suite:

- Kafka in single-node KRaft mode, using the smallest official image.
- PostgreSQL 17 Alpine.
- MariaDB 11.4 (the official image has no Alpine variant).

The databases are behind Compose profiles so only **one** runs at a time. The host has limited memory and swap; starting Kafka plus both databases together is not recommended.

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
