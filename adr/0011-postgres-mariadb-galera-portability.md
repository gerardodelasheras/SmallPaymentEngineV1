# ADR-0011: PostgreSQL and MariaDB/Galera Portability

## Status

Accepted

## Date

2026-10-24

## Context and Problem Statement

The database backend must be configurable between PostgreSQL and MariaDB with Galera. The two stacks differ in upsert syntax, boolean/timestamp types, migration tooling, identity allocation, and conflict behaviour. Galera is often mistaken for sharding, but it is full-copy multi-primary replication.

## Considered Options

- Support PostgreSQL only. Rejected: fails the configurable-backend requirement.
- Rely on native auto-increment. Rejected: MariaDB/Galera requires node-specific offsets and behaves differently from PostgreSQL sequences.
- Maintain two independently designed schemas. Rejected: doubles the semantics and the test matrix.
- One repository contract with two adapters and a portable allocator. Chosen: one ledger contract, backend-specific dialects hidden.

## Decision Outcome

Chosen option: "One repository contract with two adapters and a portable allocator". The contract covers mint-or-get, account locking, ledger event insertion, transaction lookup/upsert, job progress, and report queries. Adapters own `ON CONFLICT` versus `ON DUPLICATE KEY UPDATE`, boolean/timestamp mapping, and migration paths. A portable `id_allocator` table allocates IDs in blocks; gaps are acceptable. Both adapters use `READ COMMITTED` plus `SELECT ... FOR UPDATE`; Postgres deadlocks/serialization and Galera certification failures are retried with bounded backoff. Galera is treated as virtually synchronous, certification-based, multi-primary HA, not data or write sharding. V1 uses one logical database.

### Consequences

- Good, because the same ledger contract runs on both required backends.
- Good, because the portable allocator avoids backend-specific auto-increment behaviour.
- Good, because transient conflicts are handled explicitly rather than surfacing as random failures.
- Bad, because every repository operation has two implementations and a larger contract-test matrix.
- Bad, because Galera conflicts and retries add latency, and a hot account can contend across primaries.
- Bad, because one logical database caps write scale; this ADR explicitly excludes sharding.
- Follow-up: define allocator block size, retry budgets, and the optional Galera multi-node CI topology.
