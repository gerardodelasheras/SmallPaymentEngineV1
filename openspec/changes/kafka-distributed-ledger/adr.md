# ADR Review Manifest

- Status: completed
- Review date: 2026-10-24

## Review Summary

ADR review completed for `kafka-distributed-ledger`. The change introduces a separate distributed crate and eight durable decisions covering packaging, shared-core extraction, canonical identity, ordered ingestion, stateless DB-backed workers, the global terminal journal, database portability, and job-scoped export. The four in-force ADRs were reviewed and remain unaffected; none is superseded.

## In-Force ADRs Reviewed

- `adr/0001-fixed-worker-pool-sharding.md` — the distributed variant preserves per-client ownership through Kafka partition ownership; the CLI worker pool is unchanged. No supersession.
- `adr/0002-fixed-point-i64-money.md` — the distributed schema stores the same `i64` scaled by 10,000 and introduces no floating point. No supersession.
- `adr/0003-single-pass-streaming-lifecycle.md` — the CLI lifecycle is unchanged; the distributed variant adds durable offsets and checkpoints. No supersession.
- `adr/0004-reject-dispute-overdraft.md` — the first-attempt lock and default warning are inherited; the distributed worker persists the lock and names the canonical client in the warning. No supersession.

## New Durable ADRs Created

- `adr/0005-separate-distributed-crate.md` — distributed services live in a separate crate/workspace member; the existing package keeps its single binary.
- `adr/0006-shared-ledger-core-extraction.md` — one pure ledger core with in-memory and durable adapters, including the explicit library API break.
- `adr/0007-source-scoped-canonical-client-identity.md` — `(source_id, external_client)` maps to DB-minted `BIGINT` identities in the partitioner.
- `adr/0008-ordered-manifest-and-watermarks.md` — manifest order, durable file watermarks, single-client partitioner ownership, and content-addressed event identity.
- `adr/0009-stateless-db-backed-workers.md` — one DB transaction per event, idempotent inserts, and offset commit after the DB commit.
- `adr/0010-global-transaction-journal.md` — global `tx_id` index and full terminal `charged_back` journal in one logical database.
- `adr/0011-postgres-mariadb-galera-portability.md` — one repository contract, portable allocator, conflict retries, and Galera as HA rather than sharding.
- `adr/0012-job-scoped-export-serialization.md` — one active batch job, completion watermark, canonical-ID export, and terminal failure semantics.
