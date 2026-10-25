# ADR-0007: Source-Scoped Canonical Client Identity

## Status

Accepted

## Date

2026-10-24

## Context and Problem Statement

The CSV `client` field is a `u16` label that is only meaningful within one input source. Many files may come from different sources, and the same numeric value in two sources may refer to different clients. The distributed variant needs a globally unique, stable identity to key Kafka records and to shard client state.

## Considered Options

- Use the bare `u16` as the global identity. Rejected: a 65,536-client ceiling and cross-source collisions.
- Widen the CSV key to text or a UUID. Rejected: changes the existing CSV parser and ingestion contract.
- Mint in the worker. Rejected: the transactions topic cannot be keyed by canonical ID before consumption.
- Use `(source_id, external_client: u16)` and mint a DB `BIGINT` in the partitioner. Chosen: source isolation, stable global IDs, and canonical Kafka keys.

## Decision Outcome

Chosen option: "Use `(source_id, external_client: u16)` and mint a DB `BIGINT` in the partitioner". A unique constraint covers the source-scoped key and a unique constraint covers the canonical ID. Mapping is mint-or-get and atomic under concurrency. The job report uses the canonical `BIGINT` as its `client` column.

### Consequences

- Good, because two sources can safely reuse the same external client value.
- Good, because canonical IDs are stable across re-ingestion and can key Kafka and the database.
- Good, because the existing CSV parser and u16 field remain valid inside a source.
- Bad, because the identity table grows and canonical IDs can contain allocator gaps.
- Bad, because the report's `client` values differ from the CSV's external values; operators need a mapping query.
- Follow-up: define the allocator block size and the operational lookup for external-to-canonical mappings.
