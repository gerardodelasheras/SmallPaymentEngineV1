# ADR-0010: Global Transaction Journal with Terminal States

## Status

Accepted

## Date

2026-10-24

## Context and Problem Statement

The current in-memory processor removes a transaction entry on chargeback. That allows a later row to reuse the same transaction ID, and it leaves no audit record of the chargeback. The distributed variant needs global transaction uniqueness, dispute/resolve lookups, and an auditable journal.

## Considered Options

- Delete on chargeback as the in-memory processor does. Rejected: allows ID reuse and loses audit history.
- Partition the transaction index by client. Rejected: cannot enforce global `tx_id` uniqueness without a global constraint.
- Store only an event log and derive balances. Rejected: adds projection and checkpoint complexity before the report contract is stable.
- Keep a global `tx_id` index with a terminal `charged_back` state. Chosen: global uniqueness, audit history, and dispute lookups in one logical database.

## Decision Outcome

Chosen option: "Keep a global `tx_id` index with a terminal `charged_back` state". A chargeback updates the row to `charged_back` and never deletes it. A charged-back ID cannot be reused, disputed, or resolved. The global index is one unpartitioned table in the single logical database; this is the explicit write/scale ceiling. The in-memory journal keeps its current delete behaviour, and the divergence is owned by the distributed-ledger capability.

### Consequences

- Good, because transaction IDs remain globally unique for the life of the ledger.
- Good, because chargebacks and later disputes have an audit trail.
- Good, because the distributed variant is stronger than the CLI's reuse-permitting behaviour.
- Bad, because the global index and one logical database cap write scaling.
- Bad, because the journal grows without a v1 retention policy.
- Bad, because the two variants now intentionally differ on post-chargeback reuse; conformance tests must encode the divergence.
- Follow-up: journal retention/archival is captured as the future `journal-retention-policy` change; the eventual sharding strategy is separate.
