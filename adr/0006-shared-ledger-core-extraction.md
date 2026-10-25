# ADR-0006: Shared Ledger Core Extraction

## Status

Accepted

## Date

2026-10-24

## Context and Problem Statement

The existing `Account` arithmetic is reusable, but `TransactionProcessor` owns in-memory maps and `ClientId` is `u16`. The distributed variant needs durable BIGINT identity and a database-backed journal. Duplicating the ledger rules would invite drift; wrapping the stateful processor would tie the database adapter to in-memory maps.

## Considered Options

- Duplicate the ledger rules in the distributed crate. Rejected: two sources of truth and drift risk.
- Wrap the stateful processor. Rejected: the maps and state ownership cannot be replaced by a database.
- Copy the public types into the distributed crate. Rejected: same drift and incompatible identities.
- Extract a pure shared ledger core. Chosen: one state machine, two storage adapters.

## Decision Outcome

Chosen option: "Extract a pure shared ledger core". The core accepts an account, a transaction, and a journal lookup, and returns the resulting account and journal effect without owning storage. The in-memory adapter keeps its current chargeback-delete behaviour; the distributed adapter retains terminal rows. The identifier boundary is generalized so the CLI keeps `u16` and the distributed variant uses canonical `BIGINT`.

This is a **BREAKING** change for library consumers of `ClientId`, the processor internals, and the shared API. The CLI's observable behaviour and arguments do not change.

### Consequences

- Good, because both variants exercise one implementation of the ledger rules.
- Good, because a cross-variant conformance suite can compare the two adapters.
- Bad, because the extraction touches core types and is a public library API break.
- Bad, because the in-memory chargeback-delete policy must be preserved explicitly while the distributed adapter retains rows.
- Follow-up: add a CLI regression test for chargeback followed by reusing the same transaction ID, and run the shared ledger cases against both variants.
