# ADR Review Manifest

- Status: completed
- Review date: 2026-10-24

## Review Summary

ADR review completed for `reject-dispute-overdraft`. The change introduces one durable ledger policy: reject an over-available dispute, immediately lock the account on the first attempt, and emit one default warning. This intentionally diverges from the initial design's permissive overdraw reading. The three in-force ADRs were reviewed and remain unaffected.

## In-Force ADRs Reviewed

- `adr/0001-fixed-worker-pool-sharding.md` — fixed worker pool and client affinity are unchanged; the dispute path stays worker-local. No supersession.
- `adr/0002-fixed-point-i64-money.md` — fixed-point `i64` amounts scaled by 10,000 are unchanged; no floating point or decimal dependency is introduced. No supersession.
- `adr/0003-single-pass-streaming-lifecycle.md` — single-pass ingestion, bounded queues, and snapshot-then-shutdown are unchanged; the new warning uses stderr only. No supersession.

## New Durable ADRs Created

- `adr/0004-reject-dispute-overdraft.md` — first-attempt rejection and terminal lock for over-available disputes, with a default warn-level diagnostic.
