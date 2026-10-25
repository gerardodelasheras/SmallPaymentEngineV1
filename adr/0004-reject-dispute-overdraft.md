# ADR-0004: Reject Over-Available Disputes and Lock on First Attempt

## Status

Accepted

## Date

2026-10-24

## Context and Problem Statement

The initial design defined insufficient-funds handling for withdrawals but was silent on disputes. The implementation interpreted that silence permissively: a dispute moved the full referenced amount from `available` to `held` even when `available` was smaller, so `available` could become negative. The README documented this as an assumption and the acceptance spec contained a scenario named "A dispute may overdraw available".

That behavior creates a loss path. A client can deposit 100, withdraw 80, dispute the original deposit, and charge back the dispute. The final state is `available = -80`, `total = -80`, a locked account, and no remaining hold for the bank to recover. `available` is the balance that represents money actually on hand, so a negative value means paying out funds that were already spent.

The question is how to close that path without adding a retry budget, partial-dispute state, or a second account-lock semantic. The in-memory processor is the reference implementation for later streaming and database-backed variants, so the rule chosen here becomes the shared ledger contract.

## Considered Options

- Keep permissive overdraw. Rejected: it leaves the fraud path open and permits a negative total.
- Reject the over-available dispute without locking. Rejected: it allows repeated attempts and does not treat the first fraudulent claim as terminal.
- Reject, count attempts, and lock after three strikes. Rejected by product decision: it adds account state and deliberately gives the fraudster more attempts.
- Partially hold up to the available balance. Rejected: it requires a partial-dispute state and changes resolve and chargeback semantics.
- Reject the over-available dispute and immediately set the existing terminal `locked` state. Chosen: it closes the path with the smallest state change and reuses the lock semantics already applied by chargeback.
- Introduce a separate fraud-lock or restricted state, or allow resolve/chargeback on locked accounts. Rejected: the report exposes only a `locked` column, and a second lock semantic would complicate every future variant.
- Keep diagnostics trace-only. Rejected: a first-attempt lock is a deliberate, high-impact policy change and should be visible by default, not only when `RUST_LOG=trace` is enabled.

## Decision Outcome

Chosen option: "Reject the over-available dispute and immediately set the existing terminal `locked` state", because it closes the loss path without adding per-account counters, partial-dispute state, or a second lock mode.

The rule is:

- A dispute is accepted only when `available >= the referenced transaction's amount`; exact-available disputes still succeed.
- When the amount exceeds `available`, the dispute is rejected without changing `available`, `held`, or the transaction entry. The entry stays settled.
- The first such rejection immediately sets the account's existing terminal `locked` state. Every later row for that client is rejected as locked.
- The account emits one warn-level stderr warning per locking rejection, naming the client and stating that the account is locked. Release builds always emit it; explicit `RUST_LOG` filtering may suppress it in development and test builds.
- There is no counter, threshold, retry path, partial hold, new lock type, or unlock path. Funds already held by active disputes remain held.

### Consequences

- Good, because the deposit-withdraw-dispute-chargeback path can no longer produce a negative balance or total.
- Good, because the ledger invariant `available >= 0` and `total >= 0` now holds through every accepted operation.
- Good, because the change reuses the existing `locked` state and output column, adds no dependency, and is reusable by later streaming and database-backed variants.
- Good, because the lock is visible by default on stderr without polluting the accounts CSV on stdout.
- Bad, because one mistaken or unusual over-available dispute can lock a legitimate client; this is a deliberate, strict policy.
- Bad, because funds held by earlier active disputes are frozen under the terminal lock and cannot be resolved or charged back.
- Bad, because the accounts CSV does not distinguish a first-attempt lock from a chargeback lock; the stderr warning is the only signal.
- Bad, because this deliberately diverges from the initial design's permissive reading, so the README, specs, tests, and acceptance scenarios must all change.
- Follow-up: a future database-backed variant must persist the lock and emit the same warning contract. Releasing or refunding stranded held funds, adding an unlock process, or introducing a configurable threshold are separate decisions that would supersede or extend this ADR.
