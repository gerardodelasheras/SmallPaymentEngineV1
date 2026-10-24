## Why

Disputes currently hold the full amount of the referenced transaction even when the account no longer has that much `available`, so `available` can go negative. A client can deposit 100, withdraw 80, dispute the deposit, then charge back the dispute: `available` and `total` end at -80, the account is locked, and the bank has no remaining hold to recover. `available` represents money actually on hand, so a negative value means paying out funds already spent. The initial design is silent on this boundary; the implementation interpreted that silence as permission, recorded in the README and in the `transaction-processing` scenario "A dispute may overdraw available".

This change deliberately deviates. An over-available dispute is treated as a first fraud attempt and locks the account immediately, so the first attempt is also the last. The rule is intentionally strict: a legitimate client can also trigger it through a mistake or an unusual settlement sequence, but the priority is to close the loss path. The change also makes the rejection visible by default, a second deviation from `SPEC.md` §5.1's silent-skip rule and the project's opt-in diagnostics posture. Both deviations are recorded in a new durable ADR; `SPEC.md` remains the historical baseline.

## What Changes

- A dispute is accepted only when `available` is at least the referenced transaction's amount. Exact-available disputes still succeed.
- **BREAKING**: a dispute whose amount exceeds `available` is rejected in full and immediately locks the account. The row is skipped; balances and the transaction entry are unchanged; the entry stays settled; a later chargeback is rejected as not disputed. Previously the row moved the full amount into `held` and produced a negative `available`.
- Only the insufficient-`available` branch of the dispute hold locks. Unknown, foreign, or already-disputed references are rejected without locking, and a locked account is rejected before validation. The new locking error is distinct from the withdrawal insufficient-funds error.
- The rejection is recoverable: processing continues, later rows for other clients are applied, and the run exits 0. The triggering row reports the overdraft rejection, not the locked-account error.
- The immediate lock uses the existing terminal locked state. All later rows for that client are rejected as locked. Funds already held by active disputes remain held, so the report can show `locked = true` with `held > 0`. The lock is indistinguishable from a chargeback lock in the accounts CSV; there is no unlock path.
- Emit one warning per account locked this way, at warn level by default, naming the client and stating the lock. Release builds always emit it; debug and test builds can suppress it with explicit `RUST_LOG` filtering. Every other recoverable row remains trace-only.
- The current variant is in-memory, so the lock lasts for the run; persistence belongs to the future database-backed variant.
- The resulting invariants are `available >= 0` and `total >= 0`. The negative-`available` clauses in `account-reporting` and `amount-arithmetic` become unobservable through the ledger. Keep minus-sign rendering as defensive formatter behavior covered by the existing `Amount` unit test, but remove the now-false scenarios from both specs.
- No other ledger rule changes. The exception is the chargeback-after-rejected-dispute path, which now returns not-disputed instead of succeeding.
- The rejected-row atomicity requirement is amended to allow exactly one rejected-row state effect: the immediate lock on an over-available dispute. All retained scenarios of every modified requirement must be restated, with requirement names matched exactly, because the acceptance composer replaces the whole source block and only warns when a modified name is not found.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `transaction-processing`: adds the insufficient-`available` boundary; amends rejected-row atomicity for the single lock effect; adds first-attempt lock, exact-available, settled-entry preservation, chargeback-after-rejection, only-insufficient-locks (unknown, foreign, or already-disputed references do not lock), locked-with-`held = 0`, locked-with-`held > 0`, and locked-precedence scenarios; removes the overdraw scenario.
- `processor-cli`: carves out the default warning for this locking rejection; other row diagnostics remain opt-in; adds a scenario for the warning and one for silent rejection of other rows.
- `account-reporting`: the locked column can now be caused by a chargeback or an over-available dispute; adds both lock-cause scenarios; removes the negative-`available` scenario.
- `amount-arithmetic`: removes the negative-`available` scenario because the ledger can no longer produce it; keeps the minus-sign rendering requirement as defensive formatter behavior. No parsing, scaling, or arithmetic rule changes, and no new formatter test is needed.

## Impact

- **Code:** the account hold funds check; a new recoverable error distinct from withdrawal insufficient funds; the processor dispute path; the worker-pool recoverable-row logging path, which must warn for this class; unit tests; the integration oracle. No account-level bookkeeping is added.
- **Public API:** `ProcessingError` gains a variant; it defaults to recoverable through the existing wildcard, and the worker pool needs an explicit warning arm. The crate is not published, so there are no external consumers. `Account` needs no new field.
- **CLI and output:** the stdout schema is unchanged; stderr gains one warning per account locked this way; exit status is unchanged; no new flags or dependencies.
- **Breaking behavior:** final balances and lock state change for inputs containing an over-available dispute, and an account locks on the first one, skipping later rows. In `tests/accounts1.csv`, all five rows change: clients 1, 3, 4, and 5 lock on their first over-available dispute, and client 2 is already locked by chargeback. `tests/accounts0.csv` and `tests/accounts2.csv` are expected unchanged but must be verified. `accounts1` stops being a mixed lock-state baseline, so dedicated locked and unlocked cases are needed.
- **Tests and acceptance:** replace the overdraw-success unit test; add first-attempt lock, warning, locked-with-`held = 0`, locked-with-`held > 0`, chargeback-after-rejection, exact-available, only-insufficient-locks, and locked-precedence tests; update the integration oracle with the funds check and lock; note that the seeded generator may exercise locked-skip more; add behave steps with a stable stderr substring; keep the stderr-empty scenario away from locking disputes; regenerate reviewed fixture references.
- **Docs:** README assumptions, operation-effects table, output-schema `locked` note, Usage diagnostics paragraph, the "rows are skipped ... `RUST_LOG=trace`" paragraph, the error-variant list, and the tested-scenarios "negative available balances" entry; a new durable ADR under `adr/` documenting both deviations, alternatives, and accepted risks; the change ADR review manifest references it. `SPEC.md` is not rewritten.
- **Out of scope:** partial disputes, terminal `charged_back` journal rows in the current solution, the Kafka/distributed database design, cross-client transfers, lock persistence across runs, and releasing or refunding stranded held funds on a locked account.
- **Risks:** a legitimate client can be locked by one mistaken or unusual over-available dispute, including a dispute of a withdrawal; held funds are frozen with no resolve or chargeback escape; the lock cause is unobservable in the CSV and there is no unlock path. These are accepted costs and a documented support limitation in the ADR.
