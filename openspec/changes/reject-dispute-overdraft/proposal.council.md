# Council Notes: proposal.md

## Author Summary

The first draft framed the overdraw dispute as a fraud path, proposed rejecting it, and added a per-account three-strike counter with a default stderr warning. After the user removed the retry budget, the second draft replaced the counter and threshold with an immediate lock on the first rejected overdraft dispute. The final artifact is the first-attempt-lock version; the counter design is superseded and is not part of the proposal.

## Reviewer Challenges

First round:

- The counter lifetime mixed "process" and "run", which implied state leaking between integration cases.
- The fixture claim said both `tests/accounts0.csv` and `tests/accounts1.csv` contained negative `available`; only `accounts1.csv` did.
- The draft claimed `src/worker_pool.rs` was unchanged, but that is where recoverable row errors are logged.
- Only the overdraft deviation was recorded; the default warning also deviates from `SPEC.md` §5.1 and the trace-only production posture.
- The ADR was pointed at the change-level manifest rather than a durable repository-level ADR.
- The Impact section omitted public API and several README statements that become false.
- Scenario deletion is not expressible in the acceptance composer; modified requirements must restate their retained scenarios.
- A second breaking marker was redundant, and the draft did not say the lock is indistinguishable from a chargeback lock.

Second round, after the first-attempt-lock revision:

- The lock contradicts the existing "rejected operations do not mutate ledger state" requirement, which was not listed as modified.
- The claim that explicit `RUST_LOG` filtering can suppress the warning is false in release builds.
- The fixture impact understated the change: four clients in `accounts1` hit an over-available dispute and all five rows change; `accounts2` was omitted.
- The justification said an over-available dispute "has no innocent reading" while also conceding legitimate mistakes, which cannot both stand.
- The trigger's observable row and the non-locking precedence cases had no named scenarios.
- The warning's content and whether the trace line is also emitted were unspecified.
- The "source-level break for exhaustive matches" claim was not true in this crate, because `is_recoverable` uses a wildcard.
- "Later rows are applied" conflicted with "later rows for that client are rejected".
- A residual "three-strike idea" reference survived in the impact section.

## Resolutions

- Accepted: first-attempt lock replaces the counter and all threshold/retry language.
- Accepted: the rejected-row atomicity requirement is amended to allow exactly one rejected-row state effect, the immediate lock.
- Accepted: warning suppression is qualified to debug and test builds; release builds always emit the warning.
- Accepted: fixture impact is stated as all five `accounts1` rows changing, with `accounts0` and `accounts2` expected unchanged but verified.
- Accepted: the "no innocent reading" contradiction is replaced by a statement that the strict rule can also catch legitimate mistakes.
- Accepted: scenarios now name both lock outcomes, only-insufficient-locks, chargeback-after-rejection, exact-available, and locked precedence.
- Accepted: the warning contract is one warning per locked account, naming the client and the lock.
- Accepted: the public API claim is limited to the new `ProcessingError` variant and the explicit warning arm.
- Accepted: "later rows for other clients are applied" replaces the ambiguous wording.
- Accepted: the residual three-strike reference is removed.
- Accepted: requirements must be matched by exact name, and both negative-render scenarios must be pruned consistently.
- Accepted: the ADR records the strict-rule trade-off and the absence of an unlock path as a support limitation.
- Rejected: the reviewer attributed the `accounts1` locks for clients 1 and 4 to withdrawal disputes. A replay of the fixture shows clients 1, 4, and 5 lock on deposit disputes and client 3 locks on a withdrawal dispute; the broader point that withdrawal disputes can trigger the lock is retained.
- Deferred: exact post-change fixture values are implementation/test outputs, not proposal content.

## Remaining Risks

- A legitimate client can be locked by one mistaken or unusual over-available dispute, including a dispute of a withdrawal.
- Held funds from active disputes are frozen under the terminal lock with no resolve or chargeback escape.
- The lock cause is not observable in the accounts CSV, and there is no unlock path.
- `accounts1` stops being a mixed lock-state baseline, so the acceptance suite needs dedicated locked and unlocked fixture coverage.
- The seeded integration oracle will exercise the locked-skip path more often, reducing fuzzing coverage of the dispute rule itself unless dedicated cases are added.
