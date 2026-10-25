`acceptance-tests/` already exists, so the first-time setup section is skipped.

## 1. Preconditions and acceptance tests

- [ ] 1.1 Commit the planning checkpoint on `main` with user approval: `proposal.md`, `specs/`, `design.md`, `adr.md`, `adr/0004-reject-dispute-overdraft.md`, and this `tasks.md`. Do not start code while specs-zone changes are uncommitted.
- [ ] 1.2 Add the pending acceptance step `the processor runs with RUST_LOG set to error` in `acceptance-tests/steps/steps.py`, reusing `context.cli.run(rust_log="error")`; run the scenario so it fails for the right reason, then keep it green once the warning behavior exists.
- [ ] 1.3 Add the pending acceptance step `stderr contains a warning naming client {client}`; assert the client id and a stable warning marker are present in stderr.
- [ ] 1.4 Add the pending acceptance step `stderr states that the account is locked`; assert a stable "account locked" phrase is present.
- [ ] 1.5 Run the acceptance suite and confirm the new over-available scenarios fail for the right reason: the dispute is still accepted, the account is not locked, and the default warning is absent. Record the red scenarios.
- [ ] 1.6 Run the spec extractor and `gherkin-lint` over the effective spec; confirm the new requirement names compose, the removed overdraw and negative-render scenarios do not run, and there are no pending or undefined steps caused by the new scenarios.
- [ ] 1.7 Confirm `stderr contains no row diagnostic` is not paired with a locking dispute except the explicit `RUST_LOG=error` suppression scenario; keep the default-silent scenario on a non-dispute invalid row.

## 2. Domain and processor implementation (TDD)

- [ ] 2.1 Add a failing unit test in `src/account/account.rs` that `hold` rejects an over-available dispute with `ProcessingError::DisputeExceedsAvailable` and leaves `available` and `held` unchanged.
- [ ] 2.2 Add failing unit tests for exact-available success and for `Overflow`/`Underflow` not mapping to the new dispute error.
- [ ] 2.3 Add `ProcessingError::DisputeExceedsAvailable { amount, available }` with a `thiserror` message; keep the `is_recoverable` wildcard behavior and run the account tests to green.
- [ ] 2.4 Implement the `Account::hold` funds check before the checked subtraction; do not mutate either balance on rejection.
- [ ] 2.5 Add failing unit tests in `src/transaction/transaction_processor.rs` for: an over-available dispute locks and leaves the entry settled; exact-available succeeds; unknown, foreign, and already-disputed references do not lock; later operations return `AccountLocked`; held funds remain held.
- [ ] 2.6 Implement the lock transition in `process_dispute` only for `DisputeExceedsAvailable`; leave the entry untouched and return the error.
- [ ] 2.7 Run `cargo test` and confirm the unit suite is green; verify no floating point or new dependency was introduced.

## 3. Worker diagnostics

- [ ] 3.1 Add an explicit `DisputeExceedsAvailable` arm in `run_worker` that emits `tracing::warn!` with the client, transaction id, disputed amount, available balance, and an "account locked" message; keep every other recoverable error trace-only.
- [ ] 3.2 Add a focused test or rely on the acceptance scenario to prove exactly one warning per account and no warning for `AccountLocked` rows; verify the worker does not panic and the run exits 0.
- [ ] 3.3 Verify the release/debug warning behavior: `warn!` survives release, the default filter is `warn`, and `RUST_LOG=error` suppresses it in the test build.

## 4. Integration oracle and fixtures

- [ ] 4.1 Update the reference oracle in `tests/integration.rs` so an over-available dispute sets `locked = true` and returns without mutating balances or the entry; mirror the ledger exactly.
- [ ] 4.2 Add deterministic integration cases for the fraud path, exact-available success, chargeback-after-rejection with held funds, and unknown/foreign/already-disputed no-lock.
- [ ] 4.3 Run the integration suite and confirm randomized and generated comparisons match; add seeds if locked-skip reduces dispute coverage.
- [ ] 4.4 Regenerate `tests/accounts1.csv`; review all five rows (clients 1, 3, 4, and 5 lock on their first over-available dispute; client 2 is already locked by chargeback). Confirm `tests/accounts0.csv` and `tests/accounts2.csv` are unchanged.
- [ ] 4.5 Add dedicated cases for a locked account with `held = 0` and with `held > 0` if the fixtures do not already cover both.
- [ ] 4.6 Run `./tests/run_tests.sh` and confirm every fixture matches the reviewed references.

## 5. Documentation

- [ ] 5.1 Update `README.md`: remove the "Disputes may overdraw `available`" assumption; document the first-attempt lock, the funds check, held funds remaining, the warning, and the absence of an unlock path.
- [ ] 5.2 Update the README operation-effects table, output-schema `locked` note, Usage diagnostics paragraph, "rows are skipped ... `RUST_LOG=trace`" paragraph, error-variant list, and tested-scenarios "negative available balances" entry.
- [ ] 5.3 Verify `adr/0004-reject-dispute-overdraft.md` and the change ADR manifest remain consistent; do not edit `SPEC.md`; do not edit `openspec/specs/` (the deltas sync at archive).

## 6. Full verification

- [ ] 6.1 Run `cargo test` (unit and integration).
- [ ] 6.2 Run `./tests/run_tests.sh`.
- [ ] 6.3 Run the acceptance suite; every scenario passes, zero pending/undefined steps, and the HTML report is generated.
- [ ] 6.4 Run `openspec validate reject-dispute-overdraft --type change --strict` (or the supported equivalent) and confirm the change is valid.
- [ ] 6.5 Review the effective spec: the removed overdraw and negative-render scenarios do not run; the new requirement names are active; the warning is asserted only where expected.
- [ ] 6.6 Confirm specs-zone and code-zone changes were kept in separate commits with user approval; do not auto-commit or archive from a feature branch.
