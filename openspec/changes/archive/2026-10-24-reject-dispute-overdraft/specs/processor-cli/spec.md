# Processor CLI Specification

## MODIFIED Requirements

### Requirement: Trace diagnostics are opt-in

The system SHALL emit no row-level diagnostics by default, except one warning at warn level when an account is locked by a dispute whose amount exceeds `available`. The system SHALL enable other row diagnostics through `RUST_LOG=trace` in development and test builds, and SHALL compile trace logging out of release builds. The lock warning SHALL be emitted in release builds; in development and test builds, explicit `RUST_LOG` filtering may suppress it.

#### Scenario: A rejected row is silent by default

```gherkin
Given an input CSV whose only row is invalid
When the processor runs without RUST_LOG
Then the run exits successfully
And stderr contains no row diagnostic
```

#### Scenario: Trace logging reveals a rejected row

```gherkin
Given an input CSV whose only row is invalid
When the processor runs with RUST_LOG set to trace
Then the rejected row is reported on stderr
```

#### Scenario: An overdraft dispute warning is emitted by default

```gherkin
Given a new client deposits 10.0
And the client withdraws 10.0
And the client disputes the deposit
When the processor runs without RUST_LOG
Then the run exits successfully
And stderr contains a warning naming client 1
And stderr states that the account is locked
```

#### Scenario: The overdraft dispute warning is suppressed by explicit filtering in a test build

```gherkin
Given a new client deposits 10.0
And the client withdraws 10.0
And the client disputes the deposit
When the processor runs with RUST_LOG set to error
Then the run exits successfully
And stderr contains no row diagnostic
```
