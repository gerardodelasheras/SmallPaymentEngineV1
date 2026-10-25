# Processor CLI Specification

## Purpose

TBD - created by archiving change implement-transaction-processor. Update Purpose after archive.

## Requirements

### Requirement: The CLI takes exactly one input path

The system SHALL accept exactly one positional argument naming the input CSV path, and SHALL fail when the argument is missing or when more than one is supplied.

#### Scenario: One positional argument runs the processor

```gherkin
Given a valid input CSV path
When the processor is invoked with that one argument
Then the run exits successfully
And the output contains the accounts header
```

#### Scenario: No input argument fails

```gherkin
Given no positional argument
When the processor is invoked
Then the run exits with a non-zero status
And a usage message is written to stderr
```

#### Scenario: Two input arguments fail

```gherkin
Given two positional arguments
When the processor is invoked
Then the run exits with a non-zero status
And a usage message is written to stderr
```

### Requirement: Standard output contains only the accounts CSV

The system SHALL reserve stdout for contract-compliant accounts CSV records and SHALL write every diagnostic to stderr.

#### Scenario: A skipped row does not change stdout

```gherkin
Given an input CSV with one invalid row and one valid deposit
When the processor runs
Then stdout contains the accounts header
And stdout contains only account rows
And the invalid row is reported on stderr when diagnostics are enabled
```

### Requirement: Row failures are isolated

The system SHALL continue after a malformed or rejected row so that later rows are still applied, without panicking or halting the stream.

#### Scenario: A malformed row does not halt processing

```gherkin
Given a malformed row followed by a valid deposit for client 1
When the processor runs
Then the run exits successfully
And the output contains a row for client 1
```

#### Scenario: Invalid UTF-8 does not halt processing

```gherkin
Given a row with invalid UTF-8 bytes followed by a valid deposit for client 1
When the processor runs
Then the run exits successfully
And the output contains a row for client 1
```

### Requirement: Infrastructure failures end the run

The system SHALL exit with a non-zero status and a stderr message when the input cannot be read, the accounts CSV cannot be written, or a worker or its channel is lost; such failures SHALL NOT be silently converted into partial output.

#### Scenario: A missing input file fails the run

```gherkin
Given an input path that does not exist
When the processor runs
Then the run exits with a non-zero status
And an error is written to stderr
```

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

### Requirement: Successful runs exit zero

The system SHALL exit with status zero after writing the accounts CSV when no infrastructure failure occurs.

#### Scenario: A valid input exits zero

```gherkin
Given a valid input CSV with one deposit
When the processor runs
Then the run exits with status zero
And the accounts CSV is written to stdout
```
