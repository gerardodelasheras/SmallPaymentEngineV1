# Amount Arithmetic Specification

## Purpose

TBD - created by archiving change implement-transaction-processor. Update Purpose after archive.

## Requirements

### Requirement: Amounts are stored as scaled integers

The system SHALL represent every monetary amount as an `i64` equal to the decimal value multiplied by 10,000, and MUST NOT use floating-point types in parsing, ledger arithmetic, or output.

#### Scenario: A fractional deposit is stored on the four-decimal grid

```gherkin
Given a new client deposits 1.23
When the accounts are written
Then the client's available is 1.23
And the client's total is 1.23
```

#### Scenario: A leading-dot amount is accepted

```gherkin
Given a new client deposits .5
When the accounts are written
Then the client's available is 0.5
```

#### Scenario: A leading-dot two-digit fraction is accepted

```gherkin
Given a new client deposits .25
When the accounts are written
Then the client's available is 0.25
```

#### Scenario: A leading-dot four-digit fraction is accepted

```gherkin
Given a new client deposits .0001
When the accounts are written
Then the client's available is 0.0001
```

#### Scenario: A whole amount is accepted

```gherkin
Given a new client deposits 5
When the accounts are written
Then the client's available is 5.0
```

#### Scenario: Trailing fractional zeros do not change the value

```gherkin
Given a new client deposits 1.2300
When the accounts are written
Then the client's available is 1.23
```

#### Scenario: Surrounding whitespace is ignored

```gherkin
Given a new client deposits an amount padded with spaces around 1.5
When the accounts are written
Then the client's available is 1.5
```

### Requirement: Invalid amounts are rejected

The system SHALL reject amounts that are negative, empty, non-numeric (including a bare decimal point), have more than four fractional digits, or overflow the scaled `i64`.

#### Scenario: A negative amount is rejected

```gherkin
Given a transaction row deposits the negative amount -1.0
When the processor runs
Then the row is skipped
And no account is initialized for that client
```

#### Scenario: More than four fractional digits is rejected

```gherkin
Given a transaction row deposits 1.23456
When the processor runs
Then the row is skipped
And no account is initialized for that client
```

#### Scenario: A non-numeric amount is rejected

```gherkin
Given a transaction row deposits abc
When the processor runs
Then the row is skipped
And no account is initialized for that client
```

#### Scenario: A bare decimal point is rejected

```gherkin
Given a transaction row deposits .
When the processor runs
Then the row is skipped
And no account is initialized for that client
```

#### Scenario: An amount that overflows the scaled integer is rejected

```gherkin
Given a transaction row deposits 922337203685477.5808
When the processor runs
Then the row is skipped
And no account is initialized for that client
```

### Requirement: Amounts render canonically with defensive minus signs

The system SHALL render a scaled amount as decimal text with trailing fractional zeros removed and at least one fractional digit. Negative scaled values SHALL render with a leading minus sign as defensive formatter behavior, although the ledger no longer produces them.

#### Scenario: Zero renders with one decimal place

```gherkin
Given a new client deposits 1.0
And the client withdraws 1.0
When the accounts are written
Then the client's available is 0.0
And the client's total is 0.0
```

#### Scenario: A held balance renders with stripped zeros

```gherkin
Given a new client deposits 10.0
And the client disputes the deposit
When the accounts are written
Then the client's held is 10.0
```

#### Scenario: A one-unit fraction renders at full precision

```gherkin
Given a new client deposits 0.0001
When the accounts are written
Then the client's available is 0.0001
```

### Requirement: Balance arithmetic is checked

The system SHALL perform ledger arithmetic with checked integer operations and SHALL reject a row whose result would overflow, leaving all balances unchanged.

#### Scenario: An overflowing deposit is skipped without changing balances

```gherkin
Given a new client deposits 922337203685477.5807
And the client deposits 0.0001
When the accounts are written
Then the client's available is 922337203685477.5807
```
