# Amount Arithmetic Specification

## ADDED Requirements

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

## REMOVED Requirements

### Requirement: Amounts render canonically

**Reason**: Replaced by "Amounts render canonically with defensive minus signs". The negative-`available` scenario is no longer reachable because a dispute whose amount exceeds `available` is rejected and locks the account.

**Migration**: No client action. Parsing, scaling, and arithmetic are unchanged; negative rendering remains defensive formatter behavior covered by unit tests.
