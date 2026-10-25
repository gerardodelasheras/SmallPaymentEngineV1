# Account Reporting Specification

## ADDED Requirements

### Requirement: Amounts use canonical decimal rendering with defensive minus signs

The system SHALL render `available`, `held`, and `total` with trailing fractional zeros removed and at least one fractional digit. Negative values SHALL render with a leading minus sign as defensive formatter behavior, although the ledger no longer produces them.

#### Scenario: A zero balance renders as 0.0

```gherkin
Given a new client deposits 1.0
And the client withdraws 1.0
When the processor runs
Then the output contains the row "1,0.0,0.0,0.0,false"
```

## MODIFIED Requirements

### Requirement: The locked column reflects the account state

The system SHALL write `true` in the `locked` column exactly when a chargeback or a dispute whose amount exceeds `available` locked the account, and `false` otherwise.

#### Scenario: A chargeback reports a locked account

```gherkin
Given a new client deposits 10.0
And the client disputes the deposit
And the client charges back the dispute
When the processor runs
Then the output contains the row "1,0.0,0.0,0.0,true"
```

#### Scenario: An overdraft dispute reports a locked account

```gherkin
Given a new client deposits 10.0
And the client withdraws 10.0
And the client disputes the deposit
When the processor runs
Then the output contains the row "1,0.0,0.0,0.0,true"
```

#### Scenario: An overdraft dispute with held funds reports a locked account

```gherkin
Given a new client deposits 10.0 with transaction 1
And the client deposits 20.0 with transaction 2
And the client disputes transaction 1
And the client withdraws 10.0
And the client disputes transaction 2
When the processor runs
Then the output contains the row "1,10.0,10.0,20.0,true"
```

#### Scenario: An active account reports false

```gherkin
Given a new client deposits 10.0
When the processor runs
Then the output contains the row "1,10.0,0.0,10.0,false"
```

## REMOVED Requirements

### Requirement: Amounts use canonical decimal rendering

**Reason**: Replaced by "Amounts use canonical decimal rendering with defensive minus signs". The negative-`available` scenario is no longer reachable because a dispute whose amount exceeds `available` is rejected and locks the account.

**Migration**: No client action. The output schema is unchanged, and the ledger no longer produces negative balances; minus-sign rendering remains defensive formatter behavior covered by unit tests.
