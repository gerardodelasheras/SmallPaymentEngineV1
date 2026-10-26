# Distributed Ledger Specification

## Purpose

Durable, idempotent application of the shared ledger rules with one database transaction per event and a full terminal transaction journal.

## Requirements

### Requirement: Each event is applied atomically before its offset is committed

The system SHALL process each transaction record in one database transaction that dedupes the event, locks the account, applies the ledger rules, updates the transaction index and job progress, and commits before the Kafka offset is committed.

#### Scenario: A redelivered event does not apply twice

```gherkin
Given an event was committed and its offset was committed
When Kafka redelivers the same event after a rebalance
Then the dedupe record causes a no-op
And the account balances do not change
```

#### Scenario: A crash before the offset commit is retried

```gherkin
Given a worker commits the database transaction and crashes before committing the offset
When another worker receives the event
Then the event is deduped
And the final account state is unchanged
```

### Requirement: The distributed ledger applies the existing transaction rules

The system SHALL apply deposits, withdrawals, disputes, resolves, and chargebacks with the same balance and state rules as the in-memory reference, including exact-available disputes, over-available rejection and lock, and no lock for unknown, foreign, or already-disputed references.

#### Scenario: A deposit and withdrawal update available and total

```gherkin
Given a client has no balance
When the client deposits 10.0
And the client withdraws 4.0
Then the client's available is 6.0
And the client's total is 6.0
```

#### Scenario: An over-available dispute locks the account

```gherkin
Given a client has available 20.0
When the client disputes a settled transaction for 100.0
Then the dispute is rejected
And the client's available is 20.0
And the client's held is 0.0
And the client is locked
```

#### Scenario: An exact-available dispute succeeds

```gherkin
Given a client has available 10.0
When the client disputes a settled transaction for 10.0
Then the client's available is 0.0
And the client's held is 10.0
And the client is not locked
```

#### Scenario: An unknown or foreign reference does not lock

```gherkin
Given a client has available 10.0
When the client disputes a transaction it does not own
Then the dispute is rejected
And the client is not locked
```

### Requirement: The distributed journal is full and terminal

The system SHALL retain every accepted transaction, including charged-back transactions, and SHALL mark a charged-back transaction `charged_back` instead of deleting it. A charged-back transaction ID SHALL NOT be reused, disputed, or resolved.

#### Scenario: A chargeback retains the transaction

```gherkin
Given a settled transaction 1
And the client disputes and charges back transaction 1
Then transaction 1 remains in the journal with state charged_back
And the account is locked
```

#### Scenario: A charged-back transaction ID cannot be reused

```gherkin
Given transaction 1 is charged_back
When a new deposit uses transaction 1
Then the deposit is rejected as a duplicate transaction
```

### Requirement: A parsed row initializes its client before validation

The system SHALL create the canonical account on the first successfully parsed row for a client, before domain validation, so a client whose only row is rejected still appears in the job report.

#### Scenario: A rejected withdrawal still initializes the client

```gherkin
Given a new client's only row is a withdrawal that exceeds available
When the job is exported
Then the client appears with zero balances
And the client is not locked
```

### Requirement: Duplicate identifiers and foreign references are classified distinctly

The system SHALL reject a monetary row whose transaction ID already exists as a duplicate transaction, and SHALL reject a dispute, resolve, or chargeback that references another client's transaction as not owned.

#### Scenario: A duplicate monetary ID is rejected

```gherkin
Given transaction 5 is settled for client 42
When client 42 deposits with transaction 5
Then the row is rejected as a duplicate transaction
```

#### Scenario: A foreign reference is rejected as not owned

```gherkin
Given transaction 5 is settled for client 42
When client 43 disputes transaction 5
Then the row is rejected as not owned
And client 43's account is not locked
```

### Requirement: A locking rejection emits exactly one warning

The system SHALL emit exactly one warn-level warning per account locked by an over-available dispute, naming the canonical client ID and stating that the account is locked. A later locked-account rejection SHALL NOT emit the warning again.

#### Scenario: The first locking rejection warns

```gherkin
Given a client has available 0.0
When the client disputes a settled transaction for 10.0
Then the account locks
And one warning names canonical client 42
And the warning states that the account is locked
```

#### Scenario: Later locked rows do not warn again

```gherkin
Given the account is locked by an over-available dispute
When another row for the client is received
Then it is rejected as locked
And no second warning is emitted
```
