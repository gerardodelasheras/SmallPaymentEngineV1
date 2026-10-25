# Transaction Processing Specification

## Purpose

TBD - created by archiving change implement-transaction-processor. Update Purpose after archive.

## Requirements

### Requirement: A deposit credits the account

The system SHALL credit `available` by the deposit amount and record the transaction as settled. `total` SHALL remain `available + held`.

#### Scenario: A deposit increases available and total

```gherkin
Given a new client
When the client deposits 10.0
Then the client's available is 10.0
And the client's held is 0.0
And the client's total is 10.0
And the client is not locked
```

#### Scenario: Successive deposits accumulate

```gherkin
Given a new client deposits 10.0
When the client deposits 5.0
Then the client's available is 15.0
And the client's total is 15.0
```

### Requirement: A withdrawal debits only sufficient funds

The system SHALL debit `available` by the withdrawal amount when `available` is at least the amount, and SHALL reject the withdrawal without changing balances otherwise.

#### Scenario: A sufficient withdrawal reduces available and total

```gherkin
Given a new client deposits 10.0
When the client withdraws 4.0
Then the client's available is 6.0
And the client's total is 6.0
```

#### Scenario: An insufficient withdrawal leaves balances unchanged

```gherkin
Given a new client deposits 3.0
When the client withdraws 5.0
Then the client's available is 3.0
And the client's total is 3.0
```

#### Scenario: A withdrawal of exactly the available balance is allowed

```gherkin
Given a new client deposits 3.0
When the client withdraws 3.0
Then the client's available is 0.0
```

### Requirement: A dispute requires sufficient available funds

The system SHALL move the referenced settled transaction's amount from `available` to `held` only when `available` is at least the amount, keep `total` unchanged, and mark the transaction disputed. When the amount exceeds `available`, the system SHALL reject the dispute, leave `available`, `held`, and the transaction entry unchanged, and immediately lock the account. A locked account SHALL reject every later operation.

#### Scenario: Disputing a deposit holds its amount

```gherkin
Given a new client deposits 10.0
When the client disputes the deposit
Then the client's available is 0.0
And the client's held is 10.0
And the client's total is 10.0
And the client is not locked
```

#### Scenario: A dispute that exceeds available is rejected and locks the account

```gherkin
Given a new client deposits 100.0
And the client withdraws 80.0
When the client disputes the deposit
Then the client's available is 20.0
And the client's held is 0.0
And the client's total is 20.0
And the client is locked
```

#### Scenario: A chargeback after a rejected dispute changes nothing

```gherkin
Given a new client deposits 10.0 with transaction 1
And the client deposits 20.0 with transaction 2
And the client disputes transaction 1
And the client withdraws 10.0
And the client disputes transaction 2
When the client charges back the dispute
Then the client's available is 10.0
And the client's held is 10.0
And the client's total is 20.0
And the client is locked
```

#### Scenario: A dispute of an unknown transaction is skipped

```gherkin
Given a new client
When the client disputes transaction 99
Then the client's available is 0.0
And the client's held is 0.0
And the client is not locked
```

#### Scenario: A second dispute of the same transaction is skipped

```gherkin
Given a new client deposits 10.0
And the client disputes the deposit
When the client disputes the deposit again
Then the client's held is 10.0
And the client's total is 10.0
And the client is not locked
```

### Requirement: A resolve releases held funds

The system SHALL move the referenced disputed transaction's amount from `held` back to `available`, keep `total` unchanged, and mark the transaction settled.

#### Scenario: Resolving a dispute returns the funds

```gherkin
Given a new client deposits 10.0
And the client disputes the deposit
When the client resolves the dispute
Then the client's available is 10.0
And the client's held is 0.0
And the client's total is 10.0
```

#### Scenario: Resolving a transaction that is not disputed is skipped

```gherkin
Given a new client deposits 10.0
When the client resolves the deposit
Then the client's available is 10.0
And the client's held is 0.0
```

#### Scenario: A resolved transaction may be disputed again

```gherkin
Given a new client deposits 10.0
And the client disputes the deposit
And the client resolves the dispute
When the client disputes the deposit again
Then the client's available is 0.0
And the client's held is 10.0
```

### Requirement: A chargeback withdraws held funds and locks the account

The system SHALL remove the referenced disputed transaction's amount from `held`, leave `available` unchanged, reduce `total` accordingly, and lock the account. A locked account SHALL reject every later operation.

#### Scenario: A chargeback withdraws held funds and locks the account

```gherkin
Given a new client deposits 10.0
And the client disputes the deposit
When the client charges back the dispute
Then the client's available is 0.0
And the client's held is 0.0
And the client's total is 0.0
And the client is locked
```

#### Scenario: A chargeback of a transaction that is not disputed is skipped

```gherkin
Given a new client deposits 10.0
When the client charges back the deposit
Then the client's available is 10.0
And the client is not locked
```

#### Scenario: A later deposit on a locked account is rejected

```gherkin
Given a new client deposits 10.0
And the client disputes the deposit
And the client charges back the dispute
When the client deposits 5.0
Then the client's available is 0.0
And the client is still locked
```

### Requirement: Transactions are indexed and owned by one client

The system SHALL associate each accepted monetary transaction with its client and identifier, SHALL reject a duplicate identifier, and SHALL reject a reference to a transaction owned by another client.

#### Scenario: A duplicate transaction identifier is rejected

```gherkin
Given a new client deposits 10.0 with transaction 1
When the client deposits 5.0 with transaction 1
Then the client's available is 10.0
```

#### Scenario: A reference to another client's transaction is rejected

```gherkin
Given client 1 deposits 10.0 with transaction 1
When client 2 disputes transaction 1
Then client 2's held is 0.0
And client 1's available is 10.0
```

### Requirement: Rows are applied in input order per client

The system SHALL apply each client's transactions in the order they appear in the input, regardless of interleaving with other clients.

#### Scenario: Interleaved clients keep their own order

```gherkin
Given client 1 deposits 10.0
And client 2 deposits 20.0
And client 1 withdraws 4.0
And client 2 withdraws 5.0
When the processor runs
Then client 1's available is 6.0
And client 2's available is 15.0
```

### Requirement: Unknown clients are initialized when a row parses

The system SHALL initialize a zeroed account for a client on the first successfully parsed row for that client, before domain validation, so the client appears in the report even when the operation is rejected.

#### Scenario: A rejected withdrawal still initializes the client

```gherkin
Given a new client withdraws 5.0
When the accounts are written
Then the client's available is 0.0
And the client's held is 0.0
And the client's total is 0.0
And the client is not locked
```

#### Scenario: A row rejected during parsing does not initialize the client

```gherkin
Given a row for a new client with an invalid amount
When the accounts are written
Then no account is initialized for that client
```

### Requirement: Rejected operations do not mutate ledger state

When the system rejects a parsed transaction, it SHALL leave every account's `available`, `held`, and `locked` state unchanged and SHALL leave the accepted-transaction index unchanged, except that a dispute rejected because its amount exceeds `available` SHALL lock the account. The only permitted state effects of a rejected parsed row SHALL be the already-specified zeroed-account initialization for that client and that lock.

#### Scenario: A rejected duplicate deposit leaves balances unchanged

```gherkin
Given a new client deposits 10.0 with transaction 1
When the client deposits 5.0 with transaction 1
Then the client's available is 10.0
And the client's held is 0.0
And the client's total is 10.0
And the client is not locked
```

#### Scenario: A rejected duplicate deposit preserves the original entry

```gherkin
Given a new client deposits 10.0 with transaction 1
When the client deposits 5.0 with transaction 1
And the client disputes transaction 1
Then the client's available is 0.0
And the client's held is 10.0
And the client's total is 10.0
And the client is not locked
```

#### Scenario: A rejected dispute leaves other transactions intact

```gherkin
Given a new client deposits 250.0 with transaction 11
And the client withdraws 100.0
When the client disputes transaction 99
Then the client's available is 150.0
And the client's held is 0.0
And the client's total is 150.0
And the client is not locked
```

#### Scenario: A foreign reference does not lock the disputing client

```gherkin
Given client 1 deposits 10.0 with transaction 1
When client 2 disputes transaction 1
Then client 2's held is 0.0
And the client is not locked
And client 1's available is 10.0
```

#### Scenario: A rejected over-available dispute locks the account without changing balances

```gherkin
Given a new client deposits 10.0
And the client withdraws 10.0
When the client disputes the deposit
Then the client's available is 0.0
And the client's held is 0.0
And the client's total is 0.0
And the client is locked
```

#### Scenario: A rejected operation on a locked account changes nothing

```gherkin
Given a new client deposits 40.0 with transaction 21
When the client disputes transaction 21
And the client charges back the dispute
And the client deposits 15.0 with transaction 22
Then the client's available is 0.0
And the client's held is 0.0
And the client's total is 0.0
And the client is locked
```
