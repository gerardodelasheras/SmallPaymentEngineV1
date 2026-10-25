# Transaction Processing Specification

## ADDED Requirements

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

## MODIFIED Requirements

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

## REMOVED Requirements

### Requirement: A dispute holds funds

**Reason**: Replaced by "A dispute requires sufficient available funds". The permissive overdraw behavior is removed because it allows a dispute and chargeback to leave a negative balance.

**Migration**: No client action. Deposits, withdrawals, resolve, and chargeback keep their existing behavior; a dispute whose amount exceeds `available` is now rejected and locks the account instead of overdrawing.
