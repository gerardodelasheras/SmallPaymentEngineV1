# Database Portability Specification

## Purpose

One repository contract and portable schema for PostgreSQL and MariaDB/Galera, with fixed-point money, bounded conflict retries, and one logical database.

## Requirements

### Requirement: The ledger behavior is identical on PostgreSQL and MariaDB/Galera

The system SHALL implement the same repository contract for both backends, including identity mint-or-get, account locking, transaction upserts, idempotency, and job progress. The same ledger scenarios SHALL produce identical account and journal results on both.

#### Scenario: The same ledger cases pass on both backends

```gherkin
Given the ledger conformance suite
When it runs against PostgreSQL and MariaDB/Galera
Then both produce identical account and journal results
```

### Requirement: Client IDs use a portable allocator

The system SHALL allocate canonical client IDs without relying on backend-specific auto-increment behavior. Concurrent allocations SHALL be unique, and the same external key SHALL resolve to the same ID after restart.

#### Scenario: Concurrent allocation is unique

```gherkin
Given many concurrent first sightings
When IDs are allocated on either backend
Then every canonical ID is unique
And no mapping is lost
```

### Requirement: Transient database conflicts are retried

The system SHALL retry deadlock, serialization, and Galera certification failures within a bounded budget. A retry SHALL preserve idempotency; an exhausted budget SHALL surface an operator-visible failure and SHALL NOT partially apply the event.

#### Scenario: A certification conflict is retried

```gherkin
Given the database rejects a transaction with a transient certification conflict
When the worker retries
Then the event applies exactly once
And the offset is not committed before the successful database commit
```

#### Scenario: Exhausted retries are surfaced

```gherkin
Given every attempt fails with a transient conflict
When the retry budget is exhausted
Then the event is sent to the dead-letter path
And the job is marked failed
```

### Requirement: Schema and migrations are portable

The system SHALL provide migrations for both backends creating clients, accounts, the global transaction index with terminal state, idempotency records, and job progress, with constraints that reject duplicate transaction IDs and preserve account invariants.

#### Scenario: A fresh database migrates on both backends

```gherkin
Given an empty database
When migrations run on PostgreSQL and MariaDB/Galera
Then the required tables and constraints exist
And a duplicate transaction ID is rejected
```

### Requirement: V1 uses one logical database

The system SHALL keep the global transaction index and account state in one logical database. Galera SHALL be treated as virtually synchronous, certification-based, multi-primary high availability, not as data or write sharding; cross-shard transactions are out of scope.

#### Scenario: The global index is enforced in one logical database

```gherkin
Given the same transaction ID is submitted for two clients
When the second write is attempted
Then the unique index rejects it
And no cross-shard path exists
```
