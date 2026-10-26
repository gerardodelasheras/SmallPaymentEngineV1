# Job Reporting Specification

## Purpose

Serialized batch jobs that export a complete, job-scoped accounts CSV only after every partition and file watermark has drained.

## ADDED Requirements

### Requirement: Export waits for the job to drain

The system SHALL export the job accounts CSV only after every transaction partition has processed all events emitted for the job. A lagging partition SHALL delay the export rather than produce a partial report.

#### Scenario: A lagging partition delays the export

```gherkin
Given one transaction partition still has unprocessed job events
When the exporter checks the job
Then no CSV is written
And the job remains running
```

### Requirement: The export is job-scoped and uses canonical client IDs

The system SHALL write `client,available,held,total,locked` for the clients touched by the job, using canonical `BIGINT` client IDs, including clients initialized by rejected rows. It SHALL NOT include clients untouched by the job.

#### Scenario: Only touched clients are exported

```gherkin
Given job A touches clients 42 and 43
And client 99 is touched by another job
When job A is exported
Then the CSV contains clients 42 and 43
And the CSV does not contain client 99
```

#### Scenario: A rejected row's client appears with zero balances

```gherkin
Given a client's only job row is a rejected withdrawal
When the job is exported
Then the client appears with available 0.0
And the client's held is 0.0
And the client's total is 0.0
And the client is not locked
```

### Requirement: One active job at a time

The system SHALL allow at most one active batch job. A second submission while a job is active SHALL be rejected as job already active; after the active job reaches a terminal state, a new job may start.

#### Scenario: A second job is rejected

```gherkin
Given job A is running
When job B is submitted
Then job B is rejected as job already active
And job A is unaffected
```

### Requirement: Job failure is terminal and partial output is never written

The system SHALL mark a job failed when an unrecoverable file or manifest error occurs, SHALL write no accounts CSV for that job, and SHALL allow a later job to start after the failure.

#### Scenario: A hash mismatch fails the job

```gherkin
Given a manifest with a hash mismatch
When the job runs
Then the job is marked failed
And no accounts CSV is written
And a later valid job can start
```

### Requirement: Re-submitting the same manifest is idempotent

The system SHALL not duplicate ledger effects when the same manifest or file content is submitted again. A re-submitted manifest SHALL produce the same exported state for the touched clients and SHALL not double-count balances.

#### Scenario: The same manifest produces the same state

```gherkin
Given a manifest has completed and was exported
When the same manifest is submitted again
Then the exported balances are unchanged
And no transaction is applied twice
```
