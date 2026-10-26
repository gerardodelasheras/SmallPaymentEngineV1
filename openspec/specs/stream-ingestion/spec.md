# Stream Ingestion Specification

## Purpose

Manifest-driven, parallel ingestion that preserves per-client order across files and makes partial or repeated ingestion idempotent.

## Requirements

### Requirement: The manifest defines source, order, and integrity

The system SHALL accept a job manifest whose entries carry a source identifier, a unique non-negative file ordinal, a file hash, and a location. It SHALL reject a manifest with duplicate ordinals or a missing file, and SHALL fail the job on a hash mismatch before emitting ledger events.

#### Scenario: A valid manifest starts a job

```gherkin
Given a manifest with files at ordinals 1 and 2 and matching hashes
When the job is submitted
Then the job is accepted
And both files are staged before ordering is applied
```

#### Scenario: A duplicate ordinal is rejected

```gherkin
Given a manifest with two files at ordinal 1
When the job is submitted
Then the job is rejected
And no events are emitted
```

#### Scenario: A hash mismatch fails the job

```gherkin
Given a manifest whose file hash does not match the file content
When the job runs
Then the job is marked failed
And no partial accounts CSV is exported
```

### Requirement: Parallel parsing stages complete transaction records

The system SHALL parse manifest files in parallel and stage each valid CSV row with the parsed transaction fields and `(job_id, source_id, external_client, file_ord, row_ord)`. A malformed row SHALL be quarantined or skipped without stopping other rows or files.

#### Scenario: Valid rows from many files are staged

```gherkin
Given a manifest with 1,000 files
When ingestion runs in parallel
Then every valid row is staged with its file and row coordinates
```

#### Scenario: A malformed row does not stop the file

```gherkin
Given a file with a malformed row between two valid rows
When ingestion runs
Then both valid rows are staged
And the malformed row is reported
```

### Requirement: Per-client order is preserved across overlapping files

The system SHALL emit events for a canonical client in ascending `(file_ord, row_ord)` order, even when the files that contain that client are parsed out of order.

#### Scenario: A later file waits for an earlier file

```gherkin
Given file 1 and file 17 both contain source "alpha" client 7
And file 17 finishes parsing before file 1
When the partitioner emits client 7
Then file 1's rows are emitted before file 17's rows
And the final account state reflects that order
```

### Requirement: One partitioner owns each canonical client

The system SHALL route all events for a canonical client through one partitioner owner at a time, and SHALL bound per-client buffers with backpressure.

#### Scenario: Two partitioners do not interleave one client

```gherkin
Given two partitioner instances and several clients
When the partitioners run
Then every event for one client is emitted by exactly one owner
And no client's rows are interleaved or duplicated
```

#### Scenario: A full buffer applies backpressure

```gherkin
Given a client with more buffered rows than the configured limit
When more rows arrive
Then ingestion pauses rather than growing the buffer without bound
```

### Requirement: File completion is durable and re-ingestion is idempotent

The system SHALL mark a file complete only after all its rows are staged, SHALL persist that watermark, and SHALL use content-addressed event identity so re-staging the same file does not duplicate ledger effects.

#### Scenario: A restart resumes after a partial file

```gherkin
Given ingestion crashes after staging half of a file
When it restarts
Then the file is not complete
And the partitioner does not emit that file's rows until they are fully staged
```

#### Scenario: Re-ingesting the same file is idempotent

```gherkin
Given a completed file is submitted again under a later job
When the job runs
Then no ledger effect is applied twice
And the later job's export matches the earlier state
```
