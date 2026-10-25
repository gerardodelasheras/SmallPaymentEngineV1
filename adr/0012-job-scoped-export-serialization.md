# ADR-0012: Job-Scoped Export with Serialized Jobs

## Status

Accepted

## Date

2026-10-24

## Context and Problem Statement

The distributed variant must produce an accounts CSV for a batch job after all of its events are applied. If another job mutates the same clients concurrently, a current-state export is not a deterministic job result. The first version also needs a clear completion, failure, and resubmission contract.

## Considered Options

- Export current state on demand. Rejected: concurrent jobs make the result non-deterministic.
- Allow concurrent jobs and materialize per-job account snapshots. Rejected: more tables, write amplification, and a larger first version.
- Run only one active batch job and export after it drains. Chosen: deterministic output with the smallest viable state model.

## Decision Outcome

Chosen option: "Run only one active batch job and export after it drains". A `job_lock` singleton row rejects a second active submission as `JobAlreadyActive`. The exporter waits until every transaction partition has processed all emitted events, then reads `job_clients` and accounts and writes `client,available,held,total,locked` with canonical `BIGINT` IDs. Clients initialized by rejected rows are included. An unrecoverable manifest/file error marks the job failed, writes no CSV, and releases the lock. Re-submitting a completed manifest is idempotent.

### Consequences

- Good, because a job's export is deterministic and includes only clients touched by that job.
- Good, because failure cannot produce a partial CSV.
- Good, because the first version avoids per-job ledger snapshots.
- Bad, because batch concurrency is limited to one job at a time.
- Bad, because a failed job needs operator action or resubmission before the next job.
- Bad, because the job-lock row is a serialization point and can contend under Galera.
- Follow-up: consider per-job account snapshots or per-client job leases if batch concurrency becomes necessary.
