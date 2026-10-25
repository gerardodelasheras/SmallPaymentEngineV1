# ADR-0008: Ordered Manifest and Watermarks

## Status

Accepted

## Date

2026-10-24

## Context and Problem Statement

Files may be parsed in parallel and may overlap by client. The CSV has no event timestamp. To preserve per-client input order, the pipeline needs an explicit order, a durable completion signal, and a single owner per client. Arrival order would let a later file overtake an earlier one.

## Considered Options

- Process files sequentially. Rejected: loses parallel ingestion for thousands of files.
- Order by event time. Rejected: the CSV has no timestamp.
- Use arrival order. Rejected: overlapping files reorder a client's transactions.
- Use an ordered manifest with durable file watermarks and a partitioner that owns each client. Chosen: parallel parsing with deterministic per-client order.

## Decision Outcome

Chosen option: "Use an ordered manifest with durable file watermarks and a partitioner that owns each client". Each manifest entry carries a source, file ordinal, and hash. Ingest stages parsed rows and marks a file complete only after all rows are staged. The partitioner buffers each source-scoped client and emits in ascending `(file_ord, row_ord)` order once earlier files are complete. All events for a source-scoped client are keyed to one staging partition, so one owner emits them. Buffers are bounded with backpressure. Event identity is content-addressed from `(source_id, file_hash, row_ord)`, making re-ingestion idempotent.

### Consequences

- Good, because parallel ingestion preserves deterministic per-client order across overlapping files.
- Good, because a restart resumes from durable watermarks instead of reordering in memory.
- Good, because content-addressed events make re-submitting a file safe.
- Bad, because the partitioner is stateful and its buffers must be bounded and monitored.
- Bad, because a stalled or missing file can block later files for its clients until the job fails or is repaired.
- Bad, because manifest integrity and file lifecycle become part of the operator contract.
- Follow-up: define the manifest format, buffer bounds, and job-timeout/failure policy.
