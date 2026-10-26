# Council Notes: proposal.md

## Author Summary

The initial draft proposed five new capabilities for a Kafka-partitioned, DB-backed variant: canonical client identity, manifest ingestion with ordering watermarks, stateless consumer workers, job-scoped reporting, and PostgreSQL/MariaDB-Galera portability. It kept the existing CLI unchanged and framed the variant as additive and correctness-first.

## Reviewer Challenges

- The claim that the existing package could gain "new binaries alongside the existing one" while keeping bare `cargo run` contradicts the graded single-binary packaging constraint.
- The claim that the ledger rules were "already factored" and that the library surface would not change contradicts the `u16` `ClientId` and the stateful `TransactionProcessor`.
- The terminal `charged_back` journal diverges from the in-memory chargeback deletion; the divergence had no owning requirement.
- The export's client identity and "touched" definition were undefined, including the existing zeroed-initialization-on-rejection rule.
- The external key domain was unspecified, so source-scoped collisions and the `u16` parser boundary were unaddressed.
- Per-client ordering had no single-owner rule for the partitioner.
- A permanently failing file could block a serialized job forever.
- "Horizontally scaled" overstated write capacity given a single logical DB.
- Operational behavior (config, credentials, monitoring, DLQ) had no capability home.
- "Reuse by reference" is not expressible as mergeable delta scenarios.
- Staging payload, manifest failure behavior, event-ID scope, warning client identity, and the ordering key were underspecified.
- Impact omitted packaging/build implications, journal/idempotency growth, ADR impacts, and the strongest cross-variant oracle.

## Resolutions

- Accepted: the distributed variant is a separate crate/workspace member; the existing package keeps its single binary and bare `cargo run`. The public submission remains the existing crate.
- Accepted: the library API is explicitly **BREAKING** because `ClientId`, processor internals, and the shared core change; the CLI behavior is unchanged.
- Accepted: a shared core is extracted; `Account` arithmetic is reused, while the state machine and index/journal boundary are generalized.
- Accepted: the terminal journal divergence is owned by `distributed-ledger` and documented, not hidden behind "reuse by reference."
- Accepted: export uses canonical `BIGINT` client IDs and includes clients initialized by rejected rows.
- Accepted: external keys are source-scoped `(source_id, u16 client)`, preserving the CSV parser while avoiding cross-source collisions.
- Accepted: the partitioner owns each canonical client through one staging partition and uses bounded buffers.
- Accepted: manifest failures and unrecoverable files produce a terminal failed job with no partial export; a second active job is rejected.
- Accepted: event identity is content-addressed for re-ingestion idempotency, separate from job progress.
- Accepted: ordering is `(file_ord, row_ord)`; the warning names the canonical client ID.
- Accepted: scale language now says consumer parallelism grows with partitions, while write capacity is capped by the single logical DB.
- Accepted: `distributed-operations` is a sixth capability for config/credentials, migrations, retry/DLQ, and health/metrics.
- Accepted: the new specs carry their own scenarios, with a cross-variant conformance suite instead of "reuse by reference."
- Accepted: Impact names packaging/build implications, unbounded journal/idempotency retention, ADR impacts, and the CLI-vs-distributed oracle.
- Rejected: adding binaries to the existing package or using `default-run` as the packaging answer.
- Deferred: exact topic partition counts, ID-allocator block size, buffer bounds, retention policy, and gateway cutover, which belong in design/tasks.

## Remaining Risks

- Shared-core extraction can regress the CLI; a chargeback-then-reuse regression test is required.
- The global transaction index and single logical DB remain write-throughput ceilings.
- Galera improves availability, not write sharding.
- Partitioner buffers and staging tables need explicit bounds/backpressure.
- Full journal and idempotency records grow without a v1 retention policy.
- Serialized jobs reduce batch concurrency and require operator-visible failure recovery.
- Rekeying clients when Kafka partition counts change must be planned.
