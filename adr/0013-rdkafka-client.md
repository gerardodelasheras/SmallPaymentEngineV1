# ADR-0013: Use rdkafka for Kafka Client Features

## Status

Accepted

## Date

2026-10-25

## Context and Problem Statement

The partitioner and worker need Kafka consumer groups, partition ownership and rebalancing, offset commits after durable work, idempotent production, and admin topic management. The first implementation used `rskafka` because it is pure Rust and has no native build dependency.

`rskafka` is explicitly a minimal client. Its README states that it has no support for offset tracking, consumer groups, or transactions. It can produce to and consume from a specific partition and create topics, but it cannot provide the consumer-group ownership or committed-offset semantics that the distributed ledger design depends on. The pure-Rust `kafka` crate also lacks dynamic group membership; it supports manual assignments and group-offset storage only.

## Considered Options

- **Keep `rskafka` with manual partition assignment and database-stored offsets.** Rejected: each replica would be pinned to explicit partitions, rebalancing would be manual, and the design's "consumer group plus offset commit after DB commit" model would change. It also requires a new durable offset table and an operational scaling procedure.
- **Implement the Kafka consumer-group protocol on top of `rskafka`.** Rejected: join/sync/heartbeat/rebalance/offset-commit coordination is high-risk protocol work that is not part of the ledger problem.
- **Use `rdkafka` (the Rust wrapper around librdkafka) for producer, consumer, and admin.** Chosen: it provides consumer groups, rebalancing, committed offsets, idempotent producers, and topic administration.
- **Use `rskafka` for admin and `rdkafka` only for consuming/producing.** Rejected: two Kafka clients, two configuration paths, and two failure modes for little benefit.
- **Use another pure-Rust client.** Rejected for this version: none of the available crates provide dynamic consumer-group membership and offset commits together.

## Decision Outcome

Chosen option: "Use `rdkafka` for producer, consumer, and admin." The distributed crate uses `rdkafka` 0.37 with dynamic linking against the system librdkafka.

The pin is deliberate. `rdkafka` 0.39 depends on `rdkafka-sys` 4.10, which requires librdkafka 2.12 or newer; Debian trixie provides librdkafka 2.8. `rdkafka` 0.37 uses `rdkafka-sys` 4.8.0+2.3.0, which accepts the system 2.8 library. `Cargo.lock` pins `rdkafka-sys` to `4.8.0+2.3.0` so a later resolution cannot silently raise the native requirement.

Producers enable `enable.idempotence=true` with `acks=all`. Consumers disable automatic commits and commit offsets explicitly after the transaction/progress work. The admin client performs idempotent topic creation and the destructive topic recreation used by `migrate --fresh`.

Build and runtime requirements:

- build: `librdkafka-dev`, `pkg-config`, a C compiler
- runtime: `librdkafka1` (or the equivalent shared library in the deployment image)
- alternative: static linking or `cmake-build`, at the cost of a longer build

### Consequences

- Good, because the partitioner and worker get real consumer-group ownership, rebalancing, and offset commits.
- Good, because the offset-commit-after-durable-work model in ADR-0009 is implementable without a custom group protocol.
- Good, because idempotent production and admin topic management come from the same client.
- Bad, because the distributed service now has a native C dependency at build time and runtime.
- Bad, because the crate version is coupled to the system librdkafka version; upgrading either side needs a coordinated change.
- Bad, because container images must include librdkafka or link it statically.
- Bad, because synchronous offset commits block the async loop; they may need to move to a blocking task under higher load.
- Bad, because the pure-Rust build simplicity of `rskafka` is lost.
- Follow-up: document the native dependency in the distributed operations guide, pin the image library version, and revisit a pure-Rust client if one gains consumer-group and offset-commit support.
