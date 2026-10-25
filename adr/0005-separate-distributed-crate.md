# ADR-0005: Separate Distributed Crate

## Status

Accepted

## Date

2026-10-24

## Context and Problem Statement

The graded package exposes exactly one binary and bare `cargo run` must select it. The distributed variant needs several entry points (ingest, partition, worker, export, migrate), but adding `[[bin]]` targets to the existing package would make bare `cargo run` ambiguous and violate the existing packaging contract.

## Considered Options

- Add multiple binaries to the existing package. Rejected: breaks bare `cargo run` and the graded single-binary contract.
- Use `default-run` to disambiguate. Rejected: still adds binaries to the public package and changes its manifest.
- Put the distributed variant in a separate repository. Rejected: heavier review, testing, and release coordination.
- Add a separate crate/workspace member. Chosen: preserves the existing package as the default member while allowing the distributed entry points.

## Decision Outcome

Chosen option: "Add a separate crate/workspace member". The existing package remains the default member with its single binary, bare `cargo run`, and public submission. The distributed crate owns its binaries, migrations, and tests, and is delivered separately or excluded from the sanitized submission.

### Consequences

- Good, because the existing CLI and its packaging contract are untouched.
- Good, because the distributed services can evolve and deploy independently.
- Bad, because the repository becomes multi-crate and needs workspace-aware build, test, and release commands.
- Bad, because the distributed crate needs a separate delivery and documentation path.
- Follow-up: define the exact workspace layout and default-member behaviour in the implementation tasks.
