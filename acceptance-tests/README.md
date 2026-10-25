# Acceptance tests

These tests execute the Gherkin scenarios in `openspec/` against the compiled
`transaction_processor` binary.

## Setup

```sh
uv venv acceptance-tests/.venv
uv pip install --python acceptance-tests/.venv/bin/python -r acceptance-tests/requirements.txt
```

## Run

```sh
acceptance-tests/.venv/bin/python acceptance-tests/run_acceptance.py
```

The suite builds the binary, extracts every `spec.md` under `openspec/` into
`acceptance-tests/.extracted/`, and runs behave. An HTML report is written to
`acceptance-tests/reports/behave-report.html`.

Other commands:

```sh
acceptance-tests/.venv/bin/python acceptance-tests/run_acceptance.py --specs
acceptance-tests/.venv/bin/python acceptance-tests/run_acceptance.py --lint
acceptance-tests/.venv/bin/python acceptance-tests/run_acceptance.py --print-locations
```

`.extracted/`, `reports/`, and `.venv/` are generated and gitignored.

## Distributed suite

The distributed change has its own Behave project under `acceptance-tests/distributed/`.
The CLI runner excludes every change listed in `acceptance-tests/distributed/scope.txt`,
so the existing CLI suite stays focused on the single-node processor.

Run the CLI suite:

```sh
acceptance-tests/.venv/bin/python acceptance-tests/run_acceptance.py
```

Run the distributed suite:

```sh
acceptance-tests/.venv/bin/python acceptance-tests/distributed/run_distributed.py \
  --backend postgres \
  --capability client-identity \
  --capability stream-ingestion
```

The distributed runner starts the compose profile (Kafka plus PostgreSQL or MariaDB)
unless `--no-compose` is supplied. It writes
`acceptance-tests/distributed/reports/behave-report.html`. Pass `--keep` to leave the
containers running after the run.
