#!/usr/bin/env python3
"""Page object and process harness for the distributed acceptance suite.

The harness drives the ``distributed`` binary through the job lifecycle and
reads observable results from three places:

* the exported accounts CSV,
* the process exit status and stderr,
* operator commands that report JSON (journal, job status, health, metrics,
  dead-letter path, redacted configuration).

Database setup is limited to seeding canonical client identities so scenarios
that name canonical IDs are deterministic. The expected schema is documented in
``acceptance-tests/distributed/README.md``.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXTRACTED = HERE.parent / ".extracted"
COMPOSE = HERE / "compose.yaml"

# Command exit-code contract for the distributed binary.
EXIT_OK = 0
EXIT_REJECTED = 2
EXIT_FAILED = 3

ACCEPTED = "accepted"
REJECTED = "rejected"
FAILED = "failed"
RUNNING = "running"


def parse_scaled(text):
    """Parse a decimal string into the ledger's 10,000-scaled integer."""
    text = text.strip()
    if not text:
        raise ValueError("empty amount")
    negative = text.startswith("-")
    if negative:
        text = text[1:]
    whole, _, fraction = text.partition(".")
    if not whole:
        whole = "0"
    digits = (fraction + "0000")[:4]
    value = int(whole) * 10_000 + int(digits)
    return -value if negative else value


def format_scaled(value):
    sign = "-" if value < 0 else ""
    value = abs(int(value))
    whole, fraction = divmod(value, 10_000)
    fraction_text = f"{fraction:04d}".rstrip("0") or "0"
    return f"{sign}{whole}.{fraction_text}"


@dataclass
class JobResult:
    status: str | None = None
    failure_reason: str | None = None
    returncode: int | None = None
    stdout: str = ""
    stderr: str = ""
    export_path: Path | None = None
    job_id: str | None = None


@dataclass
class DistributedHarness:
    binary: Path
    backend: str = "postgres"
    workdir: Path = field(init=False)

    def __post_init__(self):
        EXTRACTED.mkdir(parents=True, exist_ok=True)
        self.workdir = Path(
            tempfile.mkdtemp(prefix="distributed-harness-", dir=str(EXTRACTED))
        )
        self.reset()

    # --- lifecycle ---------------------------------------------------------

    def reset(self):
        for child in self.workdir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        self.source = "alpha"
        self.external = None
        self.mappings = {}
        self.identity_rows = []
        self.seeds = {}
        self.files = []
        self.file_count = None
        self.concurrent = False
        self.staged = 0
        self.order = []
        self.owner_duplicates = 0
        self.backpressure = False
        self.file_complete = True
        self.emitted = 0
        self.export_path = self.workdir / "accounts.csv"
        self.result = None
        self.last_key = None
        self.applied_count = 0
        self.exports_match = False
        self.malformed_reported = False

        self.rows = []
        self.rows_flushed = 0
        self.subject = 42
        self.next_tx = 1
        self.job_seq = 0
        self.job_id = None
        self.accounts = {}
        self.export_previous = None
        self.export_snapshot = None
        self.worker_stderr = ""
        self.worker_stdout = ""
        self.last_stderr = ""
        self.last_stdout = ""
        self.redelivery_noop = False
        self.fault = None
        self.extra_env = {}
        self.config_file = None
        self.config_show = None
        self.health = None
        self.metrics_data = None
        self.dlq_events = None
        self.job_state = None
        self.migrate_result = None
        self.migrate_result_second = None
        self.concurrent_ids = []
        self.conformance = None
        self.backend_runs = {}
        self.pending_check = False
        self.reachable = False
        self.stalled = False
        self.missing_broker = False
        self.password = "s3cr3t-distributed"
        self.conflict_attempts = 0
        self.journal_counts = []

    def cleanup(self):
        shutil.rmtree(self.workdir, ignore_errors=True)

    # --- manifest and row construction ------------------------------------

    def add_file(self, ordinal, source=None, rows=None, hash_mismatch=False):
        source = source or self.source
        rows = rows or [f"deposit,1,{ordinal},1.0"]
        path = self.workdir / f"file-{source}-{ordinal}-{len(self.files)}.csv"
        path.write_text(
            "type,client,tx,amount\n" + "\n".join(rows) + "\n", encoding="utf-8"
        )
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if hash_mismatch:
            digest = "0" * 64
        self.files.append(
            {
                "source_id": source,
                "ordinal": ordinal,
                "path": str(path),
                "sha256": digest,
            }
        )

    def ledger_rows(self):
        rows = []
        for row in self.rows:
            amount = "" if row.get("amount") is None else row["amount"]
            rows.append(f"{row['type']},{row['client']},{row['tx']},{amount}")
        return rows

    def flush_rows(self):
        pending = self.rows[self.rows_flushed :]
        if not pending:
            return
        ordinal = len(self.files) + 1
        rows = [
            f"{row['type']},{row['client']},{row['tx']},"
            f"{'' if row.get('amount') is None else row['amount']}"
            for row in pending
        ]
        self.add_file(ordinal, source=self.source, rows=rows)
        self.rows_flushed = len(self.rows)

    def ensure_ledger_file(self):
        self.flush_rows()

    def add_row(self, row_type, client, amount=None, tx=None):
        client = int(client)
        if tx is None:
            tx = self.next_tx
        tx = int(tx)
        self.next_tx = max(self.next_tx, tx + 1)
        self.rows.append(
            {"type": row_type, "client": client, "tx": tx, "amount": amount}
        )
        self.seeds.setdefault((self.source, client), client)
        return self.rows[-1]

    def deposit(self, client, amount, tx=None):
        return self.add_row("deposit", client, str(amount), tx)

    def withdraw(self, client, amount, tx=None):
        return self.add_row("withdrawal", client, str(amount), tx)

    def dispute(self, client, tx):
        return self.add_row("dispute", client, None, tx)

    def resolve(self, client, tx):
        return self.add_row("resolve", client, None, tx)

    def chargeback(self, client, tx):
        return self.add_row("chargeback", client, None, tx)

    def set_mapping(self, source, client, canonical_id):
        self.mappings[(source, client)] = canonical_id
        self.identity_rows.append((source, client, canonical_id))
        self.seeds[(source, client)] = canonical_id

    def canonical_id(self, source, client):
        return self.mappings.get((source, client))

    def manifest(self):
        if self.file_count is not None:
            files = [
                {
                    "source_id": self.source,
                    "ordinal": index + 1,
                    "path": f"/tmp/distributed-file-{index}.csv",
                    "sha256": "0" * 64,
                }
                for index in range(self.file_count)
            ]
        else:
            files = self.files
        return {"source_id": self.source, "files": files}

    # --- process execution -------------------------------------------------

    def _compose(self):
        return ["docker", "compose", "-f", str(COMPOSE)]

    def _exec_sql(self, statement, backend=None):
        backend = backend or self.backend
        if backend == "postgres":
            command = self._compose() + [
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                "ledger",
                "-d",
                "ledger",
                "-v",
                "ON_ERROR_STOP=1",
                "-tAc",
                statement,
            ]
        else:
            command = self._compose() + [
                "exec",
                "-T",
                "mariadb",
                "mariadb",
                "-uledger",
                "-pledger",
                "ledger",
                "-N",
                "-B",
                "-e",
                statement,
            ]
        return subprocess.run(command, capture_output=True, text=True, timeout=60)

    def sql(self, statement, backend=None):
        completed = self._exec_sql(statement, backend=backend)
        if completed.returncode != 0:
            raise RuntimeError(
                completed.stderr.strip()
                or f"database query failed with {completed.returncode}"
            )
        return completed.stdout.strip()

    def seed_identities(self, backend=None):
        for (source, client), canonical in self.seeds.items():
            if (backend or self.backend) == "postgres":
                statement = (
                    "INSERT INTO clients (client_id, source_id, external_client) "
                    f"VALUES ({canonical}, '{source}', {client}) "
                    "ON CONFLICT DO NOTHING"
                )
            else:
                statement = (
                    "INSERT IGNORE INTO clients (client_id, source_id, external_client) "
                    f"VALUES ({canonical}, '{source}', {client})"
                )
            self._exec_sql(statement, backend=backend)

    def _env(self, backend=None, drop_bootstrap=False):
        env = os.environ.copy()
        if drop_bootstrap:
            env.pop("KAFKA_BOOTSTRAP", None)
        else:
            env.setdefault("KAFKA_BOOTSTRAP", "localhost:9092")
        backend = backend or self.backend
        if backend == "postgres":
            env["DATABASE_URL"] = "postgres://ledger:ledger@localhost:5432/ledger"
        else:
            env["DATABASE_URL"] = "mysql://ledger:ledger@localhost:3306/ledger"
        env.update(self.extra_env)
        return env

    def _run(self, args, timeout=120, drop_bootstrap=False, backend=None):
        return subprocess.run(
            [str(self.binary), *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=self._env(drop_bootstrap=drop_bootstrap, backend=backend),
        )

    def _manifest_path(self, job_id):
        path = self.workdir / f"manifest-{job_id}.json"
        path.write_text(json.dumps(self.manifest()), encoding="utf-8")
        return path

    def _new_job_id(self):
        job_id = f"job-{uuid.uuid4().hex[:12]}"
        self.job_seq += 1
        return job_id

    def _record(self, status, completed, stdout, stderr, job_id):
        self.last_stdout = "\n".join(stdout)
        self.last_stderr = "\n".join(stderr)
        self.result = JobResult(
            status=status,
            failure_reason=(completed.stderr or "").strip() or None,
            returncode=completed.returncode,
            stdout=self.last_stdout,
            stderr=self.last_stderr,
            export_path=self.export_path,
            job_id=job_id,
        )
        return self.result

    def _classify(self, stage, completed):
        if completed.returncode == EXIT_REJECTED:
            return REJECTED
        if completed.returncode == EXIT_FAILED:
            return FAILED
        if completed.returncode != EXIT_OK:
            return "error"
        if stage == "ingest" and self._stderr_marks(completed.stderr, "already active"):
            return REJECTED
        return None

    @staticmethod
    def _stderr_marks(stderr, marker):
        return marker in (stderr or "").lower()

    # --- job stages --------------------------------------------------------

    def run_stage(self, stage, job_name=None, manifest_path=None, backend=None):
        job_name = job_name or self.job_id or self._new_job_id()
        self.job_id = job_name
        if stage == "migrate":
            command = ["migrate", "--backend", backend or self.backend, "--json"]
        elif stage == "ingest":
            path = manifest_path or self._manifest_path(job_name)
            command = ["ingest", "--manifest", str(path), "--job", job_name]
        elif stage == "partition":
            command = ["partition", "--job", job_name]
        elif stage == "worker":
            command = ["worker", "--drain", "--job", job_name]
        elif stage == "export":
            command = [
                "export",
                "--job",
                job_name,
                "--out",
                str(self.export_path),
            ]
        elif stage == "export-check":
            command = [
                "export",
                "--job",
                job_name,
                "--out",
                str(self.export_path),
                "--check",
                "--json",
            ]
        else:
            raise ValueError(f"unknown stage {stage}")

        completed = self._run(command, backend=backend)
        if stage == "worker":
            self.worker_stderr = completed.stderr or ""
            self.worker_stdout = completed.stdout or ""
        if stage == "migrate" and completed.returncode == EXIT_OK:
            self.seed_identities(backend=backend)
        status = self._classify(stage, completed)
        self._record(status or ACCEPTED, completed, [completed.stdout], [completed.stderr], job_name)
        self.job_state = status or ACCEPTED
        return completed

    def run_job(self, job_name=None, manifest_path=None, backend=None, stages=None):
        self.ensure_ledger_file()
        job_name = job_name or self._new_job_id()
        self.job_id = job_name
        stages = stages or ["migrate", "ingest", "partition", "worker", "export"]
        stdout, stderr = [], []
        for stage in stages:
            completed = self.run_stage(
                stage,
                job_name=job_name,
                manifest_path=manifest_path,
                backend=backend,
            )
            stdout.append(completed.stdout or "")
            stderr.append(completed.stderr or "")
            if completed.returncode in (EXIT_REJECTED, EXIT_FAILED) or (
                completed.returncode != EXIT_OK
            ):
                return self.result
        self.result = JobResult(
            status=ACCEPTED,
            returncode=EXIT_OK,
            stdout="\n".join(stdout),
            stderr="\n".join(stderr),
            export_path=self.export_path,
            job_id=job_name,
        )
        self.job_state = ACCEPTED
        self.accounts = self.read_export_accounts()
        return self.result

    def run_ledger_job(self, job_name=None, backend=None):
        result = self.run_job(job_name=job_name, backend=backend)
        if result.status == ACCEPTED:
            self.accounts = self.read_export_accounts()
        return result

    # --- export and account state -----------------------------------------

    def read_export(self):
        if not self.export_path.exists():
            return []
        return self.export_path.read_text(encoding="utf-8").splitlines()

    def export_job(self, job_name):
        completed = self.run_stage("export", job_name=job_name)
        if completed.returncode == EXIT_OK:
            self.accounts = self.read_export_accounts()
        return completed

    def read_export_accounts(self):
        lines = self.read_export()
        accounts = {}
        for line in lines[1:]:
            if not line.strip():
                continue
            client, available, held, total, locked = line.split(",")
            accounts[int(client)] = {
                "available": parse_scaled(available),
                "held": parse_scaled(held),
                "total": parse_scaled(total),
                "locked": locked.strip().lower() == "true",
            }
        return accounts

    def account(self, client):
        client = int(client)
        if client in self.accounts:
            return self.accounts[client]
        completed = self._exec_sql(
            "SELECT available, held, locked FROM accounts "
            f"WHERE client_id = {client}"
        )
        if completed.returncode != 0 or not completed.stdout.strip():
            raise AssertionError(
                f"account {client} not found: {completed.stderr.strip()}"
            )
        available, held, locked = completed.stdout.strip().splitlines()[0].split("|")
        available = int(available)
        held = int(held)
        return {
            "available": available,
            "held": held,
            "total": available + held,
            "locked": str(locked).strip().lower() in ("t", "true", "1"),
        }

    def only_client(self):
        if self.accounts:
            return next(iter(self.accounts))
        raise AssertionError("the export contains no account rows")

    def exported_clients(self):
        return set(self.accounts)

    def journal_state(self, tx):
        # journal is an operator command, not a job stage.
        completed = self._run(["journal", "--tx", str(tx), "--json"])
        if completed.returncode != EXIT_OK:
            raise AssertionError(
                f"journal query failed: {completed.stderr.strip()}"
            )
        return json.loads(completed.stdout).get("state")

    def journal_count(self, backend=None):
        completed = self._exec_sql(
            "SELECT COUNT(*) FROM transactions", backend=backend
        )
        if completed.returncode != 0:
            raise AssertionError(completed.stderr.strip())
        return int(completed.stdout.strip().splitlines()[0])

    def migrate(self, backend=None, fresh=False):
        args = ["migrate", "--backend", backend or self.backend, "--json"]
        if fresh:
            args.append("--fresh")
        return self._run(args, backend=backend)

    def worker_summary(self):
        for line in self.worker_stdout.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
        return {}

    # --- job status, health, metrics, DLQ ---------------------------------

    def _json_command(self, args, drop_bootstrap=False):
        completed = self._run(args, drop_bootstrap=drop_bootstrap)
        return completed

    def read_job_state(self, job_name=None):
        job_name = job_name or self.job_id
        completed = self._json_command(
            ["job", "status", "--job", job_name, "--json"]
        )
        if completed.returncode != EXIT_OK:
            raise AssertionError(f"job status failed: {completed.stderr.strip()}")
        self.job_state = json.loads(completed.stdout)
        return self.job_state

    def check_export(self, job_name=None):
        job_name = job_name or self.job_id
        completed = self._json_command(
            [
                "export",
                "--job",
                job_name,
                "--out",
                str(self.export_path),
                "--check",
                "--json",
            ]
        )
        if completed.returncode != EXIT_OK:
            raise AssertionError(f"export check failed: {completed.stderr.strip()}")
        return json.loads(completed.stdout)

    def health_check(self, drop_bootstrap=False):
        completed = self._json_command(["health", "--json"], drop_bootstrap=drop_bootstrap)
        self.health = {
            "returncode": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
        return self.health

    def health_json(self, drop_bootstrap=False):
        health = self.health_check(drop_bootstrap=drop_bootstrap)
        try:
            return json.loads(health["stdout"])
        except json.JSONDecodeError:
            return {}

    def read_metrics(self):
        completed = self._json_command(["metrics", "--json"])
        if completed.returncode != EXIT_OK:
            raise AssertionError(f"metrics failed: {completed.stderr.strip()}")
        self.metrics_data = json.loads(completed.stdout)
        return self.metrics_data

    def read_dlq(self, job_name=None):
        job_name = job_name or self.job_id
        completed = self._json_command(
            ["dlq", "list", "--job", job_name, "--json"]
        )
        if completed.returncode != EXIT_OK:
            raise AssertionError(f"dlq query failed: {completed.stderr.strip()}")
        self.dlq_events = json.loads(completed.stdout)
        return self.dlq_events

    def show_config(self):
        completed = self._json_command(["config", "show", "--json"])
        if completed.returncode != EXIT_OK:
            raise AssertionError(f"config show failed: {completed.stderr.strip()}")
        self.config_show = json.loads(completed.stdout)
        return self.config_show

    # --- multi-backend helpers --------------------------------------------

    def ensure_backend_running(self, backend):
        completed = subprocess.run(
            self._compose()
            + [
                "--profile",
                backend,
                "up",
                "-d",
                "--wait",
                "--wait-timeout",
                "120",
            ],
            capture_output=True,
            text=True,
            timeout=240,
        )
        if completed.returncode != 0:
            raise AssertionError(
                f"could not start the {backend} profile: {completed.stderr.strip()}"
            )

    def run_on_backend(self, backend):
        self.backend_runs[backend] = {
            "balances": dict(self.accounts),
            "journal": self.journal_count(backend=backend),
        }
        return self.backend_runs[backend]

    def run_conformance_both(self):
        for backend in ("postgres", "mariadb"):
            self.ensure_backend_running(backend)
            self.run_ledger_job(job_name=f"conformance-{backend}", backend=backend)
            self.run_on_backend(backend)
        return self.backend_runs

    def compare_backends(self):
        first, second = self.backend_runs.get("postgres"), self.backend_runs.get(
            "mariadb"
        )
        if first is None or second is None:
            raise AssertionError(
                "the ledger conformance suite must run on PostgreSQL and MariaDB"
            )
        if first["balances"] != second["balances"]:
            raise AssertionError(f"balances differ: {first} != {second}")
        if first["journal"] != second["journal"]:
            raise AssertionError(f"journal differs: {first} != {second}")

    def migrate_both(self, fresh=False):
        results = {}
        for backend in ("postgres", "mariadb"):
            args = ["migrate", "--backend", backend, "--json"]
            if fresh:
                args.append("--fresh")
            results[backend] = self._run(args)
        return results

    def table_exists(self, table, backend):
        if backend == "postgres":
            statement = (
                "SELECT COUNT(*) FROM information_schema.tables "
                f"WHERE table_name = '{table}'"
            )
        else:
            statement = (
                "SELECT COUNT(*) FROM information_schema.tables "
                f"WHERE table_schema = 'ledger' AND table_name = '{table}'"
            )
        completed = self._exec_sql(statement, backend=backend)
        if completed.returncode != 0:
            raise AssertionError(completed.stderr.strip())
        return int(completed.stdout.strip().splitlines()[0]) > 0

    def duplicate_tx_rejected(self, backend):
        self.seeds.setdefault((self.source, 42), 42)
        self.seeds.setdefault((self.source, 43), 43)
        self.seed_identities(backend=backend)
        statement = (
            "INSERT INTO transactions (tx_id, client_id, amount, state) "
            "VALUES (9001, 42, 10000, 'settled')"
        )
        statement2 = (
            "INSERT INTO transactions (tx_id, client_id, amount, state) "
            "VALUES (9001, 43, 10000, 'settled')"
        )
        # The first insert may already exist from a prior scenario.
        first = self._exec_sql(statement, backend=backend)
        if first.returncode != 0 and not any(
            marker in first.stderr.lower()
            for marker in ("unique", "duplicate", "primary")
        ):
            raise AssertionError(first.stderr.strip())
        completed = self._exec_sql(statement2, backend=backend)
        return completed.returncode != 0

    def allocate_concurrently(self, count):
        """Mint identities concurrently through the partitioner's identity command."""
        env = self._env()
        processes = [
            subprocess.Popen(
                [
                    str(self.binary),
                    "identity",
                    "mint",
                    "--source",
                    self.source,
                    "--client",
                    str(index + 1),
                    "--json",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )
            for index in range(count)
        ]
        ids = []
        for process in processes:
            stdout, stderr = process.communicate(timeout=60)
            if process.returncode != EXIT_OK:
                raise AssertionError(f"identity mint failed: {stderr.strip()}")
            ids.append(json.loads(stdout)["client_id"])
        self.concurrent_ids = ids
        return ids

    # --- scenario controls -------------------------------------------------

    def start_job(self, job_name=None):
        self.ensure_ledger_file()
        job_name = job_name or self._new_job_id()
        self.job_id = job_name
        self.run_stage("migrate", job_name=job_name)
        completed = self.run_stage("ingest", job_name=job_name)
        return completed

    def submit_job(self, job_name=None, manifest_path=None):
        job_name = job_name or self._new_job_id()
        completed = self.run_stage(
            "ingest", job_name=job_name, manifest_path=manifest_path
        )
        return completed

    def finish_job(self, job_name=None):
        job_name = job_name or self.job_id
        self.run_stage("partition", job_name=job_name)
        self.run_stage("worker", job_name=job_name)
        self.run_stage("export", job_name=job_name)
        self.accounts = self.read_export_accounts()
        return self.result

    def redeliver(self):
        before = dict(self.accounts)
        completed = self._run(
            ["worker", "--drain", "--job", self.job_id, "--redeliver"]
        )
        self.worker_stderr = completed.stderr or ""
        if completed.returncode != EXIT_OK:
            raise AssertionError(
                f"redelivery failed: {completed.stderr.strip()}"
            )
        self.accounts = self.read_export_accounts()
        self.redelivery_noop = before == self.accounts
        return self.redelivery_noop

    def pending_rows(self):
        return len(self.rows) - self.rows_flushed

    def assert_row_rejected(self, *markers):
        if self.result is not None and self.result.status == FAILED:
            raise AssertionError("the job failed instead of rejecting a row")
        text = f"{self.worker_stderr}\n{self.last_stderr}".lower()
        if not any(marker.lower() in text for marker in markers):
            raise AssertionError(
                "expected a rejection containing one of "
                f"{markers} in the worker output; got:\n{text}"
            )

    def warnings(self):
        lines = []
        for line in self.worker_stderr.splitlines():
            lowered = line.lower()
            if "account locked" in lowered and ("warn" in lowered or "lock" in lowered):
                lines.append(line)
        return lines

    def warning_count(self):
        return len(self.warnings())
