#!/usr/bin/env python3
"""Page object and process harness for the distributed acceptance suite."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXTRACTED = HERE.parent / ".extracted"


@dataclass
class JobResult:
    status: str | None = None
    failure_reason: str | None = None
    returncode: int | None = None
    stdout: str = ""
    stderr: str = ""
    export_path: Path | None = None


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

    def add_file(self, ordinal, source=None, rows=None, hash_mismatch=False):
        source = source or self.source
        rows = rows or [f"deposit,1,{ordinal},1.0"]
        path = self.workdir / f"file-{source}-{ordinal}-{len(self.files)}.csv"
        path.write_text("type,client,tx,amount\n" + "\n".join(rows) + "\n", encoding="utf-8")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if hash_mismatch:
            digest = "0" * 64
        self.files.append(
            {"source_id": source, "ordinal": ordinal, "path": str(path), "sha256": digest}
        )

    def set_mapping(self, source, client, canonical_id):
        self.mappings[(source, client)] = canonical_id
        self.identity_rows.append((source, client, canonical_id))

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

    def _run(self, args, timeout=60):
        env = os.environ.copy()
        env.setdefault("KAFKA_BOOTSTRAP", "localhost:9092")
        if self.backend == "postgres":
            env.setdefault("DATABASE_URL", "postgres://ledger:ledger@localhost:5432/ledger")
        else:
            env.setdefault("DATABASE_URL", "mysql://ledger:ledger@localhost:3306/ledger")
        return subprocess.run(
            [str(self.binary), *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )

    def run_job(self):
        manifest_path = self.workdir / "manifest.json"
        manifest_path.write_text(json.dumps(self.manifest()), encoding="utf-8")
        commands = [
            ["migrate", "--backend", self.backend],
            ["ingest", "--manifest", str(manifest_path)],
            ["partition"],
            ["worker", "--drain"],
            ["export", "--job", "test-job", "--out", str(self.export_path)],
        ]
        stdout, stderr = [], []
        for command in commands:
            try:
                completed = self._run(command)
            except Exception as error:  # noqa: BLE001 - surfaced as a job failure
                self.result = JobResult(
                    status="error",
                    failure_reason=str(error),
                    returncode=None,
                    stdout="\n".join(stdout),
                    stderr="\n".join(stderr),
                    export_path=self.export_path,
                )
                return self.result
            stdout.append(completed.stdout)
            stderr.append(completed.stderr)
            if completed.returncode != 0:
                self.result = JobResult(
                    status="error",
                    failure_reason=completed.stderr.strip()
                    or f"{command[0]} exited {completed.returncode}",
                    returncode=completed.returncode,
                    stdout="\n".join(stdout),
                    stderr="\n".join(stderr),
                    export_path=self.export_path,
                )
                return self.result
        self.result = JobResult(
            status="accepted",
            returncode=0,
            stdout="\n".join(stdout),
            stderr="\n".join(stderr),
            export_path=self.export_path,
        )
        return self.result

    def read_export(self):
        if not self.export_path.exists():
            return []
        return self.export_path.read_text(encoding="utf-8").splitlines()

    def cleanup(self):
        shutil.rmtree(self.workdir, ignore_errors=True)
