#!/usr/bin/env python3
"""Run the distributed Behave suite from the active change's extracted specs."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ACCEPTANCE = HERE.parent
ROOT = ACCEPTANCE.parent
sys.path.insert(0, str(ACCEPTANCE))

from extract_gherkin import extract_all  # noqa: E402


def change_name():
    scope = HERE / "scope.txt"
    for line in scope.read_text(encoding="utf-8").splitlines():
        name = line.strip()
        if name and not name.startswith("#"):
            return name
    raise SystemExit("acceptance-tests/distributed/scope.txt has no change name")


def compose_command():
    base = ["docker", "compose", "-f", str(HERE / "compose.yaml")]
    try:
        subprocess.run(base + ["version"], check=True, capture_output=True)
        return base
    except Exception:
        return ["sudo", "-n"] + base


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("postgres", "mariadb"), default="postgres")
    parser.add_argument("--capability", action="append", default=[])
    parser.add_argument("--no-compose", action="store_true")
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("behave_args", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    change = change_name()
    out_dir, _written = extract_all()
    specs = Path(out_dir) / "changes" / change / "specs"
    features = sorted(specs.rglob("*.feature"))
    if args.capability:
        features = [path for path in features if path.parent.name in set(args.capability)]
    if not features:
        sys.stderr.write(f"[distributed-acceptance] no features found under {specs}\n")
        return 1

    features_dir = HERE / "features"
    shutil.rmtree(features_dir, ignore_errors=True)
    copied = []
    for path in features:
        destination = features_dir / path.relative_to(specs)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
        copied.append(destination)
    features = copied

    (HERE / "reports").mkdir(exist_ok=True)
    subprocess.run(
        ["cargo", "build", "--quiet", "-p", "distributed"],
        cwd=ROOT,
        check=True,
    )

    backend = args.backend
    database_url = (
        "postgres://ledger:ledger@localhost:5432/ledger"
        if backend == "postgres"
        else "mysql://ledger:ledger@localhost:3306/ledger"
    )
    env = os.environ.copy()
    env["OPENSPEC_ACCEPTANCE"] = "1"
    env["DISTRIBUTED_BIN"] = str(ROOT / "target" / "debug" / "distributed")
    env["DISTRIBUTED_BACKEND"] = backend
    env["KAFKA_BOOTSTRAP"] = "localhost:9092"
    env["DATABASE_URL"] = database_url

    compose = None
    if not args.no_compose:
        compose = compose_command()
        subprocess.run(
            compose
            + ["--profile", backend, "up", "-d", "--wait", "--wait-timeout", "90"],
            check=True,
        )

    # Reset the schema and recreate the Kafka topics once per suite. A cold
    # broker can take a while to elect topic leaders, so retry the setup instead
    # of failing the whole suite on the first attempt.
    reset_command = [
        str(ROOT / "target" / "debug" / "distributed"),
        "migrate",
        "--backend",
        backend,
        "--fresh",
        "--fresh-topics",
        "--json",
    ]
    last_error = None
    for attempt in range(1, 6):
        completed = subprocess.run(
            reset_command,
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if completed.returncode == 0:
            break
        last_error = completed.stderr.strip()
        sys.stderr.write(
            "[distributed-acceptance] suite reset attempt %d failed: %s\n"
            % (attempt, last_error)
        )
        time.sleep(3)
    else:
        raise SystemExit(f"suite reset failed: {last_error}")

    try:
        command = [
            sys.executable,
            "-m",
            "behave",
            "--no-capture",
            *[os.path.relpath(path, HERE) for path in features],
            *args.behave_args,
        ]
        sys.stderr.write(
            "[distributed-acceptance] backend=%s features=%d change=%s\n"
            % (backend, len(features), change)
        )
        completed = subprocess.run(command, cwd=HERE, env=env)
        sys.stderr.write(
            "[distributed-acceptance] report=%s\n" % (HERE / "reports" / "behave-report.html")
        )
        return completed.returncode
    finally:
        if compose is not None and not args.keep:
            subprocess.run(compose + ["--profile", backend, "down"], check=False)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
