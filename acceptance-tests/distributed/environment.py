"""Behave hooks for the distributed acceptance suite."""

from __future__ import annotations

import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from harness import DistributedHarness  # noqa: E402


def before_all(context):
    if not os.environ.get("OPENSPEC_ACCEPTANCE"):
        raise RuntimeError(
            "Run the distributed suite via `python acceptance-tests/distributed/run_distributed.py`."
        )
    root = HERE.parent.parent
    (HERE / "reports").mkdir(exist_ok=True)
    context.repo_root = root
    context.harness = DistributedHarness(
        binary=Path(os.environ.get("DISTRIBUTED_BIN", root / "target" / "debug" / "distributed")),
        backend=os.environ.get("DISTRIBUTED_BACKEND", "postgres"),
    )


def before_scenario(context, scenario):
    context.harness.reset()


def after_all(context):
    context.harness.cleanup()
