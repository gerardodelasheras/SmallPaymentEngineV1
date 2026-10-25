#!/usr/bin/env python3
"""Acceptance suite entry point for the Python stack."""

import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

from extract_gherkin import ExtractionError, extract_all  # noqa: E402
from openspec_effective_spec import CompositionError, effective_locations, source_of_truth_locations  # noqa: E402


def main(argv):
    os.chdir(HERE)

    flags = ("--specs", "--print-locations", "--lint")
    specs_only = "--specs" in argv
    print_only = "--print-locations" in argv
    lint = "--lint" in argv

    exclude_changes = []
    passthrough = []
    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg == "--exclude-change":
            if index + 1 >= len(argv):
                sys.stderr.write("--exclude-change requires a change name\n")
                return 2
            exclude_changes.append(argv[index + 1])
            index += 2
            continue
        if arg.startswith("--exclude-change="):
            exclude_changes.append(arg.split("=", 1)[1])
            index += 1
            continue
        if arg not in flags:
            passthrough.append(arg)
        index += 1

    scope_file = HERE / "distributed" / "scope.txt"
    if scope_file.exists():
        for line in scope_file.read_text(encoding="utf-8").splitlines():
            name = line.strip()
            if name and not name.startswith("#") and name not in exclude_changes:
                exclude_changes.append(name)

    try:
        out_dir, written = extract_all()
    except ExtractionError as err:
        sys.stderr.write("[extract-gherkin] %s\n" % err)
        return 1
    sys.stderr.write("[extract-gherkin] %d spec.md file(s) extracted to %s\n" % (len(written), out_dir))

    if lint:
        return subprocess.call(["npx", "gherkin-lint", ".extracted"])

    try:
        locations = (
            source_of_truth_locations()
            if specs_only
            else effective_locations(exclude_changes=exclude_changes)
        )
    except CompositionError as err:
        sys.stderr.write("[effective-spec] %s\n" % err)
        return 1

    if print_only:
        for location in locations:
            print(location)
        return 0

    if not locations:
        sys.stderr.write("[effective-spec] no specs to run\n")
        return 1

    os.environ["OPENSPEC_ACCEPTANCE"] = "1"
    os.execv(sys.executable, [sys.executable, "-m", "behave"] + passthrough + locations)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
