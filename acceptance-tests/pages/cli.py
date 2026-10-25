"""Page object for the transaction_processor CLI.

All knowledge of CSV layout, process invocation, and output parsing lives here.
Step definitions only speak in domain terms and call these methods.
"""

import os
import subprocess
import tempfile
from pathlib import Path

_UNSET = object()


class Result:
    def __init__(self, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class CliHarness:
    INPUT_HEADER = "type,client,tx,amount"
    OUTPUT_HEADER = "client,available,held,total,locked"

    def __init__(self, binary):
        self.binary = Path(binary)
        self.reset()

    def reset(self):
        self.header = self.INPUT_HEADER
        self.rows = []
        self.invocation = None
        self.next_client = 1
        self.next_tx = 1
        self.last_client = None
        self.last_deposit_tx = None
        self.last_dispute_tx = None
        self.path = None
        self.result = None

    # --- input construction ------------------------------------------------

    def set_header(self, header):
        self.header = header

    def add_row(self, row):
        self._track_client(row)
        self.rows.append(row.encode())

    def add_bytes(self, data):
        self.rows.append(data)

    def _track_client(self, row):
        fields = row.split(",")
        if len(fields) >= 2:
            value = fields[1].strip()
            self.last_client = int(value) if value.isdigit() else None

    def new_client(self):
        client = self.next_client
        self.next_client += 1
        self.last_client = client
        return client

    def new_tx(self):
        tx = self.next_tx
        self.next_tx += 1
        return tx

    def _explicit_tx(self, tx):
        tx = int(tx)
        self.next_tx = max(self.next_tx, tx + 1)
        return tx

    def deposit(self, client, amount, tx=None):
        tx = self.new_tx() if tx is None else self._explicit_tx(tx)
        self.add_row(f"deposit,{client},{tx},{amount}")
        self.last_client = int(client)
        self.last_deposit_tx = tx
        return tx

    def withdraw(self, client, amount, tx=None):
        tx = self.new_tx() if tx is None else self._explicit_tx(tx)
        self.add_row(f"withdrawal,{client},{tx},{amount}")
        self.last_client = int(client)
        return tx

    def dispute(self, client, tx):
        self.add_row(f"dispute,{client},{tx}")
        self.last_client = int(client)
        self.last_dispute_tx = int(tx)

    def resolve(self, client, tx):
        self.add_row(f"resolve,{client},{tx}")
        self.last_client = int(client)

    def chargeback(self, client, tx):
        self.add_row(f"chargeback,{client},{tx}")
        self.last_client = int(client)

    # --- invocation --------------------------------------------------------

    def data(self):
        lines = [self.header.encode()] + self.rows
        return b"\n".join(lines) + b"\n"

    def write_input(self):
        fd, name = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "wb") as handle:
            handle.write(self.data())
        self.path = Path(name)
        return self.path

    def run(self, rust_log=_UNSET):
        if self.invocation is None:
            self.write_input()
            argv = [str(self.path)]
        else:
            argv = [str(item) for item in self.invocation]

        env = os.environ.copy()
        env.pop("RUST_LOG", None)
        if rust_log is not _UNSET and rust_log is not None:
            env["RUST_LOG"] = rust_log

        completed = subprocess.run(
            [str(self.binary), *argv],
            capture_output=True,
            env=env,
        )
        self.result = Result(completed.returncode, completed.stdout, completed.stderr)
        return self.result

    def ensure_run(self):
        if self.result is None:
            self.run()
        return self.result

    # --- output inspection -------------------------------------------------

    def stdout_lines(self):
        return self.ensure_run().stdout.decode(errors="replace").splitlines()

    def output_header(self):
        lines = self.stdout_lines()
        return lines[0] if lines else ""

    def account_rows(self):
        return [line for line in self.stdout_lines()[1:] if line.strip()]

    def account_for(self, client):
        for line in self.account_rows():
            if line.split(",")[0] == str(client):
                return line
        return None

    def account_fields(self, client):
        line = self.account_for(client)
        return line.split(",") if line else None

    def stderr_text(self):
        return self.ensure_run().stderr.decode(errors="replace")

    def returncode(self):
        return self.ensure_run().returncode
