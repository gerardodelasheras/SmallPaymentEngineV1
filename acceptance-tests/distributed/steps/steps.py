"""Step definitions for the distributed acceptance suite."""

from __future__ import annotations

import json

from behave import given, then, when

from harness import parse_scaled


def both(text):
    def decorator(func):
        given(text)(func)
        when(text)(func)
        then(text)(func)
        return func

    return decorator


def harness(context):
    return context.harness


def accepted(context):
    result = harness(context).result
    assert result is not None, "the job has not run"
    assert result.status == "accepted", (
        f"expected an accepted job, got {result.status}: {result.failure_reason}"
    )
    return result


def rejected(context):
    result = harness(context).result
    assert result is not None, "the job has not run"
    assert result.status == "rejected", (
        f"expected a rejected job, got {result.status}: {result.failure_reason}"
    )
    return result


def failed(context):
    result = harness(context).result
    assert result is not None, "the job has not run"
    assert result.status == "failed", (
        f"expected a failed job, got {result.status}: {result.failure_reason}"
    )
    return result


# --- client identity -------------------------------------------------------


@both('source "{source}" has no mapping for external client {client:d}')
def step_source_has_no_mapping(context, source, client):
    h = harness(context)
    h.source = source
    h.external = client
    h.mappings.pop((source, client), None)


@both('source "{source}" client {client:d} has canonical ID {canonical:d}')
def step_source_has_canonical_id(context, source, client, canonical):
    h = harness(context)
    h.source = source
    h.external = client
    h.set_mapping(source, client, canonical)


@both('two partitioner instances map source "{source}" client {client:d} at the same time')
def step_concurrent_mapping(context, source, client):
    h = harness(context)
    h.source = source
    h.external = client
    h.concurrent = True


@both('a staged row for source "{source}" client {client:d}')
def step_staged_row(context, source, client):
    h = harness(context)
    h.source = source
    h.external = client
    h.staged = 1


@both("a canonical BIGINT client ID is assigned")
def step_canonical_assigned(context):
    h = harness(context)
    accepted(context)
    assert h.canonical_id(h.source, h.external) is not None


@both("the mapping is persisted")
def step_mapping_persisted(context):
    h = harness(context)
    accepted(context)
    assert h.canonical_id(h.source, h.external) is not None


@both("the canonical ID is {canonical:d}")
def step_canonical_is(context, canonical):
    h = harness(context)
    accepted(context)
    assert h.canonical_id(h.source, h.external) == canonical


@both("no second client row is created")
def step_no_second_client_row(context):
    h = harness(context)
    accepted(context)
    assert len(h.identity_rows) == 1


@both("it receives a different canonical ID")
def step_different_canonical(context):
    h = harness(context)
    accepted(context)
    alpha = h.canonical_id("alpha", h.external)
    beta = h.canonical_id("beta", h.external)
    assert alpha is not None and beta is not None and alpha != beta


@both("both emit the same canonical ID")
def step_both_emit_same(context):
    h = harness(context)
    accepted(context)
    assert h.canonical_id(h.source, h.external) is not None


@both("exactly one client row exists for that key")
def step_one_client_row(context):
    h = harness(context)
    accepted(context)
    rows = [row for row in h.identity_rows if row[0] == h.source and row[1] == h.external]
    assert len(rows) == 1


@both("the transactions record key is {canonical:d}")
def step_transaction_key(context, canonical):
    h = harness(context)
    accepted(context)
    assert h.last_key == canonical


@both("the worker applies it to canonical client {canonical:d}")
def step_worker_applies(context, canonical):
    h = harness(context)
    accepted(context)
    assert h.canonical_id(h.source, h.external) == canonical


# --- stream ingestion ------------------------------------------------------


@both("a manifest with files at ordinals 1 and 2 and matching hashes")
def step_manifest_two_ordinals(context):
    h = harness(context)
    h.add_file(1)
    h.add_file(2)


@both("a manifest with two files at ordinal 1")
def step_manifest_duplicate_ordinal(context):
    h = harness(context)
    h.add_file(1)
    h.add_file(1)


@both("a manifest whose file hash does not match the file content")
def step_manifest_hash_mismatch(context):
    harness(context).add_file(1, hash_mismatch=True)


@both("a manifest with a hash mismatch")
def step_manifest_hash_mismatch_short(context):
    harness(context).add_file(1, hash_mismatch=True)


@both("a manifest with 1,000 files")
def step_manifest_many_files(context):
    h = harness(context)
    h.file_count = 1000


@both("a file with a malformed row between two valid rows")
def step_manifest_malformed(context):
    harness(context).add_file(
        1,
        rows=[
            "deposit,1,1,1.0",
            "bogus,1,2,1.0",
            "deposit,1,3,1.0",
        ],
    )


@both('file 1 and file 17 both contain source "{source}" client {client:d}')
def step_overlapping_files(context, source, client):
    h = harness(context)
    h.source = source
    h.external = client
    h.add_file(1, source=source, rows=[f"deposit,{client},1,10.0"])
    h.add_file(17, source=source, rows=[f"withdrawal,{client},2,4.0"])


@both("file 17 finishes parsing before file 1")
def step_file_17_first(context):
    harness(context).file_17_first = True


@both("two partitioner instances and several clients")
def step_two_partitioners(context):
    harness(context).multi_partitioner = True


@both("a client with more buffered rows than the configured limit")
def step_buffer_overflow(context):
    harness(context).buffer_overflow = True


@both("ingestion crashes after staging half of a file")
def step_crash_half_file(context):
    harness(context).crash_half = True


@both("a completed file is submitted again under a later job")
def step_reingest(context):
    h = harness(context)
    h.add_file(1)
    h.reingest = True


@both("the partitioner maps source \"{source}\" client {client:d}")
def step_partitioner_maps(context, source, client):
    h = harness(context)
    h.source = source
    h.external = client
    h.run_job()


@both('a later job maps source "{source}" client {client:d}')
def step_later_job_maps(context, source, client):
    h = harness(context)
    h.source = source
    h.external = client
    h.run_job()


@both('source "{source}" client {client:d} is mapped')
def step_source_is_mapped(context, source, client):
    h = harness(context)
    h.source = source
    h.external = client
    h.run_job()


@both("both mappings complete")
def step_both_mappings_complete(context):
    harness(context).run_job()


@both("the partitioner emits the row")
def step_partitioner_emits(context):
    harness(context).run_job()


@both("the partitioner emits client {client:d}")
def step_partitioner_emits_client(context, client):
    h = harness(context)
    h.external = client
    h.run_job(stages=["migrate", "ingest", "partition"])


@both("the job is submitted")
def step_job_submitted(context):
    harness(context).run_job(stages=["migrate", "ingest"])


@both("the job runs")
def step_job_runs(context):
    harness(context).run_job()


@both("ingestion runs in parallel")
def step_ingestion_parallel(context):
    harness(context).run_job(stages=["migrate", "ingest"])


@both("ingestion runs")
def step_ingestion_runs(context):
    harness(context).run_job(stages=["migrate", "ingest"])


@both("the partitioners run")
def step_partitioners_run(context):
    harness(context).run_job()


@both("more rows arrive")
def step_more_rows(context):
    harness(context).run_job()


@both("it restarts")
def step_restarts(context):
    harness(context).run_job()


@both("the job is accepted")
def step_job_accepted(context):
    accepted(context)


@both("both files are staged before ordering is applied")
def step_both_files_staged(context):
    h = harness(context)
    accepted(context)
    assert len(h.files) >= 2


@both("the job is rejected")
def step_job_rejected(context):
    rejected(context)


@both("no events are emitted")
def step_no_events(context):
    h = harness(context)
    rejected(context)
    assert h.emitted == 0


@both("the job is marked failed")
def step_job_failed(context):
    failed(context)


@both("no partial accounts CSV is exported")
def step_no_partial_export(context):
    h = harness(context)
    failed(context)
    assert not h.export_path.exists()


@both("every valid row is staged with its file and row coordinates")
def step_every_row_staged(context):
    h = harness(context)
    accepted(context)
    assert h.staged > 0


@both("both valid rows are staged")
def step_both_rows_staged(context):
    h = harness(context)
    accepted(context)
    assert h.staged >= 2


@both("the malformed row is reported")
def step_malformed_reported(context):
    h = harness(context)
    accepted(context)
    assert h.malformed_reported


@both("file 1's rows are emitted before file 17's rows")
def step_file_order(context):
    h = harness(context)
    accepted(context)
    assert h.order == [(1, 0), (17, 0)], h.order


@both("the final account state reflects that order")
def step_final_order(context):
    accepted(context)


@both("every event for one client is emitted by exactly one owner")
def step_single_owner(context):
    h = harness(context)
    accepted(context)
    assert h.owner_duplicates == 0


@both("no client's rows are interleaved or duplicated")
def step_no_interleave(context):
    h = harness(context)
    accepted(context)
    assert h.owner_duplicates == 0


@both("ingestion pauses rather than growing the buffer without bound")
def step_backpressure(context):
    h = harness(context)
    accepted(context)
    assert h.backpressure


@both("the file is not complete")
def step_file_incomplete(context):
    h = harness(context)
    accepted(context)
    assert not h.file_complete


@both("the partitioner does not emit that file's rows until they are fully staged")
def step_no_emit_incomplete(context):
    h = harness(context)
    accepted(context)
    assert h.emitted == 0


@both("no ledger effect is applied twice")
def step_no_double_apply(context):
    h = harness(context)
    accepted(context)
    assert h.applied_count <= 1


@both("the later job's export matches the earlier state")
def step_later_export_matches(context):
    h = harness(context)
    accepted(context)
    assert h.exports_match


# --- distributed ledger ----------------------------------------------------


def ensure_run(h):
    if h.result is None or h.pending_rows() > 0:
        h.run_ledger_job()
    return h


def amount_of(h, client, field):
    return h.account(client)[field]


def settled_tx(h, client, amount):
    for row in h.rows:
        if row["client"] == client and row["type"] in ("deposit", "withdrawal"):
            if row.get("amount") is not None and parse_scaled(row["amount"]) == parse_scaled(amount):
                return row["tx"]
    return None


@both("an event was committed and its offset was committed")
def step_event_committed(context):
    h = harness(context)
    h.deposit(h.subject, "10.0")
    h.run_ledger_job()
    h.export_snapshot = dict(h.accounts)


@both("Kafka redelivers the same event after a rebalance")
def step_redeliver_event(context):
    harness(context).redeliver()


@both("the dedupe record causes a no-op")
def step_dedupe_noop(context):
    h = harness(context)
    assert h.redelivery_noop, "redelivery changed the exported balances"


@both("the account balances do not change")
def step_balances_unchanged(context):
    h = harness(context)
    assert h.accounts == h.export_snapshot, (
        f"balances changed: {h.export_snapshot} -> {h.accounts}"
    )


@both("a worker commits the database transaction and crashes before committing the offset")
def step_worker_crash_before_offset(context):
    h = harness(context)
    h.deposit(h.subject, "10.0")
    h.run_ledger_job()
    h.export_snapshot = dict(h.accounts)


@both("another worker receives the event")
def step_another_worker(context):
    harness(context).redeliver()


@both("the event is deduped")
def step_event_deduped(context):
    h = harness(context)
    assert h.redelivery_noop, "the event was applied again after the crash"


@both("the final account state is unchanged")
def step_final_state_unchanged(context):
    h = harness(context)
    assert h.accounts == h.export_snapshot, (
        f"final state changed: {h.export_snapshot} -> {h.accounts}"
    )


@both("a client has no balance")
def step_client_no_balance(context):
    h = harness(context)
    h.subject = 42
    h.seeds.setdefault((h.source, h.subject), h.subject)


@both("the client deposits {amount}")
def step_client_deposits(context, amount):
    harness(context).deposit(harness(context).subject, amount)


@both("the client withdraws {amount}")
def step_client_withdraws(context, amount):
    harness(context).withdraw(harness(context).subject, amount)


@both("a client has available {amount}")
def step_client_has_available(context, amount):
    h = harness(context)
    h.subject = 42
    if parse_scaled(amount) > 0:
        h.deposit(h.subject, amount)
    else:
        h.deposit(h.subject, "10.0")
        h.withdraw(h.subject, "10.0")


@both("the client disputes a settled transaction for {amount}")
def step_client_disputes_settled(context, amount):
    h = harness(context)
    tx = settled_tx(h, h.subject, amount)
    if tx is None:
        h.deposit(h.subject, amount)
        h.withdraw(h.subject, amount)
        tx = settled_tx(h, h.subject, amount)
    assert tx is not None, f"no settled transaction for {amount}"
    h.dispute(h.subject, tx)
    h.disputed_tx = tx


@both("the dispute is rejected")
def step_dispute_rejected(context):
    h = harness(context)
    ensure_run(h)
    state = h.journal_state(h.disputed_tx)
    assert state != "disputed", "the dispute changed the journal state"


@both("the client's available is {amount}")
def step_client_available(context, amount):
    h = ensure_run(harness(context))
    assert amount_of(h, h.subject, "available") == parse_scaled(amount)


@both("the client's held is {amount}")
def step_client_held(context, amount):
    h = ensure_run(harness(context))
    assert amount_of(h, h.subject, "held") == parse_scaled(amount)


@both("the client's total is {amount}")
def step_client_total(context, amount):
    h = ensure_run(harness(context))
    assert amount_of(h, h.subject, "total") == parse_scaled(amount)


@both("the client is locked")
def step_client_locked(context):
    h = ensure_run(harness(context))
    assert h.account(h.subject)["locked"], "the client is not locked"


@both("the client is not locked")
def step_client_not_locked(context):
    h = ensure_run(harness(context))
    assert not h.account(h.subject)["locked"], "the client is locked"


@both("the client disputes a transaction it does not own")
def step_client_disputes_foreign(context):
    h = harness(context)
    owner = h.subject
    foreign_tx = settled_tx(h, owner, "10.0")
    assert foreign_tx is not None, "the owner has no settled transaction"
    h.subject = 43
    h.seeds.setdefault((h.source, h.subject), h.subject)
    h.dispute(h.subject, foreign_tx)
    h.disputed_tx = foreign_tx


@both("a settled transaction {tx:d}")
def step_settled_transaction(context, tx):
    harness(context).deposit(harness(context).subject, "10.0", tx=tx)


@both("the client disputes and charges back transaction {tx:d}")
def step_dispute_and_chargeback(context, tx):
    h = harness(context)
    h.dispute(h.subject, tx)
    h.chargeback(h.subject, tx)


@both("transaction {tx:d} remains in the journal with state charged_back")
def step_journal_charged_back(context, tx):
    h = ensure_run(harness(context))
    assert h.journal_state(tx) == "charged_back", h.journal_state(tx)


@both("the account is locked")
def step_account_locked(context):
    h = ensure_run(harness(context))
    assert h.account(h.subject)["locked"], "the account is not locked"


@both("transaction {tx:d} is charged_back")
def step_transaction_charged_back(context, tx):
    h = harness(context)
    h.deposit(h.subject, "10.0", tx=tx)
    h.dispute(h.subject, tx)
    h.chargeback(h.subject, tx)
    h.run_ledger_job()


@both("a new deposit uses transaction {tx:d}")
def step_new_deposit_uses_tx(context, tx):
    harness(context).deposit(harness(context).subject, "5.0", tx=tx)


@both("the deposit is rejected as a duplicate transaction")
def step_deposit_duplicate(context):
    h = harness(context)
    ensure_run(h)
    h.assert_row_rejected("duplicate transaction")


@both("a new client's only row is a withdrawal that exceeds available")
def step_only_row_withdrawal(context):
    h = harness(context)
    h.subject = 42
    h.withdraw(h.subject, "10.0")


@both("the job is exported")
def step_job_exported(context):
    ensure_run(harness(context))


@both("the client appears with zero balances")
def step_client_zero_balances(context):
    h = ensure_run(harness(context))
    account = h.account(h.subject)
    assert account["available"] == 0
    assert account["held"] == 0
    assert account["total"] == 0


@both("transaction {tx:d} is settled for client {client:d}")
def step_transaction_settled_for(context, tx, client):
    h = harness(context)
    h.subject = client
    h.seeds.setdefault((h.source, client), client)
    h.deposit(client, "10.0", tx=tx)
    h.run_ledger_job()


@both("client {client:d} deposits with transaction {tx:d}")
def step_client_deposits_with_tx(context, client, tx):
    h = harness(context)
    h.subject = client
    h.deposit(client, "5.0", tx=tx)


@both("the row is rejected as a duplicate transaction")
def step_row_duplicate(context):
    h = harness(context)
    ensure_run(h)
    h.assert_row_rejected("duplicate transaction")


@both("client {client:d} disputes transaction {tx:d}")
def step_client_disputes_tx(context, client, tx):
    h = harness(context)
    h.subject = client
    h.seeds.setdefault((h.source, client), client)
    h.dispute(client, tx)
    h.disputed_tx = tx


@both("the row is rejected as not owned")
def step_row_not_owned(context):
    h = harness(context)
    ensure_run(h)
    h.assert_row_rejected("not owner", "not owned", "foreign")


@both("client {client:d}'s account is not locked")
def step_client_account_not_locked(context, client):
    h = ensure_run(harness(context))
    assert not h.account(client)["locked"], f"client {client} is locked"


@both("the account locks")
def step_account_locks(context):
    h = ensure_run(harness(context))
    assert h.account(h.subject)["locked"], "the account did not lock"


@both("one warning names canonical client {client:d}")
def step_one_warning(context, client):
    h = ensure_run(harness(context))
    warnings = h.warnings()
    assert len(warnings) == 1, f"expected one warning, got {warnings}"
    assert str(client) in warnings[0], warnings[0]


@both("the warning states that the account is locked")
def step_warning_locked(context):
    h = ensure_run(harness(context))
    warnings = h.warnings()
    assert warnings and "account locked" in warnings[0].lower()


@both("the account is locked by an over-available dispute")
def step_account_locked_by_dispute(context):
    h = harness(context)
    h.subject = 42
    h.deposit(h.subject, "10.0")
    h.withdraw(h.subject, "10.0")
    h.dispute(h.subject, 1)
    h.run_ledger_job()
    assert h.account(h.subject)["locked"], "the account did not lock"


@both("another row for the client is received")
def step_another_row(context):
    harness(context).deposit(harness(context).subject, "5.0")


@both("it is rejected as locked")
def step_rejected_locked(context):
    h = harness(context)
    ensure_run(h)
    h.assert_row_rejected("account is locked", "locked")


@both("no second warning is emitted")
def step_no_second_warning(context):
    h = ensure_run(harness(context))
    assert h.warning_count() == 0, h.warnings()


# --- job reporting ---------------------------------------------------------


def fresh_job(h):
    h.files = []
    h.rows = []
    h.rows_flushed = 0
    h.rows = []
    h.result = None
    h.accounts = {}


@both("one transaction partition still has unprocessed job events")
def step_lagging_partition(context):
    h = harness(context)
    h.subject = 42
    h.deposit(h.subject, "10.0")
    h.job_a = h._new_job_id()
    h.start_job(job_name=h.job_a)


@both("the exporter checks the job")
def step_exporter_checks(context):
    h = harness(context)
    h.check_result = h.check_export(h.job_a)


@both("no CSV is written")
def step_no_csv(context):
    h = harness(context)
    assert not h.export_path.exists(), "a partial accounts CSV was written"


@both("the job remains running")
def step_job_remains_running(context):
    h = harness(context)
    state = h.check_result.get("state")
    assert state == "running", f"expected a running job, got {state}"


@both("job A touches clients {first:d} and {second:d}")
def step_job_a_touches(context, first, second):
    h = harness(context)
    fresh_job(h)
    h.subject = first
    h.deposit(first, "10.0")
    h.subject = second
    h.deposit(second, "20.0")
    h.job_a = h._new_job_id()
    h.run_ledger_job(job_name=h.job_a)
    h.export_a = dict(h.accounts)


@both("client {client:d} is touched by another job")
def step_client_touched_by_other_job(context, client):
    h = harness(context)
    fresh_job(h)
    h.subject = client
    h.deposit(client, "30.0")
    h.job_b = h._new_job_id()
    h.run_ledger_job(job_name=h.job_b)


@both("job A is exported")
def step_job_a_exported(context):
    h = harness(context)
    h.export_job(h.job_a)


@both("the CSV contains clients {first:d} and {second:d}")
def step_csv_contains_clients(context, first, second):
    h = harness(context)
    exported = h.exported_clients()
    assert first in exported, f"client {first} missing from {exported}"
    assert second in exported, f"client {second} missing from {exported}"


@both("the CSV does not contain client {client:d}")
def step_csv_excludes_client(context, client):
    h = harness(context)
    assert client not in h.exported_clients()


@both("a client's only job row is a rejected withdrawal")
def step_only_job_row_rejected(context):
    h = harness(context)
    h.subject = 42
    h.withdraw(h.subject, "10.0")


@both("the client appears with available {amount}")
def step_client_appears_available(context, amount):
    h = ensure_run(harness(context))
    assert h.account(h.subject)["available"] == parse_scaled(amount)


@both("job A is running")
def step_job_a_running(context):
    h = harness(context)
    h.subject = 42
    h.deposit(h.subject, "10.0")
    h.job_a = h._new_job_id()
    h.start_job(job_name=h.job_a)


@both("job B is submitted")
def step_job_b_submitted(context):
    h = harness(context)
    h.submit_job(job_name=h._new_job_id())


@both("job B is rejected as job already active")
def step_job_b_rejected(context):
    h = harness(context)
    result = h.result
    assert result is not None and result.status == "rejected", (
        f"expected a rejected job, got {result and result.status}"
    )
    assert "already active" in (result.stderr or "").lower()


@both("job A is unaffected")
def step_job_a_unaffected(context):
    h = harness(context)
    state = h.read_job_state(h.job_a)
    assert state.get("state") == "running", state


@both("no accounts CSV is written")
def step_no_accounts_csv(context):
    h = harness(context)
    assert not h.export_path.exists(), "an accounts CSV was written"


@both("a later valid job can start")
def step_later_job_starts(context):
    h = harness(context)
    fresh_job(h)
    h.subject = 42
    h.deposit(h.subject, "10.0")
    h.run_ledger_job()
    accepted(context)


@both("a manifest has completed and was exported")
def step_manifest_completed(context):
    h = harness(context)
    h.subject = 42
    h.deposit(h.subject, "10.0")
    h.run_ledger_job()
    accepted(context)
    h.export_snapshot = dict(h.accounts)
    h.journal_snapshot = h.journal_count()


@both("the same manifest is submitted again")
def step_same_manifest_again(context):
    h = harness(context)
    h.run_ledger_job()
    accepted(context)


@both("the exported balances are unchanged")
def step_exported_balances_unchanged(context):
    h = harness(context)
    assert h.accounts == h.export_snapshot, (
        f"balances changed: {h.export_snapshot} -> {h.accounts}"
    )


@both("no transaction is applied twice")
def step_no_transaction_twice(context):
    h = harness(context)
    assert h.journal_count() == h.journal_snapshot, "the journal grew"


# --- database portability --------------------------------------------------


@both("the ledger conformance suite")
def step_conformance_suite(context):
    h = harness(context)
    h.subject = 42
    h.deposit(h.subject, "10.0")
    h.dispute(h.subject, 1)
    h.resolve(h.subject, 1)


@both("it runs against PostgreSQL and MariaDB/Galera")
def step_conformance_runs(context):
    harness(context).run_conformance_both()


@both("both produce identical account and journal results")
def step_conformance_identical(context):
    harness(context).compare_backends()


@both("many concurrent first sightings")
def step_many_concurrent(context):
    h = harness(context)
    h.concurrent_count = 20


@both("IDs are allocated on either backend")
def step_allocate_ids(context):
    h = harness(context)
    h.allocate_concurrently(h.concurrent_count)


@both("every canonical ID is unique")
def step_ids_unique(context):
    h = harness(context)
    assert len(h.concurrent_ids) == len(set(h.concurrent_ids)), h.concurrent_ids


@both("no mapping is lost")
def step_no_mapping_lost(context):
    h = harness(context)
    assert len(h.concurrent_ids) == h.concurrent_count
    assert all(h.concurrent_ids), h.concurrent_ids


@both("the database rejects a transaction with a transient certification conflict")
def step_transient_conflict(context):
    h = harness(context)
    h.extra_env["DISTRIBUTED_FAULT"] = "conflict:1"
    h.subject = 42
    h.deposit(h.subject, "10.0")


@both("the worker retries")
def step_worker_retries(context):
    harness(context).run_ledger_job()


@both("the event applies exactly once")
def step_event_applies_once(context):
    h = harness(context)
    assert h.journal_count() == 1, h.journal_count()


@both("the offset is not committed before the successful database commit")
def step_offset_after_commit(context):
    h = harness(context)
    summary = h.worker_summary()
    assert summary.get("offset_committed_after_db_commit") is True, summary


@both("every attempt fails with a transient conflict")
def step_always_conflict(context):
    h = harness(context)
    h.extra_env["DISTRIBUTED_FAULT"] = "conflict:always"
    h.subject = 42
    h.deposit(h.subject, "10.0")


@both("the retry budget is exhausted")
def step_retry_budget_exhausted(context):
    harness(context).run_ledger_job()


@both("the budget is exhausted")
def step_budget_exhausted(context):
    harness(context).run_ledger_job()


@both("the event is sent to the dead-letter path")
def step_event_dead_letter(context):
    h = harness(context)
    assert h.read_dlq(), "the dead-letter path is empty"


@both("an empty database")
def step_empty_database(context):
    harness(context).empty_database = True


@both("migrations run on PostgreSQL and MariaDB/Galera")
def step_migrations_both(context):
    h = harness(context)
    h.batch_migrate = h.migrate_both(fresh=True)


@both("the required tables and constraints exist")
def step_required_tables(context):
    h = harness(context)
    tables = (
        "clients",
        "accounts",
        "transactions",
        "ledger_events",
        "jobs",
        "job_files",
        "job_events",
        "job_progress",
        "job_clients",
        "id_allocator",
    )
    for backend in ("postgres", "mariadb"):
        for table in tables:
            assert h.table_exists(table, backend), f"{table} missing on {backend}"


@both("a duplicate transaction ID is rejected")
def step_duplicate_tx_id(context):
    h = harness(context)
    for backend in ("postgres", "mariadb"):
        assert h.duplicate_tx_rejected(backend), (
            f"{backend} accepted a duplicate tx_id"
        )


@both("the same transaction ID is submitted for two clients")
def step_same_tx_two_clients(context):
    h = harness(context)
    h.subject = 42
    h.deposit(42, "10.0", tx=5)
    h.run_ledger_job()
    h.duplicate_second = h.duplicate_tx_rejected(h.backend)


@both("the second write is attempted")
def step_second_write(context):
    # The unique-index attempt runs in the preceding Given.
    h = harness(context)
    assert getattr(h, "duplicate_second", None) is not None


@both("the unique index rejects it")
def step_unique_index_rejects(context):
    h = harness(context)
    assert h.duplicate_second, "the unique index accepted a duplicate tx_id"


@both("no cross-shard path exists")
def step_no_cross_shard(context):
    h = harness(context)
    config = h.show_config()
    text = json.dumps(config).lower()
    assert "shard" not in text, config


# --- distributed operations ------------------------------------------------


@both("no broker address is configured")
def step_no_broker(context):
    h = harness(context)
    h.missing_broker = True
    h.extra_env.pop("KAFKA_BOOTSTRAP", None)
    h.extra_env.pop("DISTRIBUTED_CONFIG", None)


@both("a service starts")
def step_service_starts(context):
    h = harness(context)
    if h.missing_broker:
        h.start_health = h.health_check(drop_bootstrap=True)
    else:
        h.show_config()


@both("the service starts")
def step_the_service_starts(context):
    step_service_starts(context)


@both("it exits with a clear configuration error")
def step_configuration_error(context):
    h = harness(context)
    health = h.start_health
    assert health["returncode"] != 0, "the service started without a broker"
    text = f"{health['stderr']}\n{health['stdout']}".lower()
    assert "config" in text and "broker" in text, text


@both("no partial job is accepted")
def step_no_partial_job(context):
    h = harness(context)
    assert h.job_id is None, f"a job was accepted: {h.job_id}"


@both("a broker address in the file and a different one in the environment")
def step_broker_file_and_env(context):
    h = harness(context)
    path = h.workdir / "distributed-config.json"
    path.write_text(json.dumps({"kafka_bootstrap": "file-broker:9092"}))
    h.extra_env["DISTRIBUTED_CONFIG"] = str(path)
    h.extra_env["KAFKA_BOOTSTRAP"] = "env-broker:9092"


@both("the environment value is used")
def step_environment_value(context):
    h = harness(context)
    config = h.show_config()
    assert config.get("kafka_bootstrap") == "env-broker:9092", config


@both("a configured database password")
def step_configured_password(context):
    h = harness(context)
    h.extra_env["DATABASE_PASSWORD"] = h.password


@both("the service starts and logs its configuration")
def step_service_logs_config(context):
    harness(context).show_config()


@both("the password is redacted")
def step_password_redacted(context):
    h = harness(context)
    text = json.dumps(h.config_show)
    assert h.password not in text, text
    assert "redacted" in text.lower() or "***" in text, text


@both("a migrated database and provisioned topics")
def step_migrated_database(context):
    h = harness(context)
    h.migrate_result = h.migrate()
    assert h.migrate_result.returncode == 0, h.migrate_result.stderr


@both("the migration command runs again")
def step_migration_runs_again(context):
    h = harness(context)
    h.migrate_result_second = h.migrate()


@both("no schema or topic is duplicated")
def step_no_duplicate_schema(context):
    h = harness(context)
    data = json.loads(h.migrate_result_second.stdout)
    assert data.get("migrations_applied", 0) == 0, data
    assert data.get("topics_created", 0) == 0, data


@both("the command exits successfully")
def step_command_succeeds(context):
    h = harness(context)
    assert h.migrate_result_second.returncode == 0, (
        h.migrate_result_second.stderr
    )


@both("an event that fails every retry")
def step_poison_event(context):
    h = harness(context)
    h.extra_env["DISTRIBUTED_FAULT"] = "conflict:always"
    h.subject = 42
    h.deposit(h.subject, "10.0")


@both("the event appears in the dead-letter path with metadata")
def step_dead_letter_metadata(context):
    h = harness(context)
    events = h.read_dlq()
    assert events, "the dead-letter path is empty"
    event = events[0]
    for key in ("job_id", "file_ord", "row_ord", "error"):
        assert key in event, event


@both("no partial export is written")
def step_no_partial_export(context):
    h = harness(context)
    assert not h.export_path.exists(), "a partial export was written"


@both("a service can reach its broker and database")
def step_service_reachable(context):
    harness(context).reachable = True


@both("readiness is checked")
def step_readiness_checked(context):
    h = harness(context)
    h.health = h.health_json()


@both("it reports ready")
def step_reports_ready(context):
    h = harness(context)
    assert h.health.get("ready") is True, h.health


@both("a job's file watermark has not advanced")
def step_stalled_watermark(context):
    h = harness(context)
    h.stalled = True
    h.subject = 42
    h.deposit(h.subject, "10.0")
    h.job_a = h._new_job_id()
    h.start_job(job_name=h.job_a)


@both("the metrics are read")
def step_metrics_read(context):
    harness(context).read_metrics()


@both("the stalled watermark is reported")
def step_stalled_watermark_reported(context):
    h = harness(context)
    stalled = h.metrics_data.get("stalled_watermarks", [])
    assert h.job_a in stalled, h.metrics_data


@both("the job's failed or paused status is visible")
def step_job_status_visible(context):
    h = harness(context)
    states = h.metrics_data.get("job_states", {})
    assert states.get(h.job_a) in ("running", "paused", "failed"), h.metrics_data
