"""Step definitions for the distributed acceptance suite."""

from __future__ import annotations

from behave import given, then, when


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
    h.run_job()


@both("the job is submitted")
def step_job_submitted(context):
    harness(context).run_job()


@both("the job runs")
def step_job_runs(context):
    harness(context).run_job()


@both("ingestion runs in parallel")
def step_ingestion_parallel(context):
    harness(context).run_job()


@both("ingestion runs")
def step_ingestion_runs(context):
    harness(context).run_job()


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
    assert h.order == [(1, 1), (17, 1)]


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
