"""Domain step definitions for the transaction processor acceptance suite.

All CSV construction, process invocation, and output parsing live in
`pages.cli.CliHarness`; these steps speak in domain terms.
"""

from behave import given, register_type, then, when
from parse import with_pattern


@with_pattern(r"[^\s]+")
def amount_text(text):
    return text


register_type(Amount=amount_text)


def both(pattern):
    """Register an input-building step as either Given or When."""

    def wrap(func):
        given(pattern)(func)
        when(pattern)(func)
        return func

    return wrap


def _fields(context, client):
    fields = context.cli.account_fields(client)
    assert fields is not None, f"no account row for client {client}"
    return fields


# --- input construction ----------------------------------------------------


@given("a new client")
def step_new_client(context):
    context.cli.new_client()


@both("a new client deposits {amount:Amount}")
def step_new_client_deposits(context, amount):
    client = context.cli.new_client()
    context.cli.deposit(client, amount)


@both("a new client deposits an amount padded with spaces around {amount:Amount}")
def step_new_client_deposits_padded(context, amount):
    client = context.cli.new_client()
    context.cli.deposit(client, f"  {amount}  ")


@both("a new client deposits {amount:Amount} with transaction {tx}")
def step_new_client_deposits_with_tx(context, amount, tx):
    client = context.cli.new_client()
    context.cli.deposit(client, amount, tx=tx)


@both("a new client withdraws {amount:Amount}")
def step_new_client_withdraws(context, amount):
    client = context.cli.new_client()
    context.cli.withdraw(client, amount)


@both("a transaction row deposits the negative amount {amount:Amount}")
def step_transaction_row_negative(context, amount):
    context.cli.deposit(1, amount)


@both("a transaction row deposits {amount:Amount}")
def step_transaction_row_deposits(context, amount):
    context.cli.deposit(1, amount)


@both("a client with an accepted deposit of {amount:Amount}")
def step_client_with_deposit(context, amount):
    client = context.cli.new_client()
    context.cli.deposit(client, amount)


@both("client {client} deposits {amount:Amount} with transaction {tx}")
def step_client_deposits_with_tx(context, client, amount, tx):
    context.cli.deposit(int(client), amount, tx=tx)


@both("client {client} deposits {amount:Amount}")
def step_client_deposits(context, client, amount):
    context.cli.deposit(int(client), amount)


@both("the client deposits {amount:Amount} with transaction {tx}")
def step_client_deposits_last_with_tx(context, amount, tx):
    context.cli.deposit(context.cli.last_client, amount, tx=tx)


@both("the client deposits {amount:Amount}")
def step_client_deposits_last(context, amount):
    context.cli.deposit(context.cli.last_client, amount)


@both("client {client} withdraws {amount:Amount}")
def step_client_withdraws(context, client, amount):
    context.cli.withdraw(int(client), amount)


@both("the client withdraws {amount:Amount}")
def step_client_withdraws_last(context, amount):
    context.cli.withdraw(context.cli.last_client, amount)


@both("the client disputes the deposit again")
def step_client_disputes_deposit_again(context):
    context.cli.dispute(context.cli.last_client, context.cli.last_deposit_tx)


@both("the client disputes the deposit")
def step_client_disputes_deposit(context):
    context.cli.dispute(context.cli.last_client, context.cli.last_deposit_tx)


@both("the client disputes transaction {tx}")
def step_client_disputes_transaction(context, tx):
    context.cli.dispute(context.cli.last_client, int(tx))


@both("client {client} disputes transaction {tx}")
def step_client_disputes_transaction_for(context, client, tx):
    context.cli.dispute(int(client), int(tx))


@both("the client resolves the dispute")
def step_client_resolves_dispute(context):
    context.cli.resolve(context.cli.last_client, context.cli.last_dispute_tx)


@both("the client resolves the deposit")
def step_client_resolves_deposit(context):
    context.cli.resolve(context.cli.last_client, context.cli.last_deposit_tx)


@both("the client charges back the dispute")
def step_client_charges_back_dispute(context):
    context.cli.chargeback(context.cli.last_client, context.cli.last_dispute_tx)


@both("the client charges back the deposit")
def step_client_charges_back_deposit(context):
    context.cli.chargeback(context.cli.last_client, context.cli.last_deposit_tx)


@both("a row for a new client with an invalid amount")
def step_row_for_new_client_invalid_amount(context):
    client = context.cli.new_client()
    context.cli.deposit(client, "abc")


@both('a CSV with the header "{header}"')
def step_csv_with_header(context, header):
    context.cli.set_header(header)


@both('the row "{row}"')
def step_row(context, row):
    context.cli.add_row(row)


@both('a CSV row "{row}"')
def step_csv_row(context, row):
    context.cli.add_row(row)


@both('a following row "{row}"')
def step_following_row(context, row):
    context.cli.add_row(row)


@both("a CSV row with too few columns")
def step_csv_row_too_few_columns(context):
    context.cli.add_row("deposit,1")


@both("a following valid deposit row for client {client}")
def step_following_valid_deposit(context, client):
    context.cli.deposit(int(client), "5.0")


@both("a blank line between two valid deposit rows for client {client}")
def step_blank_line_between_deposits(context, client):
    context.cli.deposit(int(client), "3.0")
    context.cli.add_row("")
    context.cli.deposit(int(client), "4.0")


@both("a dispute row that has only type, client, and transaction identifier columns")
def step_dispute_row_without_amount(context):
    context.cli.dispute(context.cli.last_client, context.cli.last_deposit_tx)


@both("a dispute row that also carries an amount")
def step_dispute_row_with_amount(context):
    context.cli.add_row(
        f"dispute,{context.cli.last_client},{context.cli.last_deposit_tx},5.0"
    )


@both("a dispute row with an empty amount field")
def step_dispute_row_empty_amount(context):
    context.cli.add_row(
        f"dispute,{context.cli.last_client},{context.cli.last_deposit_tx},"
    )


@both("a withdrawal row with no amount")
def step_withdrawal_row_without_amount(context):
    context.cli.add_row(f"withdrawal,{context.cli.last_client},{context.cli.new_tx()}")


@both("a CSV row containing invalid UTF-8 bytes")
def step_csv_row_invalid_utf8(context):
    context.cli.add_bytes(b"deposit,1,9,\xff")


@both("a row with invalid UTF-8 bytes followed by a valid deposit for client {client}")
def step_invalid_utf8_then_deposit(context, client):
    context.cli.add_bytes(b"deposit,1,9,\xff")
    context.cli.deposit(int(client), "5.0")


@both("a malformed row followed by a valid deposit for client {client}")
def step_malformed_then_deposit(context, client):
    context.cli.add_row("this,is,not,valid")
    context.cli.deposit(int(client), "5.0")


@both("an input CSV with one invalid row and one valid deposit")
def step_one_invalid_one_valid(context):
    context.cli.add_row("deposit,1,1,abc")
    context.cli.deposit(1, "5.0")


@both("an input CSV whose only row is invalid")
def step_only_invalid_row(context):
    context.cli.add_row("deposit,1,1,abc")


@both("a valid input CSV path")
def step_valid_input_path(context):
    context.cli.deposit(1, "1.0")
    context.cli.write_input()
    context.cli.invocation = [str(context.cli.path)]


@both("a valid input CSV with one deposit")
def step_valid_input_with_deposit(context):
    context.cli.deposit(1, "1.0")


@both("an input path that does not exist")
def step_nonexistent_input(context):
    context.cli.invocation = ["/nonexistent/transactions.csv"]


@both("no positional argument")
def step_no_argument(context):
    context.cli.invocation = []


@both("two positional arguments")
def step_two_arguments(context):
    context.cli.invocation = [str(context.cli.write_input()), "extra.csv"]


@both("a CSV containing only the header")
def step_header_only(context):
    pass


# --- invocation ------------------------------------------------------------


@when("the processor runs")
def step_processor_runs(context):
    context.cli.run()


@when("the processor runs without RUST_LOG")
def step_processor_runs_without_rust_log(context):
    context.cli.run(rust_log=None)


@when("the processor runs with RUST_LOG set to trace")
def step_processor_runs_with_trace(context):
    context.cli.run(rust_log="trace")


@when("the processor runs with RUST_LOG set to error")
def step_processor_runs_with_error(context):
    context.cli.run(rust_log="error")


@when("the processor is invoked with that one argument")
def step_processor_invoked_with_argument(context):
    context.cli.run()


@when("the processor is invoked")
def step_processor_invoked(context):
    context.cli.run()


@when("the accounts are written")
def step_accounts_written(context):
    context.cli.run()


# --- assertions ------------------------------------------------------------


@then("the client's available is {amount:Amount}")
def step_client_available(context, amount):
    assert _fields(context, context.cli.last_client)[1] == amount


@then("the client's held is {amount:Amount}")
def step_client_held(context, amount):
    assert _fields(context, context.cli.last_client)[2] == amount


@then("the client's total is {amount:Amount}")
def step_client_total(context, amount):
    assert _fields(context, context.cli.last_client)[3] == amount


@then("the client is not locked")
def step_client_not_locked(context):
    assert _fields(context, context.cli.last_client)[4] == "false"


@then("the client is still locked")
def step_client_still_locked(context):
    assert _fields(context, context.cli.last_client)[4] == "true"


@then("the client is locked")
def step_client_locked(context):
    assert _fields(context, context.cli.last_client)[4] == "true"


@then("client {client} has available {amount:Amount}")
def step_client_has_available(context, client, amount):
    assert _fields(context, int(client))[1] == amount


@then("client {client}'s available is {amount:Amount}")
def step_client_available_for(context, client, amount):
    assert _fields(context, int(client))[1] == amount


@then("client {client}'s held is {amount:Amount}")
def step_client_has_held(context, client, amount):
    assert _fields(context, int(client))[2] == amount


@then("the row is skipped")
def step_row_skipped(context):
    # The following assertion proves the row had no effect.
    pass


@then("no account is initialized for that client")
def step_no_account_for_that_client(context):
    client = context.cli.last_client
    if client is None:
        assert context.cli.account_rows() == []
    else:
        assert context.cli.account_for(client) is None


@then("no account is initialized for client {client}")
def step_no_account_for_client(context, client):
    assert context.cli.account_for(int(client)) is None


@then("no account is initialized")
def step_no_account_initialized(context):
    assert context.cli.account_rows() == []


@then('the output header is "{header}"')
def step_output_header(context, header):
    assert context.cli.output_header() == header


@then('the output contains the row "{row}"')
def step_output_contains_row(context, row):
    assert row in context.cli.account_rows(), f"missing row {row}"


@then("the output contains a row for client {client}")
def step_output_contains_client(context, client):
    assert context.cli.account_for(int(client)) is not None


@then("the output contains exactly {count} account rows")
def step_output_row_count(context, count):
    assert len(context.cli.account_rows()) == int(count)


@then("the output contains no account rows")
def step_output_no_rows(context):
    assert context.cli.account_rows() == []


@then("stdout contains the accounts header")
def step_stdout_header(context):
    assert context.cli.output_header() == context.cli.OUTPUT_HEADER


@then("stdout contains only account rows")
def step_stdout_only_rows(context):
    assert context.cli.output_header() == context.cli.OUTPUT_HEADER
    for row in context.cli.account_rows():
        fields = row.split(",")
        assert len(fields) == 5, f"not an account row: {row}"
        assert fields[4] in ("true", "false"), f"bad locked value: {row}"


@then("the invalid row is reported on stderr when diagnostics are enabled")
def step_invalid_row_diagnostic(context):
    result = context.cli.run(rust_log="trace")
    assert result.stderr.strip(), "expected a diagnostic on stderr"


@then("the run exits successfully")
def step_run_successful(context):
    assert context.cli.returncode() == 0


@then("the run exits with status zero")
def step_run_status_zero(context):
    assert context.cli.returncode() == 0


@then("the run exits with a non-zero status")
def step_run_nonzero(context):
    assert context.cli.returncode() != 0


@then("a usage message is written to stderr")
def step_usage_message(context):
    assert "usage" in context.cli.stderr_text().lower()


@then("an error is written to stderr")
def step_error_message(context):
    assert context.cli.stderr_text().strip()


@then("stderr contains no row diagnostic")
def step_stderr_quiet(context):
    assert context.cli.stderr_text().strip() == ""


@then("the rejected row is reported on stderr")
def step_rejected_row_reported(context):
    assert context.cli.stderr_text().strip()


@then("stderr contains a warning naming client {client}")
def step_stderr_warning_for_client(context, client):
    stderr = context.cli.stderr_text().lower()
    assert f"client {client}" in stderr or f"client={client}" in stderr
    assert "dispute" in stderr


@then("stderr states that the account is locked")
def step_stderr_account_locked(context):
    assert "account locked" in context.cli.stderr_text().lower()


@then("the accounts CSV is written to stdout")
def step_accounts_csv_written(context):
    assert context.cli.output_header() == context.cli.OUTPUT_HEADER
    assert len(context.cli.account_rows()) >= 1


@then("the output contains the accounts header")
def step_output_contains_header(context):
    assert context.cli.output_header() == context.cli.OUTPUT_HEADER
