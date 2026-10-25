//! Cross-variant conformance between the in-memory CLI ledger and the durable
//! distributed adapter.

use std::io::Cursor;

use distributed::DurableLedger;
use transaction_processor::transaction::parser::transactions;
use transaction_processor::transaction::transaction_processor::TransactionProcessor;
use transaction_processor::{
    CanonicalClientId, ClientId, EntryState, LedgerTransaction, ProcessingError, Transaction,
    TransactionId,
};

fn to_canonical(transaction: &Transaction) -> LedgerTransaction<CanonicalClientId> {
    LedgerTransaction {
        transaction_type: transaction.transaction_type,
        client: i64::from(transaction.client),
        transaction_id: transaction.transaction_id,
        amount: transaction.amount,
    }
}

fn error_text(result: &Result<(), ProcessingError>) -> Option<String> {
    result.as_ref().err().map(ToString::to_string)
}

fn build_csv() -> String {
    let mut rows: Vec<String> = [
        // Over-available dispute: both lock, later rows are locked.
        "deposit,1,1,100.0",
        "withdrawal,1,2,80.0",
        "dispute,1,1,",
        "deposit,1,3,10.0",
        "chargeback,1,1,",
        // Dispute/resolve/dispute/chargeback: the durable journal retains.
        "deposit,2,10,10.0",
        "dispute,2,10,",
        "resolve,2,10,",
        "dispute,2,10,",
        "chargeback,2,10,",
        "deposit,2,11,5.0",
        // Dispute and chargeback on a settled withdrawal.
        "deposit,3,20,50.0",
        "withdrawal,3,21,20.0",
        "dispute,3,21,",
        "chargeback,3,21,",
        // Foreign, unknown, and duplicate references.
        "deposit,4,30,10.0",
        "dispute,5,30,",
        "dispute,4,99,",
        "deposit,6,40,10.0",
        "deposit,6,40,5.0",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();

    // Seeded monetary rows keep the comparison broad without a random crate.
    let mut seed = 0x5eed_1234_9abc_def0_u64;
    let mut next_tx = [0_u32; 8];
    for _ in 0..200 {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let client = 10 + (seed % 8) as u16;
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let amount = 1 + (seed % 50);
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let kind = if seed % 3 == 0 {
            "withdrawal"
        } else {
            "deposit"
        };
        let index = usize::from(client - 10);
        next_tx[index] += 1;
        let transaction_id = 1_000 + index as u32 * 1_000 + next_tx[index];
        rows.push(format!("{kind},{client},{transaction_id},{amount}.0"));
    }

    let mut csv = String::from("type,client,tx,amount\n");
    for row in rows {
        csv.push_str(&row);
        csv.push('\n');
    }
    csv
}

#[test]
fn generated_csvs_match_balances_journal_and_lock_state() {
    let csv = build_csv();
    let parsed: Vec<Transaction> = transactions(Cursor::new(csv))
        .expect("valid header")
        .map(|row| row.expect("valid row"))
        .collect();

    let mut cli = TransactionProcessor::new();
    let mut distributed = DurableLedger::new();

    for transaction in &parsed {
        let cli_result = cli.process(transaction);
        let distributed_result = distributed.process(&to_canonical(transaction));
        assert_eq!(
            error_text(&cli_result),
            error_text(&distributed_result),
            "row {transaction:?}"
        );
    }

    assert_eq!(cli.accounts().len(), distributed.accounts().len());
    for (client, account) in distributed.accounts() {
        let cli_account = cli
            .accounts()
            .get(&(*client as ClientId))
            .unwrap_or_else(|| panic!("missing in-memory account for client {client}"));
        assert_eq!(cli_account, account, "client {client}");
    }

    let mut transaction_ids: Vec<TransactionId> = cli.entries().keys().copied().collect();
    transaction_ids.extend(distributed.entries().keys().copied());
    transaction_ids.sort_unstable();
    transaction_ids.dedup();

    for transaction_id in transaction_ids {
        let cli_state = cli.entries().get(&transaction_id).map(|entry| entry.state);
        let distributed_state = distributed
            .entries()
            .get(&transaction_id)
            .map(|entry| entry.state);

        match (cli_state, distributed_state) {
            (Some(cli_state), Some(distributed_state)) => {
                assert_eq!(cli_state, distributed_state, "tx {transaction_id}");
            }
            // The durable journal intentionally keeps the terminal row.
            (None, Some(EntryState::ChargedBack)) => {}
            (None, None) => {}
            other => panic!("unexpected journal divergence for tx {transaction_id}: {other:?}"),
        }
    }

    assert!(distributed
        .entries()
        .values()
        .any(|entry| entry.state == EntryState::ChargedBack));
    assert!(!cli
        .entries()
        .values()
        .any(|entry| entry.state == EntryState::ChargedBack));
}
