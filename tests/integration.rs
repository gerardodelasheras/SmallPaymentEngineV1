//! Independent end-to-end verification.
//!
//! The oracle below re-implements the ledger rules in plain `i64` arithmetic. It
//! shares no code with the processor, so a disagreement is a real finding.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use transaction_processor::pipeline;
use transaction_processor::processing_error::ProcessingError;

const SCALE: i64 = 10_000;

static COUNTER: AtomicU64 = AtomicU64::new(0);

// --- oracle ---------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Row {
    Deposit { client: u16, tx: u32, amount: i64 },
    Withdrawal { client: u16, tx: u32, amount: i64 },
    Dispute { client: u16, tx: u32 },
    Resolve { client: u16, tx: u32 },
    Chargeback { client: u16, tx: u32 },
}

impl Row {
    fn client(&self) -> u16 {
        match *self {
            Row::Deposit { client, .. }
            | Row::Withdrawal { client, .. }
            | Row::Dispute { client, .. }
            | Row::Resolve { client, .. }
            | Row::Chargeback { client, .. } => client,
        }
    }

    fn csv_line(&self) -> String {
        match *self {
            Row::Deposit { client, tx, amount } => {
                format!("deposit,{client},{tx},{}", render(amount))
            }
            Row::Withdrawal { client, tx, amount } => {
                format!("withdrawal,{client},{tx},{}", render(amount))
            }
            Row::Dispute { client, tx } => format!("dispute,{client},{tx}"),
            Row::Resolve { client, tx } => format!("resolve,{client},{tx}"),
            Row::Chargeback { client, tx } => format!("chargeback,{client},{tx}"),
        }
    }
}

#[derive(Clone, Copy)]
struct Entry {
    client: u16,
    amount: i64,
    disputed: bool,
}

#[derive(Default)]
struct Ledger {
    available: i64,
    held: i64,
    locked: bool,
}

#[derive(Default)]
struct Oracle {
    accounts: HashMap<u16, Ledger>,
    entries: HashMap<u32, Entry>,
}

impl Oracle {
    fn apply(&mut self, row: &Row) {
        let client = row.client();
        let locked = self.accounts.entry(client).or_default().locked;
        if locked {
            return;
        }

        match *row {
            Row::Deposit { client, tx, amount } => {
                if self.entries.contains_key(&tx) {
                    return;
                }
                let account = self.accounts.get_mut(&client).expect("initialized");
                let Some(available) = account.available.checked_add(amount) else {
                    return;
                };
                account.available = available;
                self.entries.insert(
                    tx,
                    Entry {
                        client,
                        amount,
                        disputed: false,
                    },
                );
            }
            Row::Withdrawal { client, tx, amount } => {
                if self.entries.contains_key(&tx) {
                    return;
                }
                let account = self.accounts.get_mut(&client).expect("initialized");
                if account.available < amount {
                    return;
                }
                account.available -= amount;
                self.entries.insert(
                    tx,
                    Entry {
                        client,
                        amount,
                        disputed: false,
                    },
                );
            }
            Row::Dispute { client, tx } => {
                let Some(entry) = self.entries.get(&tx).copied() else {
                    return;
                };
                if entry.client != client || entry.disputed {
                    return;
                }
                let account = self.accounts.get_mut(&client).expect("initialized");
                if account.available < entry.amount {
                    account.locked = true;
                    return;
                }
                account.available -= entry.amount;
                account.held += entry.amount;
                self.entries.get_mut(&tx).expect("entry exists").disputed = true;
            }
            Row::Resolve { client, tx } => {
                let Some(entry) = self.entries.get(&tx).copied() else {
                    return;
                };
                if entry.client != client || !entry.disputed {
                    return;
                }
                let account = self.accounts.get_mut(&client).expect("initialized");
                account.held -= entry.amount;
                account.available += entry.amount;
                self.entries.get_mut(&tx).expect("entry exists").disputed = false;
            }
            Row::Chargeback { client, tx } => {
                let Some(entry) = self.entries.get(&tx).copied() else {
                    return;
                };
                if entry.client != client || !entry.disputed {
                    return;
                }
                let account = self.accounts.get_mut(&client).expect("initialized");
                account.held -= entry.amount;
                account.locked = true;
                self.entries.remove(&tx);
            }
        }
    }

    fn rows(&self) -> Vec<String> {
        let mut clients: Vec<u16> = self.accounts.keys().copied().collect();
        clients.sort_unstable();
        let mut rows: Vec<String> = clients
            .into_iter()
            .map(|client| {
                let account = &self.accounts[&client];
                format!(
                    "{},{},{},{},{}",
                    client,
                    render(account.available),
                    render(account.held),
                    render(account.available + account.held),
                    account.locked
                )
            })
            .collect();
        rows.sort();
        rows
    }
}

fn render(value: i64) -> String {
    let negative = value < 0;
    let magnitude = value.unsigned_abs();
    let whole = magnitude / SCALE as u64;
    let fraction = magnitude % SCALE as u64;
    let mut digits = format!("{fraction:04}");
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
    }
    if negative {
        format!("-{whole}.{digits}")
    } else {
        format!("{whole}.{digits}")
    }
}

// --- processor harness ----------------------------------------------------

fn temp_path() -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "transaction_processor_it_{}_{}.csv",
        std::process::id(),
        id
    ))
}

async fn run_bytes(input: &[u8]) -> Result<String, ProcessingError> {
    let path = temp_path();
    std::fs::write(&path, input).expect("write temp input");
    let mut output = Vec::new();
    let result = pipeline::process(&path, &mut output).await;
    let _ = std::fs::remove_file(&path);
    result.map(|()| String::from_utf8(output).expect("utf-8 output"))
}

async fn run_rows(rows: &[Row]) -> String {
    let mut input = String::from("type,client,tx,amount\n");
    for row in rows {
        input.push_str(&row.csv_line());
        input.push('\n');
    }
    run_bytes(input.as_bytes())
        .await
        .expect("processor succeeds")
}

fn account_rows(output: &str) -> Vec<String> {
    let mut rows: Vec<String> = output
        .lines()
        .skip(1)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();
    rows.sort();
    rows
}

async fn assert_matches_oracle(rows: &[Row]) {
    let mut oracle = Oracle::default();
    for row in rows {
        oracle.apply(row);
    }
    let output = run_rows(rows).await;
    assert_eq!(account_rows(&output), oracle.rows());
}

fn generate(seed: u64, count: usize) -> Vec<Row> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut rows = Vec::with_capacity(count);
    let mut next_tx: u32 = 1;

    for _ in 0..count {
        let client = rng.gen_range(1..=5u16);
        let choice = rng.gen_range(0..100);
        if choice < 50 {
            rows.push(Row::Deposit {
                client,
                tx: next_tx,
                amount: rng.gen_range(1..=500_000i64),
            });
            next_tx += 1;
        } else if choice < 70 {
            rows.push(Row::Withdrawal {
                client,
                tx: next_tx,
                amount: rng.gen_range(1..=500_000i64),
            });
            next_tx += 1;
        } else {
            let tx = rng.gen_range(1..=next_tx.max(1));
            rows.push(match choice {
                70..=84 => Row::Dispute { client, tx },
                85..=94 => Row::Resolve { client, tx },
                _ => Row::Chargeback { client, tx },
            });
        }
    }
    rows
}

// --- oracle-driven tests --------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_random_sequences_match_the_oracle() {
    for seed in 0..100 {
        let rows = generate(seed, 50);
        assert_matches_oracle(&rows).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ten_thousand_rows_match_the_oracle() {
    let rows = generate(9_999, 10_000);
    assert_matches_oracle(&rows).await;
}

#[tokio::test]
async fn insufficient_funds_leaves_the_balance_unchanged() {
    assert_matches_oracle(&[
        Row::Deposit {
            client: 1,
            tx: 1,
            amount: 30_000,
        },
        Row::Withdrawal {
            client: 1,
            tx: 2,
            amount: 50_000,
        },
    ])
    .await;
}

#[tokio::test]
async fn dispute_resolve_and_chargeback_follow_the_state_machine() {
    assert_matches_oracle(&[
        Row::Deposit {
            client: 1,
            tx: 1,
            amount: 100_000,
        },
        Row::Dispute { client: 1, tx: 1 },
        Row::Resolve { client: 1, tx: 1 },
        Row::Dispute { client: 1, tx: 1 },
        Row::Chargeback { client: 1, tx: 1 },
        Row::Deposit {
            client: 1,
            tx: 2,
            amount: 5_000,
        },
    ])
    .await;
}

#[tokio::test]
async fn over_available_dispute_locks_without_changing_balances() {
    assert_matches_oracle(&[
        Row::Deposit {
            client: 1,
            tx: 1,
            amount: 1_000_000,
        },
        Row::Withdrawal {
            client: 1,
            tx: 2,
            amount: 800_000,
        },
        Row::Dispute { client: 1, tx: 1 },
        Row::Deposit {
            client: 1,
            tx: 3,
            amount: 50_000,
        },
    ])
    .await;
}

#[tokio::test]
async fn exact_available_dispute_is_allowed() {
    assert_matches_oracle(&[
        Row::Deposit {
            client: 1,
            tx: 1,
            amount: 100_000,
        },
        Row::Dispute { client: 1, tx: 1 },
        Row::Resolve { client: 1, tx: 1 },
    ])
    .await;
}

#[tokio::test]
async fn chargeback_after_a_rejected_dispute_leaves_held_funds() {
    assert_matches_oracle(&[
        Row::Deposit {
            client: 1,
            tx: 1,
            amount: 100_000,
        },
        Row::Deposit {
            client: 1,
            tx: 2,
            amount: 200_000,
        },
        Row::Dispute { client: 1, tx: 1 },
        Row::Withdrawal {
            client: 1,
            tx: 3,
            amount: 100_000,
        },
        Row::Dispute { client: 1, tx: 2 },
        Row::Chargeback { client: 1, tx: 2 },
    ])
    .await;
}

#[tokio::test]
async fn rejected_disputes_do_not_lock_the_account() {
    assert_matches_oracle(&[
        Row::Deposit {
            client: 1,
            tx: 1,
            amount: 100_000,
        },
        Row::Dispute { client: 1, tx: 99 },
        Row::Dispute { client: 2, tx: 1 },
        Row::Dispute { client: 1, tx: 1 },
        Row::Dispute { client: 1, tx: 1 },
    ])
    .await;
}

#[tokio::test]
async fn cross_client_unknown_and_duplicate_references_are_rejected() {
    assert_matches_oracle(&[
        Row::Deposit {
            client: 1,
            tx: 1,
            amount: 100_000,
        },
        Row::Dispute { client: 2, tx: 1 },
        Row::Dispute { client: 1, tx: 99 },
        Row::Dispute { client: 1, tx: 1 },
        Row::Dispute { client: 1, tx: 1 },
    ])
    .await;
}

#[tokio::test]
async fn varying_precision_and_leading_dot_amounts_match_the_oracle() {
    assert_matches_oracle(&[
        Row::Deposit {
            client: 1,
            tx: 1,
            amount: 12_000,
        },
        Row::Deposit {
            client: 1,
            tx: 2,
            amount: 1,
        },
        Row::Deposit {
            client: 1,
            tx: 3,
            amount: 5_000,
        },
    ])
    .await;
}

// --- raw-input edge cases -------------------------------------------------

#[tokio::test]
async fn header_only_input_produces_no_account_rows() {
    let output = run_bytes(b"type,client,tx,amount\n")
        .await
        .expect("succeeds");
    assert_eq!(output, "client,available,held,total,locked\n");
}

#[tokio::test]
async fn oversized_fields_are_skipped() {
    let input = b"type,client,tx,amount\n\
deposit,70000,1,5.0\n\
deposit,1,5000000000,5.0\n\
deposit,1,1,922337203685477.5808\n\
deposit,1,2,1.0\n";
    let output = run_bytes(input).await.expect("succeeds");
    assert_eq!(account_rows(&output), vec!["1,1.0,0.0,1.0,false"]);
}

#[tokio::test]
async fn non_ascii_digits_and_invalid_utf8_are_skipped() {
    let mut input = Vec::new();
    input.extend_from_slice(b"type,client,tx,amount\n");
    input.extend_from_slice("deposit,1,1,\u{0661}.\u{0660}\n".as_bytes());
    input.extend_from_slice(b"deposit,1,2,\xff\n");
    input.extend_from_slice(b"deposit,1,3,2.0\n");

    let output = run_bytes(&input).await.expect("succeeds");
    assert_eq!(account_rows(&output), vec!["1,2.0,0.0,2.0,false"]);
}

#[tokio::test]
async fn a_malformed_row_does_not_stop_a_later_valid_row() {
    let input = b"type,client,tx,amount\n\
transfer,1,1,5.0\n\
deposit,1\n\
deposit,1,2,7.0\n";
    let output = run_bytes(input).await.expect("succeeds");
    assert_eq!(account_rows(&output), vec!["1,7.0,0.0,7.0,false"]);
}

#[tokio::test]
async fn missing_input_file_is_an_error() {
    let mut output = Vec::new();
    let result = pipeline::process(Path::new("/nonexistent/transactions.csv"), &mut output).await;
    assert!(result.is_err());
}
