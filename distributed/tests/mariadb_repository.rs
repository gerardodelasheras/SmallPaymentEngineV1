//! MariaDB repository contract test.
//!
//! The test uses a throwaway `ledger_repo_test` database and grants the runtime
//! `ledger` user access to it.

use std::io::Write;
use std::sync::Arc;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

use distributed::repository::mariadb::MariaDbRepository;
use distributed::repository::{JobState, LedgerEvent, LedgerRepository};
use transaction_processor::{
    Amount, CanonicalClientId, EntryState, LedgerTransaction, ProcessingError, TransactionType,
};

const DATABASE_URL: &str = "mysql://ledger:ledger@localhost:3306/ledger_repo_test";

fn compose_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../acceptance-tests/distributed/compose.yaml")
}

fn mariadb_running() -> bool {
    let output = Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_file().to_str().expect("utf-8 path"),
            "ps",
            "--services",
            "--status",
            "running",
        ])
        .output();
    match output {
        Ok(output) if output.status.success() => String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line.trim() == "mariadb"),
        _ => false,
    }
}

fn mariadb(database: Option<&str>, sql: &str) -> Output {
    let mut command = Command::new("docker");
    command.args([
        "compose",
        "-f",
        compose_file().to_str().expect("utf-8 path"),
        "exec",
        "-T",
        "mariadb",
        "mariadb",
        "-uroot",
        "-proot",
        "--batch",
        "--raw",
        "--skip-column-names",
    ]);
    if let Some(database) = database {
        command.arg(database);
    }

    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start mariadb client");

    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(sql.as_bytes())
        .expect("write SQL");

    child.wait_with_output().expect("finish mariadb client")
}

fn reset_database() {
    let output = mariadb(
        None,
        "DROP DATABASE IF EXISTS ledger_repo_test; \
         CREATE DATABASE ledger_repo_test; \
         GRANT ALL ON ledger_repo_test.* TO 'ledger'@'%';",
    );
    assert!(
        output.status.success(),
        "reset failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let migration = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations/mariadb/0001_init.sql"),
    )
    .expect("read migration");
    let output = mariadb(Some("ledger_repo_test"), &migration);
    assert!(
        output.status.success(),
        "migration failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn amount(value: &str) -> Amount {
    Amount::parse(value).expect("valid amount")
}

fn hash(seed: &str) -> String {
    format!("{seed:0<64}")
}

fn event(
    job_id: &str,
    client: CanonicalClientId,
    transaction_type: TransactionType,
    transaction_id: u32,
    value: Option<&str>,
    event_seed: &str,
    row_ord: i32,
) -> LedgerEvent {
    LedgerEvent {
        event_id: hash(event_seed),
        job_id: job_id.to_string(),
        source_id: "alpha".to_string(),
        file_hash: hash("file"),
        file_ord: 0,
        row_ord,
        partition: 0,
        transaction: LedgerTransaction {
            transaction_type,
            client,
            transaction_id,
            amount: value.map(amount),
        },
    }
}

fn account_for(
    accounts: &[distributed::repository::AccountRow],
    client: CanonicalClientId,
) -> distributed::repository::AccountRow {
    *accounts
        .iter()
        .find(|account| account.client_id == client)
        .expect("account row")
}

#[tokio::test]
async fn mariadb_repository_applies_events_atomically() {
    if !mariadb_running() {
        eprintln!("skipping MariaDB repository test: profile is not running");
        return;
    }

    reset_database();
    let repository = MariaDbRepository::connect(DATABASE_URL)
        .await
        .expect("connect");
    sqlx::query("UPDATE id_allocator SET block_size = 2 WHERE name = 'client'")
        .execute(repository.pool())
        .await
        .expect("set block size");

    let job = "job-repository";
    repository
        .create_job(job, &hash("manifest"))
        .await
        .expect("create job");
    repository
        .record_emitted(job, 0, 20)
        .await
        .expect("record emitted");

    let client = repository
        .mint_or_get_client("alpha", 7)
        .await
        .expect("mint client");
    assert_eq!(
        client,
        repository
            .mint_or_get_client("alpha", 7)
            .await
            .expect("get client")
    );
    let other_source = repository
        .mint_or_get_client("beta", 7)
        .await
        .expect("second source");
    assert_ne!(client, other_source);
    let third = repository
        .mint_or_get_client("alpha", 8)
        .await
        .expect("renew block");
    let fourth = repository
        .mint_or_get_client("alpha", 9)
        .await
        .expect("finish block");
    let mut ids = vec![client, other_source, third, fourth];
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 4);
    let next_id: i64 = sqlx::query_scalar("SELECT next_id FROM id_allocator WHERE name = 'client'")
        .fetch_one(repository.pool())
        .await
        .expect("next id");
    assert_eq!(next_id, 5);

    let deposit = event(
        job,
        client,
        TransactionType::Deposit,
        1,
        Some("10.0"),
        "event-deposit",
        0,
    );
    let outcome = repository.apply_event(&deposit).await.expect("deposit");
    assert!(!outcome.duplicate);
    assert!(outcome.row_error.is_none());
    let account = account_for(
        &repository.accounts_for_job(job).await.expect("accounts"),
        client,
    );
    assert_eq!(account.available, amount("10.0"));
    assert!(!account.locked);
    assert_eq!(
        repository
            .transaction(1)
            .await
            .expect("lookup")
            .expect("settled")
            .state,
        EntryState::Settled
    );

    let duplicate = repository.apply_event(&deposit).await.expect("redelivery");
    assert!(duplicate.duplicate);

    repository
        .apply_event(&event(
            job,
            client,
            TransactionType::Dispute,
            1,
            None,
            "event-dispute",
            1,
        ))
        .await
        .expect("dispute");
    let outcome = repository
        .apply_event(&event(
            job,
            client,
            TransactionType::Chargeback,
            1,
            None,
            "event-chargeback",
            2,
        ))
        .await
        .expect("chargeback");
    assert!(outcome.locked);
    assert_eq!(
        repository
            .transaction(1)
            .await
            .expect("lookup")
            .expect("charged back")
            .state,
        EntryState::ChargedBack
    );

    let client_two = repository
        .mint_or_get_client("alpha", 8)
        .await
        .expect("second client");
    repository
        .apply_event(&event(
            job,
            client_two,
            TransactionType::Deposit,
            2,
            Some("100.0"),
            "event-deposit-two",
            3,
        ))
        .await
        .expect("second deposit");
    repository
        .apply_event(&event(
            job,
            client_two,
            TransactionType::Withdrawal,
            3,
            Some("80.0"),
            "event-withdrawal-two",
            4,
        ))
        .await
        .expect("second withdrawal");
    let outcome = repository
        .apply_event(&event(
            job,
            client_two,
            TransactionType::Dispute,
            2,
            None,
            "event-over-available",
            5,
        ))
        .await
        .expect("over-available dispute");
    assert!(matches!(
        outcome.row_error,
        Some(ProcessingError::DisputeExceedsAvailable { .. })
    ));
    assert!(outcome.locked);
    let account = account_for(
        &repository.accounts_for_job(job).await.expect("accounts"),
        client_two,
    );
    assert_eq!(account.available, amount("20.0"));
    assert_eq!(account.held, Amount::ZERO);
    assert!(account.locked);
    assert_eq!(
        repository
            .transaction(2)
            .await
            .expect("lookup")
            .expect("still settled")
            .state,
        EntryState::Settled
    );

    let progress = repository.job_progress(job).await.expect("progress");
    assert_eq!(progress.len(), 1);
    assert_eq!(progress[0].emitted, 20);
    assert_eq!(progress[0].processed, 6);
    let clients = repository.job_clients(job).await.expect("job clients");
    assert!(clients.contains(&client));
    assert!(clients.contains(&client_two));

    repository
        .finish_job(job, JobState::Completed, None)
        .await
        .expect("finish job");

    // Concurrent first sightings converge on one canonical ID.
    let shared = Arc::new(
        MariaDbRepository::connect(DATABASE_URL)
            .await
            .expect("shared connection"),
    );
    let mut handles = Vec::new();
    for _ in 0..8 {
        let repository = Arc::clone(&shared);
        handles.push(tokio::spawn(async move {
            repository
                .mint_or_get_client("shared", 1)
                .await
                .expect("concurrent mint")
        }));
    }
    let mut concurrent = Vec::new();
    for handle in handles {
        concurrent.push(handle.await.expect("join"));
    }
    concurrent.sort_unstable();
    concurrent.dedup();
    assert_eq!(concurrent.len(), 1);
    let rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM clients WHERE source_id = 'shared'",
    )
    .fetch_one(shared.pool())
    .await
    .expect("client count");
    assert_eq!(rows, 1);
}
