//! Applies the MariaDB migration to the running compose database.
//!
//! The test creates a throwaway `ledger_test` database as root, so it never
//! touches the runtime `ledger` database. It is a no-op when the MariaDB
//! profile is not running.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

const TABLES: [&str; 11] = [
    "accounts",
    "clients",
    "id_allocator",
    "job_clients",
    "job_emitted",
    "job_events",
    "job_files",
    "job_lock",
    "job_progress",
    "jobs",
    "ledger_events",
];

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

fn mariadb(sql: &str, database: Option<&str>) -> Output {
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

fn expect_failure(sql: &str, marker: &str) {
    let output = mariadb(sql, Some("ledger_test"));
    assert!(
        !output.status.success(),
        "statement unexpectedly succeeded: {sql}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr).to_lowercase();
    assert!(
        stderr.contains(marker),
        "expected `{marker}` in: {stderr}"
    );
}

#[test]
fn mariadb_migration_creates_the_schema_and_enforces_constraints() {
    if !mariadb_running() {
        eprintln!("skipping MariaDB migration test: profile is not running");
        return;
    }

    let reset = mariadb(
        "DROP DATABASE IF EXISTS ledger_test; CREATE DATABASE ledger_test;",
        None,
    );
    assert!(
        reset.status.success(),
        "could not create ledger_test: {}",
        String::from_utf8_lossy(&reset.stderr)
    );

    let migration = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations/mariadb/0001_init.sql"),
    )
    .expect("read migration");

    for pass in 1..=2 {
        let output = mariadb(&migration, Some("ledger_test"));
        assert!(
            output.status.success(),
            "migration pass {pass} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let output = mariadb(
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = 'ledger_test' ORDER BY table_name",
        Some("ledger_test"),
    );
    assert!(output.status.success());
    let present: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_string)
        .collect();
    for table in TABLES {
        assert!(present.iter().any(|name| name == table), "missing {table}");
    }

    expect_failure(
        "INSERT INTO clients (client_id, source_id, external_client) \
         VALUES (1, 'alpha', 1), (2, 'alpha', 1);",
        "duplicate entry",
    );

    expect_failure(
        "INSERT INTO clients (client_id, source_id, external_client) \
         VALUES (3, 'alpha', 3); \
         INSERT INTO accounts (client_id, available) VALUES (3, -1);",
        "constraint",
    );

    expect_failure(
        "INSERT INTO clients (client_id, source_id, external_client) \
         VALUES (1, 'alpha', 1), (2, 'alpha', 2); \
         INSERT INTO transactions (tx_id, client_id, amount, state) \
         VALUES (10, 1, 10000, 'settled'); \
         INSERT INTO transactions (tx_id, client_id, amount, state) \
         VALUES (10, 2, 10000, 'settled');",
        "duplicate entry",
    );
}
