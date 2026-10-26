//! Idempotent migrate command test for both database backends.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use distributed::migrate::{
    run_fresh, run_with_topics, Backend, TopicNames, TopicPartitions,
};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};

const KAFKA_BOOTSTRAP: &str = "localhost:9092";

fn compose_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../acceptance-tests/distributed/compose.yaml")
}

fn service_running(service: &str) -> bool {
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
            .any(|line| line.trim() == service),
        _ => false,
    }
}

fn run_client(command: &str, database: Option<&str>, sql: &str) -> Output {
    let mut process = Command::new("docker");
    process.args([
        "compose",
        "-f",
        compose_file().to_str().expect("utf-8 path"),
        "exec",
        "-T",
        service_name(command),
    ]);
    match command {
        "psql" => {
            process.args([
                "psql",
                "-U",
                "ledger",
                "-d",
                database.expect("psql database"),
                "-v",
                "ON_ERROR_STOP=1",
                "-q",
                "-tA",
            ]);
        }
        "mariadb" => {
            process.args([
                "mariadb",
                "-uroot",
                "-proot",
                "--batch",
                "--raw",
                "--skip-column-names",
            ]);
            if let Some(database) = database {
                process.arg(database);
            }
        }
        other => panic!("unknown client {other}"),
    }

    let mut child = process
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start database client");

    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(sql.as_bytes())
        .expect("write SQL");

    child.wait_with_output().expect("finish database client")
}

fn service_name(client: &str) -> &'static str {
    match client {
        "psql" => "postgres",
        "mariadb" => "mariadb",
        other => panic!("unknown client {other}"),
    }
}

fn reset_postgres() {
    let output = run_client(
        "psql",
        Some("postgres"),
        "DROP DATABASE IF EXISTS ledger_migrate_test; \
         CREATE DATABASE ledger_migrate_test;",
    );
    assert!(
        output.status.success(),
        "postgres reset failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn reset_mariadb() {
    let output = run_client(
        "mariadb",
        None,
        "DROP DATABASE IF EXISTS ledger_migrate_test; \
         CREATE DATABASE ledger_migrate_test; \
         GRANT ALL ON ledger_migrate_test.* TO 'ledger'@'%'; \
         FLUSH PRIVILEGES;",
    );
    assert!(
        output.status.success(),
        "mariadb reset failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn topic_names(label: &str) -> TopicNames {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    TopicNames {
        staging: format!("{label}-staging-{suffix}"),
        transactions: format!("{label}-transactions-{suffix}"),
        dlq: format!("{label}-dlq-{suffix}"),
    }
}

fn kafka_partition_count(topic: &str) -> usize {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BOOTSTRAP)
        .create()
        .expect("metadata consumer");
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .expect("fetch metadata");
    metadata
        .topics()
        .iter()
        .find(|candidate| candidate.name() == topic)
        .map(|candidate| candidate.partitions().len())
        .unwrap_or(0)
}

fn assert_idempotent(report_backend: &str, first: &distributed::migrate::MigrateReport, second: &distributed::migrate::MigrateReport) {
    assert_eq!(first.backend, report_backend);
    assert_eq!(first.migrations_applied, 1);
    assert_eq!(first.topics_created, 3);
    assert_eq!(second.backend, report_backend);
    assert_eq!(second.migrations_applied, 0);
    assert_eq!(second.topics_created, 0);
}

#[tokio::test]
async fn postgres_migrate_is_idempotent() {
    if !service_running("postgres") || !service_running("kafka") {
        eprintln!("skipping PostgreSQL migrate test: profiles are not running");
        return;
    }

    reset_postgres();
    let names = topic_names("pg-migrate");
    let first = run_with_topics(
        Backend::Postgres,
        "postgres://ledger:ledger@localhost:5432/ledger_migrate_test",
        KAFKA_BOOTSTRAP,
        &names,
        TopicPartitions::default(),
    )
    .await
    .expect("first migration");
    let second = run_with_topics(
        Backend::Postgres,
        "postgres://ledger:ledger@localhost:5432/ledger_migrate_test",
        KAFKA_BOOTSTRAP,
        &names,
        TopicPartitions::default(),
    )
    .await
    .expect("second migration");

    assert_idempotent("postgres", &first, &second);

    let fresh = run_fresh(
        Backend::Postgres,
        "postgres://ledger:ledger@localhost:5432/ledger_migrate_test",
        KAFKA_BOOTSTRAP,
    )
    .await
    .expect("fresh migration");
    assert_eq!(fresh.migrations_applied, 1);

    let names = topic_names("pg-partitions");
    let partitions = TopicPartitions {
        staging: 1,
        transactions: 2,
        dlq: 1,
    };
    run_with_topics(
        Backend::Postgres,
        "postgres://ledger:ledger@localhost:5432/ledger_migrate_test",
        KAFKA_BOOTSTRAP,
        &names,
        partitions,
    )
    .await
    .expect("custom partition counts");
    assert_eq!(kafka_partition_count(&names.staging), 1);
    assert_eq!(kafka_partition_count(&names.transactions), 2);
    assert_eq!(kafka_partition_count(&names.dlq), 1);
}

#[tokio::test]
async fn mariadb_migrate_is_idempotent() {
    if !service_running("mariadb") || !service_running("kafka") {
        eprintln!("skipping MariaDB migrate test: profiles are not running");
        return;
    }

    reset_mariadb();
    let names = topic_names("my-migrate");
    let first = run_with_topics(
        Backend::MariaDb,
        "mysql://ledger:ledger@localhost:3306/ledger_migrate_test",
        KAFKA_BOOTSTRAP,
        &names,
        TopicPartitions::default(),
    )
    .await
    .expect("first migration");
    let second = run_with_topics(
        Backend::MariaDb,
        "mysql://ledger:ledger@localhost:3306/ledger_migrate_test",
        KAFKA_BOOTSTRAP,
        &names,
        TopicPartitions::default(),
    )
    .await
    .expect("second migration");

    assert_idempotent("mariadb", &first, &second);

    let fresh = run_fresh(
        Backend::MariaDb,
        "mysql://ledger:ledger@localhost:3306/ledger_migrate_test",
        KAFKA_BOOTSTRAP,
    )
    .await
    .expect("fresh migration");
    assert_eq!(fresh.migrations_applied, 1);
}
