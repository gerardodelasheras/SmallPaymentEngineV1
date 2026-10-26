//! Idempotent database migration and Kafka topic provisioning.

use std::str::FromStr;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::config::ClientConfig;
use rdkafka::error::RDKafkaErrorCode;
use serde::Serialize;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::repository::RepoError;

const POSTGRES_SQL: &str = include_str!("../migrations/postgres/0001_init.sql");
const MARIADB_SQL: &str = include_str!("../migrations/mariadb/0001_init.sql");

const KAFKA_CLIENT_ID: &str = "distributed-migrate";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Postgres,
    MariaDb,
}

impl Backend {
    pub fn as_str(self) -> &'static str {
        match self {
            Backend::Postgres => "postgres",
            Backend::MariaDb => "mariadb",
        }
    }
}

impl FromStr for Backend {
    type Err = MigrateError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "postgres" | "postgresql" => Ok(Backend::Postgres),
            "mariadb" | "mysql" => Ok(Backend::MariaDb),
            other => Err(MigrateError::Config(format!(
                "unknown backend '{other}'; expected postgres or mariadb"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopicNames {
    pub staging: String,
    pub transactions: String,
    pub dlq: String,
}

impl Default for TopicNames {
    fn default() -> Self {
        Self {
            staging: "staging".to_string(),
            transactions: "transactions".to_string(),
            dlq: "dlq".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicPartitions {
    pub staging: i32,
    pub transactions: i32,
    pub dlq: i32,
}

impl Default for TopicPartitions {
    fn default() -> Self {
        Self {
            staging: 3,
            transactions: 3,
            dlq: 1,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct MigrateReport {
    pub backend: String,
    pub migrations_applied: u32,
    pub topics_created: u32,
    pub topics: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum MigrateError {
    #[error("database error: {0}")]
    Database(#[from] RepoError),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("kafka error: {0}")]
    Kafka(String),
}

impl From<sqlx::Error> for MigrateError {
    fn from(error: sqlx::Error) -> Self {
        MigrateError::Database(RepoError::from(error))
    }
}

const MARIADB_TABLES: [&str; 12] = [
    "job_lock",
    "job_emitted",
    "job_clients",
    "job_progress",
    "job_events",
    "ledger_events",
    "job_files",
    "transactions",
    "accounts",
    "jobs",
    "id_allocator",
    "clients",
];

/// Reset the schema, then apply migrations and provision topics.
///
/// This is a test/teardown helper. It is destructive to the selected database.
pub async fn run_fresh(
    backend: Backend,
    database_url: &str,
    kafka_bootstrap: &str,
) -> Result<MigrateReport, MigrateError> {
    match backend {
        Backend::Postgres => reset_postgres(database_url).await?,
        Backend::MariaDb => reset_mariadb(database_url).await?,
    }

    run(backend, database_url, kafka_bootstrap).await
}

/// Reset the database and recreate the default Kafka topics.
///
/// Topic recreation is deliberately separate from a plain fresh migration: a
/// database-portability run must not delete topics that another scenario is
/// using.
pub async fn run_fresh_with_topics(
    backend: Backend,
    database_url: &str,
    kafka_bootstrap: &str,
) -> Result<MigrateReport, MigrateError> {
    let names = TopicNames::default();
    let topics = [
        names.staging.as_str(),
        names.transactions.as_str(),
        names.dlq.as_str(),
    ];
    delete_topics(kafka_bootstrap, &topics).await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    run_fresh(backend, database_url, kafka_bootstrap).await
}

async fn delete_topics(
    kafka_bootstrap: &str,
    topics: &[&str],
) -> Result<(), MigrateError> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", kafka_bootstrap)
        .set("client.id", KAFKA_CLIENT_ID)
        .create()
        .map_err(|error| {
            MigrateError::Kafka(format!("connect to Kafka admin: {error}"))
        })?;

    let results = admin
        .delete_topics(topics, &AdminOptions::new())
        .await
        .map_err(|error| {
            MigrateError::Kafka(format!("delete Kafka topics: {error}"))
        })?;

    for result in results {
        match result {
            Ok(_) => {}
            Err((_, RDKafkaErrorCode::UnknownTopicOrPartition)) => {}
            Err((name, code)) => {
                return Err(MigrateError::Kafka(format!(
                    "delete topic '{name}': {code:?}"
                )));
            }
        }
    }
    Ok(())
}

async fn reset_postgres(database_url: &str) -> Result<(), MigrateError> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    sqlx::raw_sql("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

async fn reset_mariadb(database_url: &str) -> Result<(), MigrateError> {
    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;
    sqlx::query("SET FOREIGN_KEY_CHECKS = 0")
        .execute(&pool)
        .await?;
    for table in MARIADB_TABLES {
        sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
            .execute(&pool)
            .await?;
    }
    sqlx::query("SET FOREIGN_KEY_CHECKS = 1")
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(())
}

pub async fn run(
    backend: Backend,
    database_url: &str,
    kafka_bootstrap: &str,
) -> Result<MigrateReport, MigrateError> {
    run_with_topics(
        backend,
        database_url,
        kafka_bootstrap,
        &TopicNames::default(),
        TopicPartitions::default(),
    )
    .await
}

pub async fn run_with_topics(
    backend: Backend,
    database_url: &str,
    kafka_bootstrap: &str,
    names: &TopicNames,
    partitions: TopicPartitions,
) -> Result<MigrateReport, MigrateError> {
    if database_url.trim().is_empty() {
        return Err(MigrateError::Config(
            "a database URL is required (DATABASE_URL or --database-url)".to_string(),
        ));
    }
    if kafka_bootstrap.trim().is_empty() {
        return Err(MigrateError::Config(
            "a Kafka bootstrap address is required (KAFKA_BOOTSTRAP or --kafka-bootstrap)"
                .to_string(),
        ));
    }

    let migrations_applied = match backend {
        Backend::Postgres => apply_postgres(database_url).await?,
        Backend::MariaDb => apply_mariadb(database_url).await?,
    };

    let topics = [
        (names.staging.as_str(), partitions.staging),
        (names.transactions.as_str(), partitions.transactions),
        (names.dlq.as_str(), partitions.dlq),
    ];
    let topics_created = provision_topics(kafka_bootstrap, &topics).await?;

    Ok(MigrateReport {
        backend: backend.as_str().to_string(),
        migrations_applied,
        topics_created,
        topics: topics.iter().map(|(name, _)| name.to_string()).collect(),
    })
}

fn statements(sql: &str) -> Vec<String> {
    let without_comments = sql
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");

    without_comments
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(str::to_string)
        .collect()
}

async fn table_exists_postgres(pool: &PgPool) -> Result<bool, RepoError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables \
         WHERE table_schema = 'public' AND table_name = 'clients'",
    )
    .fetch_one(pool)
    .await?;
    Ok(count > 0)
}

async fn apply_postgres(database_url: &str) -> Result<u32, MigrateError> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    let existed = table_exists_postgres(&pool).await?;
    for statement in statements(POSTGRES_SQL) {
        sqlx::query(&statement).execute(&pool).await?;
    }
    pool.close().await;
    Ok(u32::from(!existed))
}

async fn table_exists_mariadb(pool: &MySqlPool) -> Result<bool, RepoError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables \
         WHERE table_schema = DATABASE() AND table_name = 'clients'",
    )
    .fetch_one(pool)
    .await?;
    Ok(count > 0)
}

async fn apply_mariadb(database_url: &str) -> Result<u32, MigrateError> {
    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    let existed = table_exists_mariadb(&pool).await?;
    for statement in statements(MARIADB_SQL) {
        sqlx::query(&statement).execute(&pool).await?;
    }
    pool.close().await;
    Ok(u32::from(!existed))
}

async fn provision_topics(
    kafka_bootstrap: &str,
    topics: &[(&str, i32)],
) -> Result<u32, MigrateError> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", kafka_bootstrap)
        .set("client.id", KAFKA_CLIENT_ID)
        .create()
        .map_err(|error| {
            MigrateError::Kafka(format!("connect to Kafka admin: {error}"))
        })?;

    let new_topics: Vec<NewTopic<'_>> = topics
        .iter()
        .map(|(name, partitions)| {
            NewTopic::new(name, *partitions, TopicReplication::Fixed(1))
        })
        .collect();

    let results = admin
        .create_topics(&new_topics, &AdminOptions::new())
        .await
        .map_err(|error| {
            MigrateError::Kafka(format!("create Kafka topics: {error}"))
        })?;

    let mut created = 0;
    for result in results {
        match result {
            Ok(_) => created += 1,
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => {
                return Err(MigrateError::Kafka(format!(
                    "create topic '{name}': {code:?}"
                )));
            }
        }
    }
    // Topic creation is asynchronous. Wait until every topic has a leader before
    // an immediate producer or consumer connects.
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", kafka_bootstrap)
        .set("client.id", KAFKA_CLIENT_ID)
        .create()
        .map_err(|error| {
            MigrateError::Kafka(format!("create metadata client: {error}"))
        })?;
    for (name, _) in topics {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let ready = consumer
                .fetch_metadata(Some(name), std::time::Duration::from_secs(2))
                .ok()
                .and_then(|metadata| {
                    metadata
                        .topics()
                        .iter()
                        .find(|topic| topic.name() == *name)
                        .map(|topic| {
                            !topic.partitions().is_empty()
                                && topic
                                    .partitions()
                                    .iter()
                                    .all(|partition| partition.leader() >= 0)
                        })
                })
                .unwrap_or(false);
            if ready {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Err(MigrateError::Kafka(format!(
                    "topic '{name}' did not become ready"
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    Ok(created)
}

