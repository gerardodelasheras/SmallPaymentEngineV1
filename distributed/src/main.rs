//! Entry points for the distributed ledger variant.

use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use distributed::ingest::{ingest, IngestConfig, IngestError, StagingRecord};
use distributed::kafka_staging::KafkaStagingSink;
use distributed::kafka_transactions::KafkaTransactionSink;
use distributed::manifest;
use distributed::migrate::{run, run_fresh, Backend, MigrateError};
use distributed::partition::{
    EmittedEvent, PartitionError, PartitionRepository, Partitioner, StagingPosition,
};
use distributed::repository::mariadb::MariaDbRepository;
use distributed::repository::postgres::PostgresRepository;
use distributed::repository::{LedgerRepository, RepoError};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::{Message, Offset, TopicPartitionList};
use serde::Serialize;

const USAGE: &str = "\
Usage: distributed <command> [options]

Commands:
  ingest      Parse a manifest and stage rows
  partition   Map staged clients and emit ordered transactions
  worker      Consume transactions and apply the durable ledger
  export      Wait for a job and write its accounts CSV
  migrate     Apply database migrations and provision topics

Common options:
  --backend <postgres|mariadb>   Backend (default: postgres)
  --database-url <url>           Default: DATABASE_URL
  --kafka-bootstrap <url>        Default: KAFKA_BOOTSTRAP
  --json                         Print a JSON report

migrate options:
  --fresh                        Reset the schema first (test/teardown only)

ingest options:
  --manifest <path>              Manifest JSON (required)
  --job <id>                     Job id (default: derived from manifest hash)
  --staging-topic <name>         Default: staging
  --max-concurrent-files <n>     Default: 8
";

#[derive(Debug, thiserror::Error)]
enum CommandError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error(transparent)]
    Repo(#[from] RepoError),

    #[error(transparent)]
    Manifest(#[from] manifest::ManifestError),

    #[error(transparent)]
    Ingest(#[from] IngestError),

    #[error(transparent)]
    Partition(#[from] PartitionError),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Serialize)]
struct IngestReport {
    job_id: String,
    manifest_hash: String,
    files: u64,
    rows_staged: u64,
    rows_rejected: u64,
}

#[derive(Debug, Serialize)]
struct PartitionEmitted {
    file_ord: u32,
    row_ord: u32,
    client_id: i64,
    partition: i32,
}

#[derive(Debug, Serialize)]
struct PartitionReport {
    job_id: Option<String>,
    records_seen: u64,
    buffered: usize,
    emitted: Vec<PartitionEmitted>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let Some(command) = args.get(1) else {
        print!("{USAGE}");
        return ExitCode::SUCCESS;
    };

    match command.as_str() {
        "--help" | "-h" | "help" => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        "migrate" => match run_migrate(&args[2..]).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("distributed: migrate failed: {error}");
                ExitCode::from(1)
            }
        },
        "ingest" => match run_ingest(&args[2..]).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("distributed: ingest failed: {error}");
                ExitCode::from(ingest_exit_code(&error))
            }
        },
        "partition" => match run_partition(&args[2..]).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("distributed: partition failed: {error}");
                ExitCode::from(1)
            }
        },
        "worker" | "export" => {
            eprintln!("distributed: '{command}' is not implemented yet");
            ExitCode::from(1)
        }
        other => {
            eprintln!("distributed: unknown command '{other}'\n\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

fn manifest_exit_code(error: &manifest::ManifestError) -> u8 {
    match error {
        manifest::ManifestError::HashMismatch { .. } => 3,
        manifest::ManifestError::DuplicateOrdinal { .. }
        | manifest::ManifestError::MissingFile { .. }
        | manifest::ManifestError::EmptyManifest
        | manifest::ManifestError::EmptySource
        | manifest::ManifestError::InvalidHash { .. }
        | manifest::ManifestError::InvalidJson(_) => 2,
        manifest::ManifestError::ManifestRead { .. }
        | manifest::ManifestError::FileRead { .. } => 1,
    }
}

fn ingest_exit_code(error: &CommandError) -> u8 {
    match error {
        CommandError::Manifest(error) => manifest_exit_code(error),
        CommandError::Ingest(IngestError::Manifest(error)) => manifest_exit_code(error),
        _ => 1,
    }
}

fn take_value(args: &[String], index: &mut usize) -> Result<String, CommandError> {
    let option = args[*index].clone();
    let value = args.get(*index + 1).ok_or_else(|| {
        CommandError::Config(format!("{option} requires a value"))
    })?;
    *index += 2;
    Ok(value.clone())
}

fn parse_backend(value: &str) -> Result<Backend, CommandError> {
    value
        .parse::<Backend>()
        .map_err(|error: MigrateError| CommandError::Config(error.to_string()))
}

async fn run_migrate(args: &[String]) -> Result<(), MigrateError> {
    let mut backend = Backend::Postgres;
    let mut database_url = env::var("DATABASE_URL").ok();
    let mut kafka_bootstrap = env::var("KAFKA_BOOTSTRAP").ok();
    let mut json = false;
    let mut fresh = false;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--backend" => {
                let value = take_value(args, &mut index)
                    .map_err(|error| MigrateError::Config(error.to_string()))?;
                backend = parse_backend(&value)
                    .map_err(|error| MigrateError::Config(error.to_string()))?;
            }
            "--database-url" => {
                database_url = Some(
                    take_value(args, &mut index)
                        .map_err(|error| MigrateError::Config(error.to_string()))?,
                );
            }
            "--kafka-bootstrap" => {
                kafka_bootstrap = Some(
                    take_value(args, &mut index)
                        .map_err(|error| MigrateError::Config(error.to_string()))?,
                );
            }
            "--json" => {
                json = true;
                index += 1;
            }
            "--fresh" => {
                fresh = true;
                index += 1;
            }
            other => {
                return Err(MigrateError::Config(format!(
                    "unknown migrate option '{other}'"
                )));
            }
        }
    }

    let database_url = database_url.ok_or_else(|| {
        MigrateError::Config("DATABASE_URL or --database-url is required".to_string())
    })?;
    let kafka_bootstrap = kafka_bootstrap.ok_or_else(|| {
        MigrateError::Config("KAFKA_BOOTSTRAP or --kafka-bootstrap is required".to_string())
    })?;

    let report = if fresh {
        run_fresh(backend, &database_url, &kafka_bootstrap).await?
    } else {
        run(backend, &database_url, &kafka_bootstrap).await?
    };
    if json {
        println!(
            "{}",
            serde_json::to_string(&report).expect("migration report serializes")
        );
    } else {
        println!(
            "backend={} migrations_applied={} topics_created={}",
            report.backend, report.migrations_applied, report.topics_created
        );
    }
    Ok(())
}

async fn run_ingest(args: &[String]) -> Result<(), CommandError> {
    let mut manifest_path: Option<String> = None;
    let mut job_id: Option<String> = None;
    let mut backend = Backend::Postgres;
    let mut database_url = env::var("DATABASE_URL").ok();
    let mut kafka_bootstrap = env::var("KAFKA_BOOTSTRAP").ok();
    let mut staging_topic = "staging".to_string();
    let mut max_concurrent_files = 8_usize;
    let mut json = false;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--manifest" => manifest_path = Some(take_value(args, &mut index)?),
            "--job" => job_id = Some(take_value(args, &mut index)?),
            "--backend" => {
                let value = take_value(args, &mut index)?;
                backend = parse_backend(&value)?;
            }
            "--database-url" => database_url = Some(take_value(args, &mut index)?),
            "--kafka-bootstrap" => kafka_bootstrap = Some(take_value(args, &mut index)?),
            "--staging-topic" => staging_topic = take_value(args, &mut index)?,
            "--max-concurrent-files" => {
                let value = take_value(args, &mut index)?;
                max_concurrent_files = value.parse::<usize>().map_err(|_| {
                    CommandError::Config(
                        "--max-concurrent-files must be a positive integer".to_string(),
                    )
                })?;
            }
            "--json" => {
                json = true;
                index += 1;
            }
            other => {
                return Err(CommandError::Config(format!(
                    "unknown ingest option '{other}'"
                )));
            }
        }
    }

    let manifest_path = manifest_path.ok_or_else(|| {
        CommandError::Config("--manifest is required".to_string())
    })?;
    let database_url = database_url.ok_or_else(|| {
        CommandError::Config("DATABASE_URL or --database-url is required".to_string())
    })?;
    let kafka_bootstrap = kafka_bootstrap.ok_or_else(|| {
        CommandError::Config("KAFKA_BOOTSTRAP or --kafka-bootstrap is required".to_string())
    })?;

    let manifest = manifest::load(Path::new(&manifest_path))?;
    let job_id = job_id.unwrap_or_else(|| {
        format!("job-{}", &manifest.manifest_hash[..12])
    });

    let repository: Arc<dyn LedgerRepository> = match backend {
        Backend::Postgres => Arc::new(PostgresRepository::connect(&database_url).await?),
        Backend::MariaDb => Arc::new(MariaDbRepository::connect(&database_url).await?),
    };

    repository
        .create_job(&job_id, &manifest.manifest_hash)
        .await?;

    for file in &manifest.files {
        repository
            .create_job_file(
                &job_id,
                &file.source_id,
                file.ordinal,
                &file.sha256,
                &file.path.to_string_lossy(),
            )
            .await?;
    }

    let sink = Arc::new(
        KafkaStagingSink::connect(&kafka_bootstrap, &staging_topic, Arc::clone(&repository))
            .await?,
    );
    let summary = ingest(
        &manifest,
        &job_id,
        sink,
        IngestConfig {
            max_concurrent_files,
        },
    )
    .await?;

    let report = IngestReport {
        job_id,
        manifest_hash: manifest.manifest_hash,
        files: summary.files_processed,
        rows_staged: summary.rows_staged,
        rows_rejected: summary.rows_rejected,
    };

    if json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        println!(
            "job={} files={} rows_staged={} rows_rejected={} manifest_hash={}",
            report.job_id,
            report.files,
            report.rows_staged,
            report.rows_rejected,
            report.manifest_hash
        );
    }
    Ok(())
}

fn append_emitted(
    report: &mut Vec<PartitionEmitted>,
    job_filter: &Option<String>,
    emitted: Vec<EmittedEvent>,
) {
    for event in emitted {
        if let Some(job_id) = job_filter {
            if &event.job_id != job_id {
                continue;
            }
        }
        report.push(PartitionEmitted {
            file_ord: event.file_ord,
            row_ord: event.row_ord,
            client_id: event.client_id,
            partition: event.partition,
        });
    }
}

fn commit_ready(
    consumer: &StreamConsumer,
    topic: &str,
    engine: &Partitioner<KafkaTransactionSink>,
    last_offsets: &HashMap<i32, i64>,
    committed: &mut HashMap<i32, i64>,
) -> Result<(), CommandError> {
    let mut offsets = TopicPartitionList::new();
    let mut any = false;

    for (partition, offset) in last_offsets {
        if engine.has_buffered_for(*partition) {
            continue;
        }
        let next = offset + 1;
        if committed.get(partition).copied().unwrap_or(-1) >= next {
            continue;
        }
        offsets
            .add_partition_offset(topic, *partition, Offset::Offset(next))
            .map_err(|error| {
                CommandError::Config(format!("build offset list: {error}"))
            })?;
        any = true;
    }

    if any {
        consumer
            .commit(&offsets, CommitMode::Sync)
            .map_err(|error| {
                CommandError::Config(format!("commit staging offsets: {error}"))
            })?;
        for (partition, offset) in last_offsets {
            if !engine.has_buffered_for(*partition) {
                committed.insert(*partition, offset + 1);
            }
        }
    }
    Ok(())
}

async fn run_partition(args: &[String]) -> Result<(), CommandError> {
    let mut backend = Backend::Postgres;
    let mut database_url = env::var("DATABASE_URL").ok();
    let mut kafka_bootstrap = env::var("KAFKA_BOOTSTRAP").ok();
    let mut staging_topic = "staging".to_string();
    let mut transactions_topic = "transactions".to_string();
    let mut group: Option<String> = None;
    let mut job_filter: Option<String> = None;
    let mut max_buffered_rows = 100_000_usize;
    let mut expect: Option<u64> = None;
    let mut drain = false;
    let mut json = false;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--backend" => {
                let value = take_value(args, &mut index)?;
                backend = parse_backend(&value)?;
            }
            "--database-url" => database_url = Some(take_value(args, &mut index)?),
            "--kafka-bootstrap" => kafka_bootstrap = Some(take_value(args, &mut index)?),
            "--staging-topic" => staging_topic = take_value(args, &mut index)?,
            "--transactions-topic" => transactions_topic = take_value(args, &mut index)?,
            "--group" => group = Some(take_value(args, &mut index)?),
            "--job" => job_filter = Some(take_value(args, &mut index)?),
            "--max-buffered-rows" => {
                let value = take_value(args, &mut index)?;
                max_buffered_rows = value.parse::<usize>().map_err(|_| {
                    CommandError::Config(
                        "--max-buffered-rows must be a positive integer".to_string(),
                    )
                })?;
            }
            "--expect" => {
                let value = take_value(args, &mut index)?;
                expect = Some(value.parse::<u64>().map_err(|_| {
                    CommandError::Config("--expect must be a non-negative integer".to_string())
                })?);
            }
            "--drain" => {
                drain = true;
                index += 1;
            }
            "--json" => {
                json = true;
                index += 1;
            }
            other => {
                return Err(CommandError::Config(format!(
                    "unknown partition option '{other}'"
                )));
            }
        }
    }

    let database_url = database_url.ok_or_else(|| {
        CommandError::Config("DATABASE_URL or --database-url is required".to_string())
    })?;
    let kafka_bootstrap = kafka_bootstrap.ok_or_else(|| {
        CommandError::Config("KAFKA_BOOTSTRAP or --kafka-bootstrap is required".to_string())
    })?;

    let repository: Arc<dyn PartitionRepository> = match backend {
        Backend::Postgres => Arc::new(PostgresRepository::connect(&database_url).await?),
        Backend::MariaDb => Arc::new(MariaDbRepository::connect(&database_url).await?),
    };
    let sink = Arc::new(
        KafkaTransactionSink::connect(&kafka_bootstrap, &transactions_topic).await?,
    );
    let mut engine = Partitioner::new(Arc::clone(&sink), repository, max_buffered_rows);

    let group = group.unwrap_or_else(|| match &job_filter {
        Some(job_id) => format!("distributed-partitioner-{job_id}"),
        None => "distributed-partitioner".to_string(),
    });
    let consumer: StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &kafka_bootstrap)
        .set("group.id", &group)
        .set("client.id", "distributed-partitioner")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .create()
        .map_err(|error| {
            CommandError::Config(format!("create Kafka consumer: {error}"))
        })?;
    consumer
        .subscribe(&[staging_topic.as_str()])
        .map_err(|error| {
            CommandError::Config(format!("subscribe to staging topic: {error}"))
        })?;

    let expected = expect.unwrap_or(u64::MAX);
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    let mut last_offsets: HashMap<i32, i64> = HashMap::new();
    let mut committed: HashMap<i32, i64> = HashMap::new();
    let mut records_seen = 0_u64;
    let mut report = Vec::new();
    let mut last_message = Instant::now();

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let emitted = engine.tick().await?;
                append_emitted(&mut report, &job_filter, emitted);
                commit_ready(
                    &consumer,
                    &staging_topic,
                    &engine,
                    &last_offsets,
                    &mut committed,
                )?;

                if drain && records_seen >= expected && engine.buffered_rows() == 0 {
                    break;
                }
                if drain
                    && engine.buffered_rows() == 0
                    && last_message.elapsed() >= Duration::from_secs(5)
                {
                    break;
                }
            }
            message = consumer.recv() => {
                match message {
                    Ok(message) => {
                        last_message = Instant::now();
                        let partition = message.partition();
                        let offset = message.offset();
                        last_offsets.insert(partition, offset);

                        let payload = message.payload().unwrap_or_default();
                        let staging: StagingRecord = serde_json::from_slice(payload)?;

                        if let Some(job_id) = &job_filter {
                            if &staging.job_id != job_id {
                                commit_ready(
                                    &consumer,
                                    &staging_topic,
                                    &engine,
                                    &last_offsets,
                                    &mut committed,
                                )?;
                                continue;
                            }
                        }

                        records_seen += 1;
                        let position = StagingPosition { partition, offset };
                        loop {
                            match engine.push(&staging, position).await {
                                Ok(emitted) => {
                                    append_emitted(&mut report, &job_filter, emitted);
                                    commit_ready(
                                        &consumer,
                                        &staging_topic,
                                        &engine,
                                        &last_offsets,
                                        &mut committed,
                                    )?;
                                    break;
                                }
                                Err(PartitionError::BufferFull(_)) => {
                                    let emitted = engine.tick().await?;
                                    append_emitted(&mut report, &job_filter, emitted);
                                    commit_ready(
                                        &consumer,
                                        &staging_topic,
                                        &engine,
                                        &last_offsets,
                                        &mut committed,
                                    )?;
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                                Err(error) => return Err(CommandError::Partition(error)),
                            }
                        }

                        if drain && records_seen >= expected && engine.buffered_rows() == 0 {
                            break;
                        }
                    }
                    Err(KafkaError::PartitionEOF(_)) => {}
                    Err(error) => {
                        return Err(CommandError::Config(format!(
                            "consume staging topic: {error}"
                        )));
                    }
                }
            }
        }
    }

    let report = PartitionReport {
        job_id: job_filter,
        records_seen,
        buffered: engine.buffered_rows(),
        emitted: report,
    };

    if json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        println!(
            "job={:?} records_seen={} buffered={} emitted={}",
            report.job_id,
            report.records_seen,
            report.buffered,
            report.emitted.len()
        );
    }
    Ok(())
}
