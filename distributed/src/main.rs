//! Entry points for the distributed ledger variant.

use std::collections::{HashMap, HashSet};
use std::env;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use distributed::ingest::{ingest, IngestConfig, IngestError, StagingRecord};
use distributed::kafka_dlq::{DlqRecord, KafkaDlqSink};
use distributed::kafka_staging::KafkaStagingSink;
use distributed::kafka_transactions::KafkaTransactionSink;
use distributed::manifest;
use distributed::migrate::{
    run_fresh, run_fresh_with_topics, run_with_topics, Backend, MigrateError, TopicNames,
    TopicPartitions,
};
use distributed::partition::{
    EmittedEvent, PartitionError, PartitionRepository, Partitioner, StagingPosition,
    StalledWatermark, TransactionRecord,
};
use distributed::repository::mariadb::MariaDbRepository;
use distributed::repository::postgres::PostgresRepository;
use distributed::repository::{
    JobFileInput, JobState, LedgerEvent, LedgerRepository, RepoError,
};
use transaction_processor::{Amount, LedgerTransaction, ProcessingError};
use tracing_subscriber::EnvFilter;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::{Message, Offset, TopicPartitionList};
use serde::Serialize;
use sha2::{Digest, Sha256};

const USAGE: &str = "\
Usage: distributed <command> [options]

Commands:
  ingest      Parse a manifest and stage rows
  partition   Map staged clients and emit ordered transactions
  worker      Consume transactions and apply the durable ledger
  config      Show the effective, redacted configuration
  health      Report broker and database readiness
  metrics     Report lag, DLQ depth, stalled watermarks, and failed jobs
  identity    Mint or look up a canonical client ID
  journal     Query the durable transaction journal
  job         Query job status
  dlq         Query the dead-letter path
  export      Wait for a job and write its accounts CSV
  migrate     Apply database migrations and provision topics

Common options:
  --backend <postgres|mariadb>   Backend (default: postgres)
  --database-url <url>           Default: DATABASE_URL
  --kafka-bootstrap <url>        Default: KAFKA_BOOTSTRAP
  --json                         Print a JSON report

migrate options:
  --staging-topic <name>         Staging topic to provision (default: staging)
  --staging-partitions <n>       Default: 3
  --transactions-partitions <n>  Default: 3 (changing this rekeys clients)
  --dlq-partitions <n>           Default: 1
  --fresh                        Reset the schema only (test/teardown only)
  --fresh-topics                 Also recreate the topics with these counts

ingest options:
  --manifest <path>              Manifest JSON (required)
  --job <id>                     Job id (default: derived from manifest hash)
  --staging-topic <name>         Default: staging
  --max-concurrent-files <n>     Default: 8
  --crash-after-rows <n>         Test hook: simulate a crash after n rows

worker options:
  --transactions-topic <name>    Default: transactions
  --job <id>                     Filter for a finite drain run
  --drain                        Assign partitions directly and exit
  --expect <n>                   Rows expected for the job in a drain run
  --redeliver                    Test hook: replay from the beginning

partition options:
  --staging-topic <name>         Default: staging
  --transactions-topic <name>    Default: transactions
  --group <id>                   Consumer group (default: per job)
  --drain                        Assign partitions directly and exit
  --expect <n>                   Rows expected in a drain run
  --idle-exit <secs>             Group mode: exit after this much idle
  --startup-delay <ms>           Group mode: wait before processing
  --partition <id>               Drain mode: only this partition (repeatable)
  --max-buffered-rows <n>        Default: 100000
  --stall-timeout <secs>         Drain mode stall timeout (default: 5)

export options:
  --job <id>                     Job to export
  --out <path>                   CSV destination
  --check                        Report readiness without writing the CSV
  --timeout <secs>               Wait budget (default: 120)

job options:
  status --job <id>              Report state and partition progress

journal options:
  --tx <id>                      Transaction id to query

config options:
  show                           Print the effective configuration

identity options:
  mint --source <id> --client <n>  Mint or get the canonical BIGINT ID

dlq options:
  list                           List dead-letter records
  --job <id>                     Filter by job
  --dlq-topic <name>             Default: dlq
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

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("job failed: {0}")]
    JobFailed(String),

    #[error("job rejected: {0}")]
    JobRejected(String),
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
    event_id: String,
    file_ord: u32,
    row_ord: u32,
    client_id: i64,
    partition: i32,
}

#[derive(Debug, Serialize)]
struct PartitionReport {
    job_id: Option<String>,
    staging_partitions: Vec<i32>,
    records_seen: u64,
    buffered: usize,
    backpressure_applied: bool,
    stalled: bool,
    stalled_watermarks: Vec<StalledWatermark>,
    emitted: Vec<PartitionEmitted>,
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
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
        "worker" => match run_worker(&args[2..]).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(CommandError::JobFailed(message)) => {
                eprintln!("distributed: worker failed: {message}");
                ExitCode::from(3)
            }
            Err(error) => {
                eprintln!("distributed: worker failed: {error}");
                ExitCode::from(1)
            }
        },
        "identity" => match run_identity(&args[2..]).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("distributed: identity failed: {error}");
                ExitCode::from(1)
            }
        },
        "dlq" => match run_dlq(&args[2..]).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("distributed: dlq failed: {error}");
                ExitCode::from(1)
            }
        },
        "config" => match run_config(&args[2..]).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("distributed: config failed: {error}");
                ExitCode::from(1)
            }
        },
        "health" => match run_health(&args[2..]).await {
            Ok(ready) => {
                if ready {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::from(1)
                }
            }
            Err(error) => {
                eprintln!("distributed: health failed: {error}");
                ExitCode::from(1)
            }
        },
        "metrics" => match run_metrics(&args[2..]).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("distributed: metrics failed: {error}");
                ExitCode::from(1)
            }
        },
        "journal" => match run_journal(&args[2..]).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("distributed: journal failed: {error}");
                ExitCode::from(1)
            }
        },
        "export" => match run_export(&args[2..]).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(CommandError::JobFailed(message)) => {
                eprintln!("distributed: export failed: {message}");
                ExitCode::from(3)
            }
            Err(CommandError::JobRejected(message)) => {
                eprintln!("distributed: export rejected: {message}");
                ExitCode::from(2)
            }
            Err(error) => {
                eprintln!("distributed: export failed: {error}");
                ExitCode::from(1)
            }
        },
        "job" => match run_job_command(&args[2..]).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("distributed: job failed: {error}");
                ExitCode::from(1)
            }
        },
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
        CommandError::Repo(RepoError::JobAlreadyActive { .. }) => 2,
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

/// Infer the backend from `DATABASE_URL` so `mysql://` URLs do not require a
/// repeated `--backend mariadb` flag. An explicit `--backend` still wins.
fn default_backend() -> Backend {
    match env::var("DATABASE_URL") {
        Ok(url) if url.trim_start().starts_with("mysql://") => Backend::MariaDb,
        _ => Backend::Postgres,
    }
}

fn parse_backend(value: &str) -> Result<Backend, CommandError> {
    value
        .parse::<Backend>()
        .map_err(|error: MigrateError| CommandError::Config(error.to_string()))
}

async fn run_migrate(args: &[String]) -> Result<(), MigrateError> {
    let mut backend = default_backend();
    let mut database_url = env::var("DATABASE_URL").ok();
    let mut kafka_bootstrap = env::var("KAFKA_BOOTSTRAP").ok();
    let mut staging_topic = "staging".to_string();
    let mut partitions = TopicPartitions::default();
    let mut json = false;
    let mut fresh = false;
    let mut fresh_topics = false;

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
            "--fresh-topics" => {
                fresh_topics = true;
                index += 1;
            }
            "--staging-topic" => {
                staging_topic = take_value(args, &mut index)
                    .map_err(|error| MigrateError::Config(error.to_string()))?;
            }
            "--staging-partitions" => {
                partitions.staging = parse_partition_count(
                    &take_value(args, &mut index)
                        .map_err(|error| MigrateError::Config(error.to_string()))?,
                    "--staging-partitions",
                )?;
            }
            "--transactions-partitions" => {
                partitions.transactions = parse_partition_count(
                    &take_value(args, &mut index)
                        .map_err(|error| MigrateError::Config(error.to_string()))?,
                    "--transactions-partitions",
                )?;
            }
            "--dlq-partitions" => {
                partitions.dlq = parse_partition_count(
                    &take_value(args, &mut index)
                        .map_err(|error| MigrateError::Config(error.to_string()))?,
                    "--dlq-partitions",
                )?;
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

    let names = TopicNames {
        staging: staging_topic,
        ..TopicNames::default()
    };
    let report = if fresh_topics {
        run_fresh_with_topics(
            backend,
            &database_url,
            &kafka_bootstrap,
            &names,
            partitions,
        )
        .await?
    } else if fresh {
        run_fresh(backend, &database_url, &kafka_bootstrap).await?
    } else {
        run_with_topics(
            backend,
            &database_url,
            &kafka_bootstrap,
            &names,
            partitions,
        )
        .await?
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

fn parse_partition_count(value: &str, option: &str) -> Result<i32, MigrateError> {
    let count = value.parse::<i32>().map_err(|_| {
        MigrateError::Config(format!("{option} must be a positive integer"))
    })?;
    if count < 1 {
        return Err(MigrateError::Config(format!(
            "{option} must be at least 1"
        )));
    }
    Ok(count)
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

fn manifest_file_hash(path: &str) -> String {
    match std::fs::read(path) {
        Ok(bytes) => {
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            hex::encode(hasher.finalize())
        }
        Err(_) => "0".repeat(64),
    }
}

async fn connect_ledger_repository(
    backend: Backend,
    database_url: &str,
) -> Result<Arc<dyn LedgerRepository>, CommandError> {
    Ok(match backend {
        Backend::Postgres => Arc::new(PostgresRepository::connect(database_url).await?),
        Backend::MariaDb => Arc::new(MariaDbRepository::connect(database_url).await?),
    })
}

async fn run_ingest(args: &[String]) -> Result<(), CommandError> {
    let mut manifest_path: Option<String> = None;
    let mut job_id: Option<String> = None;
    let mut backend = default_backend();
    let mut database_url = env::var("DATABASE_URL").ok();
    let mut kafka_bootstrap = env::var("KAFKA_BOOTSTRAP").ok();
    let mut staging_topic = "staging".to_string();
    let mut max_concurrent_files = 8_usize;
    let mut crash_after_rows: Option<usize> = None;
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
            "--crash-after-rows" => {
                let value = take_value(args, &mut index)?;
                crash_after_rows = Some(value.parse::<usize>().map_err(|_| {
                    CommandError::Config(
                        "--crash-after-rows must be a non-negative integer".to_string(),
                    )
                })?);
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

    let manifest = match manifest::load(Path::new(&manifest_path)) {
        Ok(manifest) => manifest,
        Err(error) => {
            let raw_hash = manifest_file_hash(&manifest_path);
            let failed_job = job_id.unwrap_or_else(|| {
                format!("job-{}", &raw_hash[..12])
            });
            let repository =
                connect_ledger_repository(backend, &database_url).await?;
            repository.create_job(&failed_job, &raw_hash).await?;
            let state = if manifest_exit_code(&error) == 3 {
                JobState::Failed
            } else {
                JobState::Rejected
            };
            repository
                .finish_job(&failed_job, state, Some(&error.to_string()))
                .await?;
            return Err(CommandError::Manifest(error));
        }
    };
    let job_id = job_id.unwrap_or_else(|| {
        format!("job-{}", &manifest.manifest_hash[..12])
    });

    let repository = connect_ledger_repository(backend, &database_url).await?;

    repository
        .submit_job(&job_id, &manifest.manifest_hash)
        .await?;

    let inputs: Vec<JobFileInput> = manifest
        .files
        .iter()
        .map(|file| JobFileInput {
            source_id: file.source_id.clone(),
            file_ord: file.ordinal,
            file_hash: file.sha256.clone(),
            location: file.path.to_string_lossy().into_owned(),
        })
        .collect();
    repository.create_job_files(&job_id, &inputs).await?;

    let mut sink =
        KafkaStagingSink::connect(&kafka_bootstrap, &staging_topic, Arc::clone(&repository))
            .await?;
    if let Some(limit) = crash_after_rows {
        sink = sink.with_stage_limit(limit);
    }
    let sink = Arc::new(sink);
    let summary = match ingest(
        &manifest,
        &job_id,
        sink,
        IngestConfig {
            max_concurrent_files,
        },
    )
    .await
    {
        Ok(summary) => summary,
        Err(error) => {
            repository
                .finish_job(&job_id, JobState::Failed, Some(&error.to_string()))
                .await?;
            return Err(CommandError::Ingest(error));
        }
    };

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
            event_id: event.event_id,
            file_ord: event.file_ord,
            row_ord: event.row_ord,
            client_id: event.client_id,
            partition: event.partition,
        });
    }
}

fn pause_list(
    topic: &str,
    last_offsets: &HashMap<i32, i64>,
) -> Result<TopicPartitionList, CommandError> {
    let mut offsets = TopicPartitionList::new();
    for partition in last_offsets.keys() {
        offsets
            .add_partition_offset(topic, *partition, Offset::Offset(0))
            .map_err(|error| {
                CommandError::Config(format!("build pause list: {error}"))
            })?;
    }
    Ok(offsets)
}

fn commit_ready(
    consumer: &StreamConsumer,
    topic: &str,
    engine: &Partitioner<KafkaTransactionSink>,
    last_offsets: &HashMap<i32, i64>,
    committed: &mut HashMap<i32, i64>,
    barred: &HashSet<i32>,
) -> Result<(), CommandError> {
    let mut offsets = TopicPartitionList::new();
    let mut any = false;

    for (partition, offset) in last_offsets {
        if barred.contains(partition) || engine.has_buffered_for(*partition) {
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
            if !barred.contains(partition) && !engine.has_buffered_for(*partition) {
                committed.insert(*partition, offset + 1);
            }
        }
    }
    Ok(())
}

async fn run_partition(args: &[String]) -> Result<(), CommandError> {
    let mut backend = default_backend();
    let mut database_url = env::var("DATABASE_URL").ok();
    let mut kafka_bootstrap = env::var("KAFKA_BOOTSTRAP").ok();
    let mut staging_topic = "staging".to_string();
    let mut transactions_topic = "transactions".to_string();
    let mut group: Option<String> = None;
    let mut job_filter: Option<String> = None;
    let mut max_buffered_rows = 100_000_usize;
    let mut expect: Option<u64> = None;
    let mut stall_timeout = Duration::from_secs(5);
    let mut idle_exit: Option<Duration> = None;
    let mut startup_delay: Option<Duration> = None;
    let mut explicit_partitions: Vec<i32> = Vec::new();
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
            "--stall-timeout" => {
                let value = take_value(args, &mut index)?;
                let seconds = value.parse::<u64>().map_err(|_| {
                    CommandError::Config(
                        "--stall-timeout must be a non-negative integer".to_string(),
                    )
                })?;
                stall_timeout = Duration::from_secs(seconds);
            }
            "--idle-exit" => {
                let value = take_value(args, &mut index)?;
                let seconds = value.parse::<u64>().map_err(|_| {
                    CommandError::Config(
                        "--idle-exit must be a non-negative integer".to_string(),
                    )
                })?;
                idle_exit = Some(Duration::from_secs(seconds));
            }
            "--startup-delay" => {
                let value = take_value(args, &mut index)?;
                let millis = value.parse::<u64>().map_err(|_| {
                    CommandError::Config(
                        "--startup-delay must be a non-negative integer".to_string(),
                    )
                })?;
                startup_delay = Some(Duration::from_millis(millis));
            }
            "--partition" => {
                let value = take_value(args, &mut index)?;
                explicit_partitions.push(value.parse::<i32>().map_err(|_| {
                    CommandError::Config(
                        "--partition must be a partition id".to_string(),
                    )
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

    if max_buffered_rows == 0 {
        return Err(CommandError::Config(
            "--max-buffered-rows must be at least 1".to_string(),
        ));
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
        .set("socket.timeout.ms", "10000")
        .set("fetch.wait.max.ms", "500")
        .create()
        .map_err(|error| {
            CommandError::Config(format!("create Kafka consumer: {error}"))
        })?;
    if drain {
        // Test/teardown mode: assign every staging partition directly so the
        // finite run does not wait on consumer-group rebalancing.
        let metadata = consumer
            .fetch_metadata(Some(&staging_topic), Duration::from_secs(10))
            .map_err(|error| {
                CommandError::Config(format!(
                    "fetch staging metadata: {error}"
                ))
            })?;
        let mut offsets = TopicPartitionList::new();
        let mut any = false;
        for topic in metadata.topics() {
            if topic.name() != staging_topic {
                continue;
            }
            for partition in topic.partitions() {
                if !explicit_partitions.is_empty()
                    && !explicit_partitions.contains(&partition.id())
                {
                    continue;
                }
                offsets
                    .add_partition_offset(
                        &staging_topic,
                        partition.id(),
                        Offset::Beginning,
                    )
                    .map_err(|error| {
                        CommandError::Config(format!(
                            "build assignment: {error}"
                        ))
                    })?;
                any = true;
            }
        }
        if !any {
            return Err(CommandError::Config(format!(
                "staging topic '{staging_topic}' has no partitions"
            )));
        }
        consumer.assign(&offsets).map_err(|error| {
            CommandError::Config(format!("assign staging partitions: {error}"))
        })?;
    } else {
        consumer
            .subscribe(&[staging_topic.as_str()])
            .map_err(|error| {
                CommandError::Config(format!(
                    "subscribe to staging topic: {error}"
                ))
            })?;
    }

    if let Some(delay) = startup_delay {
        // Let every replica join the consumer group before processing, so a
        // two-instance test exercises real partition ownership.
        tokio::time::sleep(delay).await;
    }

    let expected = expect.unwrap_or(u64::MAX);
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    let mut last_offsets: HashMap<i32, i64> = HashMap::new();
    let mut committed: HashMap<i32, i64> = HashMap::new();
    let mut barred: HashSet<i32> = HashSet::new();
    let mut records_seen = 0_u64;
    let mut consumed_partitions: HashSet<i32> = HashSet::new();
    let mut report = Vec::new();
    let mut last_message = Instant::now();
    let mut last_progress = Instant::now();
    let mut backpressure_applied = false;
    let mut stalled = false;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received shutdown signal; draining in-flight work");
                break;
            }
            _ = interval.tick() => {
                let emitted = engine.tick().await?;
                if !emitted.is_empty() {
                    last_progress = Instant::now();
                    append_emitted(&mut report, &job_filter, emitted);
                }
                if !drain {
                    commit_ready(
                        &consumer,
                        &staging_topic,
                        &engine,
                        &last_offsets,
                        &mut committed,
                        &barred,
                    )?;
                }

                if drain && records_seen >= expected && engine.buffered_rows() == 0 {
                    break;
                }
                if drain
                    && engine.buffered_rows() == 0
                    && last_message.elapsed() >= Duration::from_secs(15)
                {
                    break;
                }
                if drain
                    && engine.buffered_rows() > 0
                    && last_progress.elapsed() >= stall_timeout
                {
                    stalled = true;
                    break;
                }
                if let Some(idle) = idle_exit {
                    if engine.buffered_rows() == 0 && last_message.elapsed() >= idle {
                        break;
                    }
                }
            }
            message = consumer.recv() => {
                match message {
                    Ok(message) => {
                        last_message = Instant::now();
                        let partition = message.partition();
                        let offset = message.offset();
                        consumed_partitions.insert(partition);
                        last_offsets.insert(partition, offset);

                        let payload = message.payload().unwrap_or_default();
                        let staging: StagingRecord = serde_json::from_slice(payload)?;

                        if let Some(job_id) = &job_filter {
                            if &staging.job_id != job_id {
                                if !drain {
                                    if !drain {
                                        if !drain {
                                            commit_ready(
                                                &consumer,
                                                &staging_topic,
                                                &engine,
                                                &last_offsets,
                                                &mut committed,
                                                &barred,
                                            )?;
                                        }
                                    }
                                }
                                continue;
                            }
                        }

                        records_seen += 1;
                        let position = StagingPosition { partition, offset };
                        let mut give_up = false;
                        loop {
                            match engine.push(&staging, position).await {
                                Ok(emitted) => {
                                    if !emitted.is_empty() {
                                        last_progress = Instant::now();
                                        append_emitted(&mut report, &job_filter, emitted);
                                    }
                                    if !drain {
                                        if !drain {
                                            commit_ready(
                                                &consumer,
                                                &staging_topic,
                                                &engine,
                                                &last_offsets,
                                                &mut committed,
                                                &barred,
                                            )?;
                                        }
                                    }
                                    break;
                                }
                                Err(PartitionError::BufferFull(_)) => {
                                    backpressure_applied = true;
                                    let paused = pause_list(&staging_topic, &last_offsets)?;
                                    consumer.pause(&paused).map_err(|error| {
                                        CommandError::Config(format!(
                                            "pause staging consumer: {error}"
                                        ))
                                    })?;

                                    let mut stalled_here = false;
                                    while engine.buffered_rows() >= max_buffered_rows {
                                        let emitted = engine.tick().await?;
                                        if !emitted.is_empty() {
                                            last_progress = Instant::now();
                                            append_emitted(&mut report, &job_filter, emitted);
                                        }
                                        if !drain {
                                            commit_ready(
                                                &consumer,
                                                &staging_topic,
                                                &engine,
                                                &last_offsets,
                                                &mut committed,
                                                &barred,
                                            )?;
                                        }
                                        if engine.buffered_rows() < max_buffered_rows {
                                            break;
                                        }
                                        if drain
                                            && last_progress.elapsed() >= stall_timeout
                                        {
                                            stalled_here = true;
                                            break;
                                        }
                                        tokio::time::sleep(Duration::from_millis(100)).await;
                                    }

                                    consumer.resume(&paused).map_err(|error| {
                                        CommandError::Config(format!(
                                            "resume staging consumer: {error}"
                                        ))
                                    })?;

                                    if stalled_here {
                                        stalled = true;
                                        barred.insert(partition);
                                        give_up = true;
                                        break;
                                    }
                                }
                                Err(error) => return Err(CommandError::Partition(error)),
                            }
                        }

                        if give_up {
                            break;
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

    let mut staging_partitions: Vec<i32> = consumed_partitions.into_iter().collect();
    staging_partitions.sort_unstable();
    let report = PartitionReport {
        job_id: job_filter,
        staging_partitions,
        records_seen,
        buffered: engine.buffered_rows(),
        backpressure_applied,
        stalled,
        stalled_watermarks: engine.stalled_watermarks(),
        emitted: report,
    };

    if json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        println!(
            "job={:?} records_seen={} buffered={} backpressure={} stalled={} emitted={}",
            report.job_id,
            report.records_seen,
            report.buffered,
            report.backpressure_applied,
            report.stalled,
            report.emitted.len()
        );
    }
    Ok(())
}

async fn run_worker(args: &[String]) -> Result<(), CommandError> {
    let mut backend = default_backend();
    let mut database_url = env::var("DATABASE_URL").ok();
    let mut kafka_bootstrap = env::var("KAFKA_BOOTSTRAP").ok();
    let mut transactions_topic = "transactions".to_string();
    let mut dlq_topic = "dlq".to_string();
    let mut group = "distributed-worker".to_string();
    let mut job_filter: Option<String> = None;
    let mut expect: Option<u64> = None;
    let mut drain = false;
    let mut redeliver = false;
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
            "--transactions-topic" => {
                transactions_topic = take_value(args, &mut index)?;
            }
            "--dlq-topic" => dlq_topic = take_value(args, &mut index)?,
            "--group" => group = take_value(args, &mut index)?,
            "--job" => job_filter = Some(take_value(args, &mut index)?),
            "--expect" => {
                let value = take_value(args, &mut index)?;
                expect = Some(value.parse::<u64>().map_err(|_| {
                    CommandError::Config(
                        "--expect must be a non-negative integer".to_string(),
                    )
                })?);
            }
            "--drain" => {
                drain = true;
                index += 1;
            }
            "--redeliver" => {
                redeliver = true;
                index += 1;
            }
            "--json" => {
                json = true;
                index += 1;
            }
            other => {
                return Err(CommandError::Config(format!(
                    "unknown worker option '{other}'"
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

    let repository = connect_ledger_repository(backend, &database_url).await?;
    let dlq = KafkaDlqSink::connect(&kafka_bootstrap, &dlq_topic)
        .await
        .map_err(|error| CommandError::Config(error.to_string()))?;
    let consumer: StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &kafka_bootstrap)
        .set("group.id", &group)
        .set("client.id", "distributed-worker")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("socket.timeout.ms", "10000")
        .set("fetch.wait.max.ms", "500")
        .create()
        .map_err(|error| {
            CommandError::Config(format!("create Kafka consumer: {error}"))
        })?;

    if drain {
        let metadata = consumer
            .fetch_metadata(Some(&transactions_topic), Duration::from_secs(10))
            .map_err(|error| {
                CommandError::Config(format!(
                    "fetch transactions metadata: {error}"
                ))
            })?;
        let mut offsets = TopicPartitionList::new();
        let mut any = false;
        for topic in metadata.topics() {
            if topic.name() != transactions_topic {
                continue;
            }
            for partition in topic.partitions() {
                offsets
                    .add_partition_offset(
                        &transactions_topic,
                        partition.id(),
                        Offset::Beginning,
                    )
                    .map_err(|error| {
                        CommandError::Config(format!(
                            "build worker assignment: {error}"
                        ))
                    })?;
                any = true;
            }
        }
        if !any {
            return Err(CommandError::Config(format!(
                "transactions topic '{transactions_topic}' has no partitions"
            )));
        }
        consumer.assign(&offsets).map_err(|error| {
            CommandError::Config(format!(
                "assign transactions partitions: {error}"
            ))
        })?;
    } else {
        // Readiness: fail fast with a clear error when the broker or topic is
        // unreachable instead of joining a group that never receives data.
        let metadata = consumer
            .fetch_metadata(Some(&transactions_topic), Duration::from_secs(10))
            .map_err(|error| {
                CommandError::Config(format!(
                    "fetch transactions metadata: {error}"
                ))
            })?;
        let ready = metadata
            .topics()
            .iter()
            .find(|topic| topic.name() == transactions_topic)
            .map(|topic| {
                !topic.partitions().is_empty()
                    && topic
                        .partitions()
                        .iter()
                        .all(|partition| partition.leader() >= 0)
            })
            .unwrap_or(false);
        if !ready {
            return Err(CommandError::Config(format!(
                "transactions topic '{transactions_topic}' is not ready"
            )));
        }

        consumer
            .subscribe(&[transactions_topic.as_str()])
            .map_err(|error| {
                CommandError::Config(format!(
                    "subscribe to transactions topic: {error}"
                ))
            })?;
    }

    let expected = expect.unwrap_or(u64::MAX);
    let retries_before = distributed::repository::transient_retry_count();
    let mut records_seen = 0_u64;
    let mut processed = 0_u64;
    let mut duplicates = 0_u64;
    let mut rejected = 0_u64;
    let mut locked_accounts = 0_u64;
    let mut dlq_sends = 0_u64;
    let mut last_message = Instant::now();
    let mut interval = tokio::time::interval(Duration::from_millis(250));

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received shutdown signal; draining in-flight work");
                break;
            }
            _ = interval.tick() => {
                if drain && records_seen >= expected {
                    break;
                }
                if drain && last_message.elapsed() >= Duration::from_secs(15) {
                    break;
                }
            }
            message = consumer.recv() => {
                match message {
                    Ok(message) => {
                        last_message = Instant::now();
                        let payload = message.payload().unwrap_or_default();
                        let record: TransactionRecord = serde_json::from_slice(payload)?;

                        if let Some(job_id) = &job_filter {
                            if &record.job_id != job_id {
                                continue;
                            }
                        }

                        records_seen += 1;
                        let file_ord = i32::try_from(record.file_ord).map_err(|_| {
                            CommandError::Config(
                                "file ordinal exceeds i32".to_string(),
                            )
                        })?;
                        let row_ord = i32::try_from(record.row_ord).map_err(|_| {
                            CommandError::Config(
                                "row ordinal exceeds i32".to_string(),
                            )
                        })?;
                        let ledger_event = LedgerEvent {
                            event_id: record.event_id.clone(),
                            job_id: record.job_id.clone(),
                            source_id: record.source_id.clone(),
                            file_hash: record.file_hash.clone(),
                            file_ord,
                            row_ord,
                            partition: message.partition(),
                            transaction: LedgerTransaction {
                                transaction_type: record.kind.into(),
                                client: record.client_id,
                                transaction_id: record.transaction_id,
                                amount: record.amount_scaled.map(Amount::from_scaled),
                            },
                        };

                        let outcome = match repository.apply_event(&ledger_event).await {
                            Ok(outcome) => outcome,
                            Err(RepoError::RetriesExhausted {
                                attempts,
                                last,
                            }) => {
                                let reason = format!(
                                    "event {} exhausted {attempts} attempts: {last}",
                                    record.event_id
                                );
                                let dlq_record = DlqRecord {
                                    event_id: record.event_id.clone(),
                                    job_id: record.job_id.clone(),
                                    source_id: record.source_id.clone(),
                                    file_hash: record.file_hash.clone(),
                                    file_ord,
                                    row_ord,
                                    attempts,
                                    error: last,
                                };
                                dlq.send(&dlq_record).await.map_err(|error| {
                                    CommandError::Config(error.to_string())
                                })?;
                                dlq_sends += 1;
                                repository
                                    .finish_job(
                                        &record.job_id,
                                        JobState::Failed,
                                        Some(&reason),
                                    )
                                    .await?;
                                if !drain {
                                    consumer
                                        .commit_message(&message, CommitMode::Sync)
                                        .map_err(|error| {
                                            CommandError::Config(format!(
                                                "commit dead-letter offset: {error}"
                                            ))
                                        })?;
                                }
                                if json {
                                    let report = serde_json::json!({
                                        "job_id": record.job_id,
                                        "records_seen": records_seen,
                                        "processed": processed,
                                        "duplicates": duplicates,
                                        "rejected": rejected,
                                        "locked_accounts": locked_accounts,
                                        "retries": distributed::repository::transient_retry_count()
                                            .saturating_sub(retries_before),
                                        "dlq_sends": dlq_sends,
                                        "failed": true,
                                        "failure_reason": reason,
                                    });
                                    println!("{report}");
                                }
                                return Err(CommandError::JobFailed(reason));
                            }
                            Err(error) => return Err(CommandError::Repo(error)),
                        };
                        if outcome.duplicate {
                            duplicates += 1;
                        }
                        if let Some(error) = &outcome.row_error {
                            rejected += 1;
                            match error {
                                ProcessingError::DisputeExceedsAvailable { .. } => {
                                    locked_accounts += 1;
                                    tracing::warn!(
                                        client = outcome.client_id,
                                        transaction_id = record.transaction_id,
                                        error = %error,
                                        "client {} dispute exceeds available; account locked",
                                        outcome.client_id
                                    );
                                }
                                _ => {
                                    tracing::trace!(
                                        error = %error,
                                        "skipped distributed transaction row"
                                    );
                                }
                            }
                        }

                        if !drain {
                            consumer
                                .commit_message(&message, CommitMode::Sync)
                                .map_err(|error| {
                                    CommandError::Config(format!(
                                        "commit worker offset: {error}"
                                    ))
                                })?;
                        }
                        processed += 1;

                        if drain && records_seen >= expected {
                            break;
                        }
                    }
                    Err(KafkaError::PartitionEOF(_)) => {}
                    Err(error) => {
                        return Err(CommandError::Config(format!(
                            "consume transactions topic: {error}"
                        )));
                    }
                }
            }
        }
    }

    let report = serde_json::json!({
        "job_id": job_filter,
        "records_seen": records_seen,
        "processed": processed,
        "duplicates": duplicates,
        "rejected": rejected,
        "locked_accounts": locked_accounts,
        "retries": distributed::repository::transient_retry_count()
            .saturating_sub(retries_before),
        "dlq_sends": dlq_sends,
        "offset_committed_after_db_commit": true,
        "redeliver": redeliver,
    });
    if json {
        println!("{report}");
    } else {
        println!(
            "job={:?} records_seen={} processed={} duplicates={} rejected={} locked={} retries={}",
            report["job_id"],
            records_seen,
            processed,
            duplicates,
            rejected,
            locked_accounts,
            report["retries"]
        );
    }
    Ok(())
}

async fn run_journal(args: &[String]) -> Result<(), CommandError> {
    let mut backend = default_backend();
    let mut database_url = env::var("DATABASE_URL").ok();
    let mut tx_id: Option<u32> = None;
    let mut json = false;

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--backend" => {
                let value = take_value(args, &mut index)?;
                backend = parse_backend(&value)?;
            }
            "--database-url" => database_url = Some(take_value(args, &mut index)?),
            "--tx" => {
                let value = take_value(args, &mut index)?;
                tx_id = Some(value.parse::<u32>().map_err(|_| {
                    CommandError::Config(
                        "--tx must be a non-negative integer".to_string(),
                    )
                })?);
            }
            "--json" => {
                json = true;
                index += 1;
            }
            other => {
                return Err(CommandError::Config(format!(
                    "unknown journal option '{other}'"
                )));
            }
        }
    }

    let database_url = database_url.ok_or_else(|| {
        CommandError::Config("DATABASE_URL or --database-url is required".to_string())
    })?;
    let tx_id = tx_id.ok_or_else(|| {
        CommandError::Config("--tx is required".to_string())
    })?;
    let repository = connect_ledger_repository(backend, &database_url).await?;
    let report = match repository.transaction(tx_id).await? {
        Some(row) => serde_json::json!({
            "tx_id": row.tx_id,
            "client_id": row.client_id,
            "amount": row.amount.to_string(),
            "state": row.state_str(),
        }),
        None => serde_json::json!({
            "tx_id": tx_id,
            "state": "unknown",
        }),
    };
    if json {
        println!("{report}");
    } else {
        println!("{report}");
    }
    Ok(())
}

async fn run_dlq(args: &[String]) -> Result<(), CommandError> {
    let mut kafka_bootstrap = env::var("KAFKA_BOOTSTRAP").ok();
    let mut dlq_topic = "dlq".to_string();
    let mut job_filter: Option<String> = None;
    let mut json = false;

    let mut index = 0;
    if args.first().map(String::as_str) != Some("list") {
        return Err(CommandError::Config(
            "dlq requires the 'list' subcommand".to_string(),
        ));
    }
    index += 1;

    while index < args.len() {
        match args[index].as_str() {
            "--kafka-bootstrap" => kafka_bootstrap = Some(take_value(args, &mut index)?),
            "--dlq-topic" => dlq_topic = take_value(args, &mut index)?,
            "--job" => job_filter = Some(take_value(args, &mut index)?),
            "--json" => {
                json = true;
                index += 1;
            }
            other => {
                return Err(CommandError::Config(format!(
                    "unknown dlq option '{other}'"
                )));
            }
        }
    }

    let kafka_bootstrap = kafka_bootstrap.ok_or_else(|| {
        CommandError::Config("KAFKA_BOOTSTRAP or --kafka-bootstrap is required".to_string())
    })?;
    let records = distributed::kafka_dlq::list(
        &kafka_bootstrap,
        &dlq_topic,
        job_filter.as_deref(),
    )
    .await
    .map_err(|error| CommandError::Config(error.to_string()))?;

    if json {
        println!("{}", serde_json::to_string(&records)?);
    } else {
        println!("{} dead-letter record(s)", records.len());
        for record in records {
            println!(
                "job={} file={} row={} error={}",
                record.job_id, record.file_ord, record.row_ord, record.error
            );
        }
    }
    Ok(())
}

async fn run_job_command(args: &[String]) -> Result<(), CommandError> {
    if args.first().map(String::as_str) != Some("status") {
        return Err(CommandError::Config(
            "job requires the 'status' subcommand".to_string(),
        ));
    }
    let mut index = 1;
    let mut backend = default_backend();
    let mut database_url = env::var("DATABASE_URL").ok();
    let mut job_id: Option<String> = None;
    let mut json = false;

    while index < args.len() {
        match args[index].as_str() {
            "--backend" => {
                let value = take_value(args, &mut index)?;
                backend = parse_backend(&value)?;
            }
            "--database-url" => database_url = Some(take_value(args, &mut index)?),
            "--job" => job_id = Some(take_value(args, &mut index)?),
            "--json" => {
                json = true;
                index += 1;
            }
            other => {
                return Err(CommandError::Config(format!(
                    "unknown job option '{other}'"
                )));
            }
        }
    }

    let database_url = database_url.ok_or_else(|| {
        CommandError::Config("DATABASE_URL or --database-url is required".to_string())
    })?;
    let job_id = job_id.ok_or_else(|| {
        CommandError::Config("--job is required".to_string())
    })?;
    let repository = connect_ledger_repository(backend, &database_url).await?;
    let status = repository.job_status(&job_id).await?;
    let progress: Vec<serde_json::Value> = status
        .progress
        .iter()
        .map(|progress| {
            serde_json::json!({
                "partition": progress.partition,
                "emitted": progress.emitted,
                "processed": progress.processed,
            })
        })
        .collect();
    let report = serde_json::json!({
        "job_id": status.job_id,
        "state": status.state.as_str(),
        "failure_reason": status.failure_reason,
        "partitioner_complete": status.partitioner_complete,
        "ready": status.is_complete(),
        "progress": progress,
    });
    if json {
        println!("{report}");
    } else {
        println!(
            "job={} state={} ready={} partitioner_complete={}",
            report["job_id"], report["state"], report["ready"], report["partitioner_complete"]
        );
    }
    Ok(())
}

async fn run_export(args: &[String]) -> Result<(), CommandError> {
    let mut backend = default_backend();
    let mut database_url = env::var("DATABASE_URL").ok();
    let mut job_id: Option<String> = None;
    let mut out_path: Option<String> = None;
    let mut check = false;
    let mut json = false;
    let mut timeout = Duration::from_secs(120);

    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--backend" => {
                let value = take_value(args, &mut index)?;
                backend = parse_backend(&value)?;
            }
            "--database-url" => database_url = Some(take_value(args, &mut index)?),
            "--job" => job_id = Some(take_value(args, &mut index)?),
            "--out" => out_path = Some(take_value(args, &mut index)?),
            "--timeout" => {
                let value = take_value(args, &mut index)?;
                let seconds = value.parse::<u64>().map_err(|_| {
                    CommandError::Config(
                        "--timeout must be a non-negative integer".to_string(),
                    )
                })?;
                timeout = Duration::from_secs(seconds);
            }
            "--check" => {
                check = true;
                index += 1;
            }
            "--json" => {
                json = true;
                index += 1;
            }
            other => {
                return Err(CommandError::Config(format!(
                    "unknown export option '{other}'"
                )));
            }
        }
    }

    let database_url = database_url.ok_or_else(|| {
        CommandError::Config("DATABASE_URL or --database-url is required".to_string())
    })?;
    let job_id = job_id.ok_or_else(|| {
        CommandError::Config("--job is required".to_string())
    })?;
    let out_path = out_path.ok_or_else(|| {
        CommandError::Config("--out is required".to_string())
    })?;

    let repository = connect_ledger_repository(backend, &database_url).await?;
    let deadline = Instant::now() + timeout;

    loop {
        let status = repository.job_status(&job_id).await?;
        match status.state {
            JobState::Failed => {
                let reason = status
                    .failure_reason
                    .clone()
                    .unwrap_or_else(|| "job failed".to_string());
                if json {
                    println!(
                        "{}",
                        serde_json::json!({
                            "job_id": status.job_id,
                            "state": status.state.as_str(),
                            "ready": false,
                            "failure_reason": status.failure_reason,
                        })
                    );
                }
                return Err(CommandError::JobFailed(reason));
            }
            JobState::Rejected => {
                let reason = status
                    .failure_reason
                    .clone()
                    .unwrap_or_else(|| "job rejected".to_string());
                if json {
                    println!(
                        "{}",
                        serde_json::json!({
                            "job_id": status.job_id,
                            "state": status.state.as_str(),
                            "ready": false,
                            "failure_reason": status.failure_reason,
                        })
                    );
                }
                return Err(CommandError::JobRejected(reason));
            }
            JobState::Running | JobState::Completed => {}
        }

        let ready = status.is_complete();
        if check {
            let report = serde_json::json!({
                "job_id": status.job_id,
                "state": status.state.as_str(),
                "ready": ready,
                "partitioner_complete": status.partitioner_complete,
                "progress": status.progress.iter().map(|progress| serde_json::json!({
                    "partition": progress.partition,
                    "emitted": progress.emitted,
                    "processed": progress.processed,
                })).collect::<Vec<_>>(),
            });
            if json {
                println!("{report}");
            } else {
                println!(
                    "job={} state={} ready={ready}",
                    report["job_id"], report["state"]
                );
            }
            return Ok(());
        }

        if ready {
            if status.state == JobState::Running {
                repository
                    .finish_job(&job_id, JobState::Completed, None)
                    .await?;
            }
            let accounts = repository.accounts_for_job(&job_id).await?;
            let temporary = format!("{out_path}.tmp");
            {
                let file = std::fs::File::create(&temporary)?;
                let mut writer = std::io::BufWriter::new(file);
                distributed::job_export::write_accounts(&mut writer, &accounts)?;
                std::io::Write::flush(&mut writer)?;
            }
            std::fs::rename(&temporary, &out_path)?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "job_id": job_id,
                        "state": JobState::Completed.as_str(),
                        "accounts": accounts.len(),
                        "out": out_path,
                    })
                );
            }
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(CommandError::Config(format!(
                "job {job_id} did not drain before the export timeout"
            )));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn run_identity(args: &[String]) -> Result<(), CommandError> {
    if args.first().map(String::as_str) != Some("mint") {
        return Err(CommandError::Config(
            "identity requires the 'mint' subcommand".to_string(),
        ));
    }
    let mut index = 1;
    let mut backend = default_backend();
    let mut database_url = env::var("DATABASE_URL").ok();
    let mut source_id: Option<String> = None;
    let mut external_client: Option<u16> = None;
    let mut json = false;

    while index < args.len() {
        match args[index].as_str() {
            "--backend" => {
                let value = take_value(args, &mut index)?;
                backend = parse_backend(&value)?;
            }
            "--database-url" => database_url = Some(take_value(args, &mut index)?),
            "--source" => source_id = Some(take_value(args, &mut index)?),
            "--client" => {
                let value = take_value(args, &mut index)?;
                external_client = Some(value.parse::<u16>().map_err(|_| {
                    CommandError::Config(
                        "--client must be between 0 and 65535".to_string(),
                    )
                })?);
            }
            "--json" => {
                json = true;
                index += 1;
            }
            other => {
                return Err(CommandError::Config(format!(
                    "unknown identity option '{other}'"
                )));
            }
        }
    }

    let database_url = database_url.ok_or_else(|| {
        CommandError::Config("DATABASE_URL or --database-url is required".to_string())
    })?;
    let source_id = source_id.ok_or_else(|| {
        CommandError::Config("--source is required".to_string())
    })?;
    let external_client = external_client.ok_or_else(|| {
        CommandError::Config("--client is required".to_string())
    })?;

    let repository = connect_ledger_repository(backend, &database_url).await?;
    let client_id = repository
        .mint_or_get_client(&source_id, external_client)
        .await?;
    let report = serde_json::json!({
        "source_id": source_id,
        "external_client": external_client,
        "client_id": client_id,
    });
    if json {
        println!("{report}");
    } else {
        println!("client_id={client_id}");
    }
    Ok(())
}

async fn run_config(args: &[String]) -> Result<(), CommandError> {
    if args.first().map(String::as_str) != Some("show") {
        return Err(CommandError::Config(
            "config requires the 'show' subcommand".to_string(),
        ));
    }
    let config = distributed::config::Config::load()
        .map_err(CommandError::Config)?;
    println!("{}", serde_json::to_string(&config)?);
    Ok(())
}

async fn run_health(args: &[String]) -> Result<bool, CommandError> {
    let mut json = false;
    for argument in args {
        match argument.as_str() {
            "--json" => json = true,
            other => {
                return Err(CommandError::Config(format!(
                    "unknown health option '{other}'"
                )));
            }
        }
    }

    let config = distributed::config::Config::load()
        .map_err(CommandError::Config)?;
    let broker = config.require_broker().map_err(CommandError::Config)?;
    let database_url = config
        .require_database()
        .map_err(CommandError::Config)?;
    let backend = default_backend();

    let database_ready = connect_ledger_repository(backend, database_url)
        .await
        .is_ok();
    let broker_ready = rdkafka::consumer::BaseConsumer::from(
        rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("group.id", "distributed-health")
            .set("socket.timeout.ms", "5000")
            .create::<rdkafka::consumer::BaseConsumer>()
            .map_err(|error| {
                CommandError::Config(format!(
                    "broker configuration is invalid: {error}"
                ))
            })?,
    )
    .fetch_metadata(None, Duration::from_secs(5))
    .is_ok();

    let ready = database_ready && broker_ready;
    let report = serde_json::json!({
        "ready": ready,
        "database": if database_ready { "ok" } else { "unavailable" },
        "broker": if broker_ready { "ok" } else { "unavailable" },
        "backend": backend.as_str(),
    });
    if json {
        println!("{report}");
    } else {
        println!(
            "ready={} database={} broker={}",
            report["ready"], report["database"], report["broker"]
        );
    }
    Ok(ready)
}

async fn run_metrics(args: &[String]) -> Result<(), CommandError> {
    let mut json = false;
    for argument in args {
        match argument.as_str() {
            "--json" => json = true,
            other => {
                return Err(CommandError::Config(format!(
                    "unknown metrics option '{other}'"
                )));
            }
        }
    }

    let config = distributed::config::Config::load()
        .map_err(CommandError::Config)?;
    let database_url = config
        .require_database()
        .map_err(CommandError::Config)?;
    let backend = default_backend();
    let repository = connect_ledger_repository(backend, database_url).await?;
    let jobs = repository.job_metrics().await?;

    let mut stalled_watermarks = Vec::new();
    let mut job_states = serde_json::Map::new();
    let mut failed_jobs = 0_i64;
    let mut consumer_lag = 0_i64;
    for job in &jobs {
        job_states.insert(
            job.job_id.clone(),
            serde_json::Value::String(job.state.as_str().to_string()),
        );
        consumer_lag += job.pending_events;
        if job.state == JobState::Failed {
            failed_jobs += 1;
        }
        if job.state == JobState::Running
            && (!job.partitioner_complete || job.pending_events > 0)
        {
            stalled_watermarks.push(job.job_id.clone());
        }
    }

    let dlq_depth = match config.kafka_bootstrap.as_deref() {
        Some(bootstrap) => distributed::kafka_dlq::list(bootstrap, "dlq", None)
            .await
            .map(|records| records.len() as i64)
            .unwrap_or(0),
        None => 0,
    };

    let report = serde_json::json!({
        "consumer_lag": consumer_lag,
        "dlq_depth": dlq_depth,
        "failed_jobs": failed_jobs,
        "stalled_watermarks": stalled_watermarks,
        "job_states": job_states,
    });
    if json {
        println!("{report}");
    } else {
        println!(
            "consumer_lag={} dlq_depth={} failed_jobs={} stalled={:?}",
            report["consumer_lag"],
            report["dlq_depth"],
            report["failed_jobs"],
            report["stalled_watermarks"]
        );
    }
    Ok(())
}
