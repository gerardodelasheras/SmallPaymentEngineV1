//! Entry points for the distributed ledger variant.
//!
//! The services are intentionally separate from the in-memory CLI crate. The
//! `migrate` command is implemented; the service commands are implemented by
//! later tasks in the change.

use std::env;
use std::process::ExitCode;

use distributed::migrate::{run, run_fresh, Backend, MigrateError};

const USAGE: &str = "\
Usage: distributed <command> [options]

Commands:
  ingest      Parse a manifest and stage rows
  partition   Map staged clients and emit ordered transactions
  worker      Consume transactions and apply the durable ledger
  export      Wait for a job and write its accounts CSV
  migrate     Apply database migrations and provision topics

migrate options:
  --backend <postgres|mariadb>   Backend (default: postgres)
  --database-url <url>           Default: DATABASE_URL
  --kafka-bootstrap <url>        Default: KAFKA_BOOTSTRAP
  --fresh                        Reset the schema first (test/teardown only)
  --json                         Print a JSON report
";

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
        "ingest" | "partition" | "worker" | "export" => {
            eprintln!("distributed: '{command}' is not implemented yet");
            ExitCode::from(1)
        }
        other => {
            eprintln!("distributed: unknown command '{other}'\n\n{USAGE}");
            ExitCode::from(2)
        }
    }
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
                let value = args.get(index + 1).ok_or_else(|| {
                    MigrateError::Config("--backend requires a value".to_string())
                })?;
                backend = value.parse()?;
                index += 2;
            }
            "--database-url" => {
                database_url = Some(
                    args.get(index + 1)
                        .ok_or_else(|| {
                            MigrateError::Config("--database-url requires a value".to_string())
                        })?
                        .clone(),
                );
                index += 2;
            }
            "--kafka-bootstrap" => {
                kafka_bootstrap = Some(
                    args.get(index + 1)
                        .ok_or_else(|| {
                            MigrateError::Config(
                                "--kafka-bootstrap requires a value".to_string(),
                            )
                        })?
                        .clone(),
                );
                index += 2;
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
        MigrateError::Config(
            "DATABASE_URL or --database-url is required".to_string(),
        )
    })?;
    let kafka_bootstrap = kafka_bootstrap.ok_or_else(|| {
        MigrateError::Config(
            "KAFKA_BOOTSTRAP or --kafka-bootstrap is required".to_string(),
        )
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
