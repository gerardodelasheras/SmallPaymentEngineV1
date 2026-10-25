//! Entry points for the distributed ledger variant.
//!
//! The services are intentionally separate from the in-memory CLI crate. This
//! scaffold establishes the command surface; each command is implemented by a
//! later task in the change.

use std::env;
use std::process::ExitCode;

const USAGE: &str = "\
Usage: distributed <command> [options]

Commands:
  ingest      Parse a manifest and stage rows
  partition   Map staged clients and emit ordered transactions
  worker      Consume transactions and apply the durable ledger
  export      Wait for a job and write its accounts CSV
  migrate     Apply database migrations and provision topics
";

fn main() -> ExitCode {
    let Some(command) = env::args().nth(1) else {
        print!("{USAGE}");
        return ExitCode::SUCCESS;
    };

    match command.as_str() {
        "--help" | "-h" | "help" => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        "ingest" | "partition" | "worker" | "export" | "migrate" => {
            eprintln!("distributed: '{command}' is not implemented yet");
            ExitCode::from(1)
        }
        other => {
            eprintln!("distributed: unknown command '{other}'\n\n{USAGE}");
            ExitCode::from(2)
        }
    }
}
