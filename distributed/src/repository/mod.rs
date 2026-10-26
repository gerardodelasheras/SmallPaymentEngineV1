//! Storage-independent repository contract for the distributed ledger.
//!
//! The PostgreSQL and MariaDB adapters implement this contract. Each
//! `apply_event` call is one database transaction: dedupe, account lock, ledger
//! rules, journal update, job progress, and touched-client tracking commit
//! together.

pub mod mariadb;
pub mod postgres;

use async_trait::async_trait;
use transaction_processor::{
    Amount, CanonicalClientId, EntryState, Journal, LedgerEntry, LedgerTransaction,
    ProcessingError, TransactionId,
};

pub(crate) struct RowJournal {
    pub(crate) tx_id: TransactionId,
    pub(crate) entry: Option<LedgerEntry<CanonicalClientId>>,
}

/// A process-local block of canonical client IDs reserved from `id_allocator`.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct IdBlock {
    pub(crate) next: i64,
    pub(crate) end: i64,
}

impl IdBlock {
    pub(crate) fn take(&mut self) -> Option<i64> {
        if self.next < self.end {
            let id = self.next;
            self.next += 1;
            Some(id)
        } else {
            None
        }
    }
}

const MAX_TRANSIENT_ATTEMPTS: u32 = 5;

static TRANSIENT_RETRIES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Process-local count of transient retries performed by the repository.
///
/// Workers report the delta so operators can see contention (for example Galera
/// certification failures) without scraping logs.
pub fn transient_retry_count() -> u64 {
    TRANSIENT_RETRIES.load(std::sync::atomic::Ordering::Relaxed)
}

static FAULT_ATTEMPTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Test hook for deterministic conflict coverage.
///
/// `DISTRIBUTED_FAULT=conflict:N` fails the first N event applications with a
/// transient error; `conflict:always` fails every attempt. The fault is applied
/// inside the retry loop, so the normal bounded-retry and DLQ paths are
/// exercised exactly as in production.
pub(crate) fn maybe_inject_fault() -> Result<(), RepoError> {
    let Ok(spec) = std::env::var("DISTRIBUTED_FAULT") else {
        return Ok(());
    };
    let (kind, budget) = match spec.split_once(':') {
        Some((kind, budget)) => (kind, budget),
        None => (spec.as_str(), "always"),
    };
    if kind != "conflict" {
        return Ok(());
    }

    let attempt = FAULT_ATTEMPTS.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
    let should_fail = match budget {
        "always" => true,
        count => attempt <= count.parse::<u64>().unwrap_or(0),
    };
    if should_fail {
        Err(RepoError::Transient(
            "injected certification conflict".to_string(),
        ))
    } else {
        Ok(())
    }
}

fn is_transient(error: &sqlx::Error) -> bool {
    let Some(database) = error.as_database_error() else {
        return false;
    };
    let code = database.code().unwrap_or_default();
    if matches!(code.as_ref(), "40001" | "40P01" | "1213" | "1205") {
        return true;
    }
    let message = database.message().to_lowercase();
    message.contains("deadlock") || message.contains("certification")
}

fn retryable(error: &RepoError) -> bool {
    match error {
        RepoError::Database(error) => is_transient(error),
        RepoError::Transient(_) => true,
        _ => false,
    }
}

/// Retry a transient database operation with bounded exponential backoff.
///
/// The operation must be idempotent. `apply_event` dedupes on the ledger event,
/// and `mint_or_get_client` returns the existing mapping on a retry.
pub(crate) async fn retry_transient<F, Fut, T>(mut operation: F) -> Result<T, RepoError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, RepoError>>,
{
    for attempt in 1..=MAX_TRANSIENT_ATTEMPTS {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if retryable(&error) && attempt < MAX_TRANSIENT_ATTEMPTS => {
                TRANSIENT_RETRIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let base = 5_u64 * 2_u64.pow(attempt - 1);
                let jitter = u64::from(attempt) * 3 % 7;
                tokio::time::sleep(std::time::Duration::from_millis(base + jitter)).await;
            }
            Err(error) => {
                let last = error.to_string();
                return if retryable(&error) {
                    Err(RepoError::RetriesExhausted {
                        attempts: attempt,
                        last,
                    })
                } else {
                    Err(error)
                };
            }
        }
    }

    unreachable!("the retry loop always returns")
}

impl Journal<CanonicalClientId> for RowJournal {
    fn get(&self, id: TransactionId) -> Option<LedgerEntry<CanonicalClientId>> {
        if id == self.tx_id {
            self.entry
        } else {
            None
        }
    }
}

pub(crate) fn entry_state_from_str(state: &str) -> Result<EntryState, RepoError> {
    match state {
        "settled" => Ok(EntryState::Settled),
        "disputed" => Ok(EntryState::Disputed),
        "charged_back" => Ok(EntryState::ChargedBack),
        other => Err(RepoError::InvalidState(other.to_string())),
    }
}

pub(crate) fn entry_state_str(state: EntryState) -> &'static str {
    match state {
        EntryState::Settled => "settled",
        EntryState::Disputed => "disputed",
        EntryState::ChargedBack => "charged_back",
    }
}

/// A ledger event ready for atomic application.
#[derive(Debug, Clone)]
pub struct LedgerEvent {
    pub event_id: String,
    pub job_id: String,
    pub source_id: String,
    pub file_hash: String,
    pub file_ord: i32,
    pub row_ord: i32,
    pub partition: i32,
    pub transaction: LedgerTransaction<CanonicalClientId>,
}

/// The observable result of applying one event.
#[derive(Debug)]
pub struct ApplyOutcome {
    pub duplicate: bool,
    pub locked: bool,
    pub client_id: CanonicalClientId,
    pub row_error: Option<ProcessingError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountRow {
    pub client_id: CanonicalClientId,
    pub available: Amount,
    pub held: Amount,
    pub locked: bool,
}

impl AccountRow {
    pub fn total(&self) -> Amount {
        self.available
            .checked_add(self.held)
            .expect("account total overflow")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionRow {
    pub tx_id: TransactionId,
    pub client_id: CanonicalClientId,
    pub amount: Amount,
    pub state: EntryState,
}

impl TransactionRow {
    pub fn state_str(&self) -> &'static str {
        match self.state {
            EntryState::Settled => "settled",
            EntryState::Disputed => "disputed",
            EntryState::ChargedBack => "charged_back",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobFileInput {
    pub source_id: String,
    pub file_ord: u32,
    pub file_hash: String,
    pub location: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileCompletionRow {
    pub source_id: String,
    pub file_ord: u32,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmittedRow {
    pub event_id: String,
    pub file_ord: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobFileRow {
    pub file_ord: u32,
    pub source_id: String,
    pub state: String,
    pub row_count: u64,
    pub emitted_rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobProgress {
    pub partition: i32,
    pub emitted: i64,
    pub processed: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Running,
    Completed,
    Failed,
    Rejected,
}

impl JobState {
    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Running => "running",
            JobState::Completed => "completed",
            JobState::Failed => "failed",
            JobState::Rejected => "rejected",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RepoError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("inconsistent ledger state: {0}")]
    Inconsistent(String),

    #[error("invalid ledger state: {0}")]
    InvalidState(String),

    #[error("job progress is missing for job {job_id} partition {partition}")]
    MissingProgress { job_id: String, partition: i32 },

    #[error("transient database failure: {0}")]
    Transient(String),

    #[error("transient database failure after {attempts} attempts: {last}")]
    RetriesExhausted { attempts: u32, last: String },
}

#[async_trait]
pub trait LedgerRepository: Send + Sync {
    async fn create_job(&self, job_id: &str, manifest_hash: &str) -> Result<(), RepoError>;

    async fn finish_job(
        &self,
        job_id: &str,
        state: JobState,
        failure_reason: Option<&str>,
    ) -> Result<(), RepoError>;

    async fn mint_or_get_client(
        &self,
        source_id: &str,
        external_client: u16,
    ) -> Result<CanonicalClientId, RepoError>;

    async fn apply_event(&self, event: &LedgerEvent) -> Result<ApplyOutcome, RepoError>;

    async fn create_job_files(
        &self,
        job_id: &str,
        files: &[JobFileInput],
    ) -> Result<(), RepoError>;

    async fn mark_files_complete(
        &self,
        job_id: &str,
        files: &[FileCompletionRow],
    ) -> Result<u64, RepoError>;

    async fn job_file_complete(&self, job_id: &str, file_ord: u32) -> Result<bool, RepoError>;

    async fn record_emitted(
        &self,
        job_id: &str,
        partition: i32,
        rows: &[EmittedRow],
    ) -> Result<u64, RepoError>;

    async fn job_files(&self, job_id: &str) -> Result<Vec<JobFileRow>, RepoError>;

    async fn job_progress(&self, job_id: &str) -> Result<Vec<JobProgress>, RepoError>;

    async fn job_clients(&self, job_id: &str) -> Result<Vec<CanonicalClientId>, RepoError>;

    async fn accounts_for_job(&self, job_id: &str) -> Result<Vec<AccountRow>, RepoError>;

    async fn transaction(&self, tx_id: TransactionId)
        -> Result<Option<TransactionRow>, RepoError>;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    #[tokio::test]
    async fn transient_failures_are_retried_until_success() {
        let attempts = AtomicU32::new(0);

        let value = retry_transient(|| async {
            let current = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if current < 3 {
                Err(RepoError::Transient("conflict".to_string()))
            } else {
                Ok(current)
            }
        })
        .await
        .expect("retry succeeds");

        assert_eq!(value, 3);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn transient_failures_exhaust_the_budget() {
        let attempts = AtomicU32::new(0);

        let result: Result<(), RepoError> = retry_transient(|| async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(RepoError::Transient("conflict".to_string()))
        })
        .await;

        assert!(matches!(
            result,
            Err(RepoError::RetriesExhausted {
                attempts: 5,
                ..
            })
        ));
        assert_eq!(attempts.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn transient_retries_are_counted_for_metrics() {
        let before = transient_retry_count();
        let attempts = AtomicU32::new(0);

        let value = retry_transient(|| async {
            let current = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if current < 3 {
                Err(RepoError::Transient("conflict".to_string()))
            } else {
                Ok(current)
            }
        })
        .await
        .expect("retry succeeds");

        assert_eq!(value, 3);
        // The counter is process-global, so other tests may add to it too.
        assert!(transient_retry_count() >= before + 2);
    }
}
