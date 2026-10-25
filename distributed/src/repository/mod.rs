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

    async fn record_emitted(
        &self,
        job_id: &str,
        partition: i32,
        count: i64,
    ) -> Result<(), RepoError>;

    async fn job_progress(&self, job_id: &str) -> Result<Vec<JobProgress>, RepoError>;

    async fn job_clients(&self, job_id: &str) -> Result<Vec<CanonicalClientId>, RepoError>;

    async fn accounts_for_job(&self, job_id: &str) -> Result<Vec<AccountRow>, RepoError>;

    async fn transaction(&self, tx_id: TransactionId)
        -> Result<Option<TransactionRow>, RepoError>;
}
