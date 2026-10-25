use std::path::PathBuf;

use thiserror::Error;

use crate::amount::Amount;

/// Unified error type for row-level validation, ledger rules, and infrastructure.
#[derive(Debug, Error)]
pub enum ProcessingError {
    #[error("unknown transaction type: {0}")]
    InvalidType(String),

    #[error("invalid amount: {0}")]
    InvalidAmount(String),

    #[error("negative amount: {0}")]
    NegativeAmount(String),

    #[error("too many decimal places: {0}")]
    TooManyDecimals(String),

    #[error("missing amount")]
    MissingAmount,

    #[error("unexpected amount")]
    UnexpectedAmount,

    #[error("insufficient funds")]
    InsufficientFunds,

    #[error("dispute exceeds available: disputed {amount}, available {available}")]
    DisputeExceedsAvailable { amount: Amount, available: Amount },

    #[error("account is locked")]
    AccountLocked,

    #[error("unknown transaction: {0}")]
    UnknownTransaction(u32),

    #[error("transaction already disputed: {0}")]
    AlreadyDisputed(u32),

    #[error("transaction is not under dispute: {0}")]
    NotDisputed(u32),

    #[error("transaction is not owned by this client: {0}")]
    NotOwner(u32),

    #[error("duplicate transaction identifier: {0}")]
    DuplicateTransaction(u32),

    #[error("invalid CSV header: {0}")]
    InvalidHeader(String),

    #[error("amount overflow")]
    Overflow,

    #[error("amount underflow")]
    Underflow,

    #[error("worker unavailable: {0}")]
    WorkerUnavailable(String),

    #[error("csv error: {0}")]
    Csv(#[from] csv::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("cannot open {}: {source}", path.display())]
    FileOpen {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl ProcessingError {
    /// Whether a row-level failure may be skipped while the stream continues.
    pub fn is_recoverable(&self) -> bool {
        match self {
            ProcessingError::Io(_)
            | ProcessingError::FileOpen { .. }
            | ProcessingError::WorkerUnavailable(_) => false,
            ProcessingError::Csv(error) => !matches!(error.kind(), csv::ErrorKind::Io(_)),
            _ => true,
        }
    }
}
