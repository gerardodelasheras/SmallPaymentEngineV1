//! Compatibility surface for the shared ledger journal entry.
//!
//! **BREAKING:** `TransactionEntry` used to be an enum with `Settled` and
//! `Disputed` variants. It is now an alias for `LedgerEntry<ClientId>`, whose
//! lifecycle state lives in the `state` field. Library consumers should use
//! `LedgerEntry` and `EntryState` directly.

pub use crate::ledger::entry::{EntryState, LedgerEntry};

use crate::client::client_id::ClientId;

/// The in-memory CLI's journal entry type.
pub type TransactionEntry = LedgerEntry<ClientId>;
