use crate::transaction::transaction_id::TransactionId;

use super::entry::{LedgerEntry, LedgerId};

/// Read-only journal lookup used by the shared ledger core.
///
/// Adapters own the mutation: they apply the returned [`JournalEffect`] with
/// their own storage semantics (in-memory map, database row, and so on).
///
/// [`JournalEffect`]: super::entry::JournalEffect
pub trait Journal<C: LedgerId> {
    fn get(&self, id: TransactionId) -> Option<LedgerEntry<C>>;
}
