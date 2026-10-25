use crate::amount::Amount;
use crate::transaction::transaction_id::TransactionId;
use crate::transaction::transaction_type::TransactionType;

/// Identifier boundary for the shared ledger core.
///
/// The in-memory CLI uses `u16`; the distributed adapter can use its canonical
/// `BIGINT` without duplicating the state machine.
pub trait LedgerId: Copy + Eq + std::fmt::Debug {}

impl<T: Copy + Eq + std::fmt::Debug> LedgerId for T {}

/// The lifecycle state of an accepted transaction in the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryState {
    Settled,
    Disputed,
    ChargedBack,
}

/// A journal row independent of its storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerEntry<C> {
    pub client: C,
    pub amount: Amount,
    pub state: EntryState,
}

/// A ledger operation independent of the CSV parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerTransaction<C> {
    pub transaction_type: TransactionType,
    pub client: C,
    pub transaction_id: TransactionId,
    pub amount: Option<Amount>,
}

/// The journal mutation a successful operation requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalEffect<C> {
    Insert(LedgerEntry<C>),
    Remove,
}
