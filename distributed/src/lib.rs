//! Distributed ledger adapters over the shared, storage-independent core.
//!
//! V1 keeps a full terminal journal: a chargeback retains a `charged_back` row
//! instead of deleting it. The in-memory CLI deliberately keeps its older
//! delete-on-chargeback behaviour, so `JournalPolicy` is the only divergence
//! between the two adapters.

pub mod config;
pub mod ingest;
pub mod kafka_dlq;
pub mod kafka_staging;
pub mod kafka_transactions;
pub mod job_export;
pub mod manifest;
pub mod migrate;
pub mod partition;
pub mod repository;

use std::collections::HashMap;

use transaction_processor::{
    apply, Account, CanonicalClientId, Journal, JournalEffect, JournalPolicy, LedgerEntry,
    LedgerTransaction, ProcessingError, TransactionId,
};

#[derive(Debug, Default)]
struct JournalMap(HashMap<TransactionId, LedgerEntry<CanonicalClientId>>);

impl Journal<CanonicalClientId> for JournalMap {
    fn get(&self, id: TransactionId) -> Option<LedgerEntry<CanonicalClientId>> {
        self.0.get(&id).copied()
    }
}

/// A ledger with a canonical `BIGINT` client boundary and a terminal journal.
#[derive(Debug, Default)]
pub struct DurableLedger {
    accounts: HashMap<CanonicalClientId, Account>,
    journal: JournalMap,
}

impl DurableLedger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn process(
        &mut self,
        transaction: &LedgerTransaction<CanonicalClientId>,
    ) -> Result<(), ProcessingError> {
        let Self { accounts, journal } = self;
        let account = accounts.entry(transaction.client).or_default();

        let effect = apply(
            account,
            transaction,
            &*journal,
            JournalPolicy::RetainChargedBack,
        )?;

        match effect {
            JournalEffect::Insert(entry) => {
                journal.0.insert(transaction.transaction_id, entry);
            }
            JournalEffect::Remove => {
                journal.0.remove(&transaction.transaction_id);
            }
        }
        Ok(())
    }

    pub fn accounts(&self) -> &HashMap<CanonicalClientId, Account> {
        &self.accounts
    }

    pub fn entries(&self) -> &HashMap<TransactionId, LedgerEntry<CanonicalClientId>> {
        &self.journal.0
    }
}
