use crate::account::account::Account;
use crate::amount::Amount;
use crate::processing_error::ProcessingError;
use crate::transaction::transaction_type::TransactionType;

use super::entry::{
    EntryState, JournalEffect, LedgerEntry, LedgerId, LedgerTransaction,
};
use super::journal::Journal;

/// How chargebacks affect the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalPolicy {
    /// The in-memory CLI removes the entry so its ID can be reused.
    DeleteOnChargeback,
    /// The durable ledger keeps a terminal `charged_back` row.
    RetainChargedBack,
}

/// Apply one ledger operation to an account and read-only journal.
///
/// The function does not own storage. On success it returns the journal mutation
/// the adapter must apply; on failure it returns the domain error and leaves the
/// account unchanged, except for the first over-available dispute, which locks
/// the account as required by the ledger contract.
pub fn apply<C: LedgerId>(
    account: &mut Account,
    transaction: &LedgerTransaction<C>,
    journal: &impl Journal<C>,
    policy: JournalPolicy,
) -> Result<JournalEffect<C>, ProcessingError> {
    if account.locked {
        return Err(ProcessingError::AccountLocked);
    }

    match transaction.transaction_type {
        TransactionType::Deposit => {
            let amount = transaction.amount.ok_or(ProcessingError::MissingAmount)?;
            reject_reused_identifier(transaction, journal)?;
            account.credit(amount)?;
            Ok(insert(transaction, amount, EntryState::Settled))
        }
        TransactionType::Withdrawal => {
            let amount = transaction.amount.ok_or(ProcessingError::MissingAmount)?;
            reject_reused_identifier(transaction, journal)?;
            account.debit(amount)?;
            Ok(insert(transaction, amount, EntryState::Settled))
        }
        TransactionType::Dispute => {
            let entry = lookup(transaction, journal)?;
            match entry.state {
                EntryState::Settled => match account.hold(entry.amount) {
                    Ok(()) => Ok(insert(transaction, entry.amount, EntryState::Disputed)),
                    Err(error @ ProcessingError::DisputeExceedsAvailable { .. }) => {
                        // A first over-available dispute is terminal for the run.
                        account.locked = true;
                        Err(error)
                    }
                    Err(error) => Err(error),
                },
                EntryState::Disputed => {
                    Err(ProcessingError::AlreadyDisputed(transaction.transaction_id))
                }
                EntryState::ChargedBack => {
                    Err(ProcessingError::ChargedBack(transaction.transaction_id))
                }
            }
        }
        TransactionType::Resolve => {
            let entry = lookup(transaction, journal)?;
            match entry.state {
                EntryState::Disputed => {
                    account.release(entry.amount)?;
                    Ok(insert(transaction, entry.amount, EntryState::Settled))
                }
                EntryState::Settled => {
                    Err(ProcessingError::NotDisputed(transaction.transaction_id))
                }
                EntryState::ChargedBack => {
                    Err(ProcessingError::ChargedBack(transaction.transaction_id))
                }
            }
        }
        TransactionType::Chargeback => {
            let entry = lookup(transaction, journal)?;
            match entry.state {
                EntryState::Disputed => {
                    account.chargeback(entry.amount)?;
                    Ok(match policy {
                        JournalPolicy::DeleteOnChargeback => JournalEffect::Remove,
                        JournalPolicy::RetainChargedBack => {
                            insert(transaction, entry.amount, EntryState::ChargedBack)
                        }
                    })
                }
                EntryState::Settled => {
                    Err(ProcessingError::NotDisputed(transaction.transaction_id))
                }
                EntryState::ChargedBack => {
                    Err(ProcessingError::ChargedBack(transaction.transaction_id))
                }
            }
        }
    }
}

fn reject_reused_identifier<C: LedgerId>(
    transaction: &LedgerTransaction<C>,
    journal: &impl Journal<C>,
) -> Result<(), ProcessingError> {
    if journal.get(transaction.transaction_id).is_some() {
        return Err(ProcessingError::DuplicateTransaction(
            transaction.transaction_id,
        ));
    }
    Ok(())
}

fn lookup<C: LedgerId>(
    transaction: &LedgerTransaction<C>,
    journal: &impl Journal<C>,
) -> Result<LedgerEntry<C>, ProcessingError> {
    let entry = journal.get(transaction.transaction_id).ok_or(
        ProcessingError::UnknownTransaction(transaction.transaction_id),
    )?;
    if entry.client != transaction.client {
        return Err(ProcessingError::NotOwner(transaction.transaction_id));
    }
    Ok(entry)
}

fn insert<C: LedgerId>(
    transaction: &LedgerTransaction<C>,
    amount: Amount,
    state: EntryState,
) -> JournalEffect<C> {
    JournalEffect::Insert(LedgerEntry {
        client: transaction.client,
        amount,
        state,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::amount::Amount;
    use crate::transaction::transaction_id::TransactionId;

    #[derive(Default)]
    struct MemoryJournal {
        entries: HashMap<TransactionId, LedgerEntry<u16>>,
    }

    impl Journal<u16> for MemoryJournal {
        fn get(&self, id: TransactionId) -> Option<LedgerEntry<u16>> {
            self.entries.get(&id).copied()
        }
    }

    fn amount(value: &str) -> Amount {
        Amount::parse(value).expect("valid amount")
    }

    fn transaction(
        transaction_type: TransactionType,
        client: u16,
        id: TransactionId,
        value: Option<&str>,
    ) -> LedgerTransaction<u16> {
        LedgerTransaction {
            transaction_type,
            client,
            transaction_id: id,
            amount: value.map(amount),
        }
    }

    fn apply_and_store(
        account: &mut Account,
        journal: &mut MemoryJournal,
        transaction: &LedgerTransaction<u16>,
        policy: JournalPolicy,
    ) -> Result<JournalEffect<u16>, ProcessingError> {
        let effect = apply(account, transaction, journal, policy)?;
        match effect {
            JournalEffect::Insert(entry) => {
                journal.entries.insert(transaction.transaction_id, entry);
            }
            JournalEffect::Remove => {
                journal.entries.remove(&transaction.transaction_id);
            }
        }
        Ok(effect)
    }

    fn settled(client: u16, value: &str) -> LedgerEntry<u16> {
        LedgerEntry {
            client,
            amount: amount(value),
            state: EntryState::Settled,
        }
    }

    #[test]
    fn deposit_credits_available_and_inserts_a_settled_entry() {
        let mut account = Account::default();
        let mut journal = MemoryJournal::default();
        let transaction = transaction(TransactionType::Deposit, 1, 1, Some("10.0"));

        let effect = apply_and_store(
            &mut account,
            &mut journal,
            &transaction,
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("deposit succeeds");

        assert_eq!(account.available, amount("10.0"));
        assert_eq!(effect, JournalEffect::Insert(settled(1, "10.0")));
    }

    #[test]
    fn withdrawal_debits_available_and_inserts_a_settled_entry() {
        let mut account = Account::default();
        let mut journal = MemoryJournal::default();
        account.credit(amount("10.0")).expect("credit fits");

        let transaction = transaction(TransactionType::Withdrawal, 1, 2, Some("4.0"));
        let effect = apply_and_store(
            &mut account,
            &mut journal,
            &transaction,
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("withdrawal succeeds");

        assert_eq!(account.available, amount("6.0"));
        assert_eq!(effect, JournalEffect::Insert(settled(1, "4.0")));
    }

    #[test]
    fn insufficient_withdrawal_leaves_the_account_and_journal_unchanged() {
        let mut account = Account::default();
        let mut journal = MemoryJournal::default();
        account.credit(amount("3.0")).expect("credit fits");

        let result = apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Withdrawal, 1, 2, Some("5.0")),
            JournalPolicy::DeleteOnChargeback,
        );

        assert!(matches!(result, Err(ProcessingError::InsufficientFunds)));
        assert_eq!(account.available, amount("3.0"));
        assert!(journal.entries.is_empty());
    }

    #[test]
    fn duplicate_transaction_is_rejected_before_credit() {
        let mut account = Account::default();
        let mut journal = MemoryJournal::default();
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Deposit, 1, 1, Some("10.0")),
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("first deposit succeeds");

        let result = apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Deposit, 1, 1, Some("5.0")),
            JournalPolicy::DeleteOnChargeback,
        );

        assert!(matches!(result, Err(ProcessingError::DuplicateTransaction(1))));
        assert_eq!(account.available, amount("10.0"));
    }

    #[test]
    fn dispute_moves_funds_to_held_and_marks_the_entry_disputed() {
        let mut account = Account::default();
        let mut journal = MemoryJournal::default();
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Deposit, 1, 1, Some("10.0")),
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("deposit succeeds");

        let effect = apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Dispute, 1, 1, None),
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("dispute succeeds");

        assert_eq!(account.available, Amount::ZERO);
        assert_eq!(account.held, amount("10.0"));
        assert_eq!(
            effect,
            JournalEffect::Insert(LedgerEntry {
                client: 1,
                amount: amount("10.0"),
                state: EntryState::Disputed,
            })
        );
    }

    #[test]
    fn dispute_that_exceeds_available_locks_without_moving_funds() {
        let mut account = Account::default();
        let mut journal = MemoryJournal::default();
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Deposit, 1, 1, Some("100.0")),
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("deposit succeeds");
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Withdrawal, 1, 2, Some("80.0")),
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("withdrawal succeeds");

        let result = apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Dispute, 1, 1, None),
            JournalPolicy::DeleteOnChargeback,
        );

        assert!(matches!(
            result,
            Err(ProcessingError::DisputeExceedsAvailable { .. })
        ));
        assert_eq!(account.available, amount("20.0"));
        assert_eq!(account.held, Amount::ZERO);
        assert!(account.locked);
        assert_eq!(journal.entries[&1], settled(1, "100.0"));
    }

    #[test]
    fn dispute_of_exactly_available_succeeds() {
        let mut account = Account::default();
        let mut journal = MemoryJournal::default();
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Deposit, 1, 1, Some("10.0")),
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("deposit succeeds");

        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Dispute, 1, 1, None),
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("exact-available dispute succeeds");

        assert_eq!(account.available, Amount::ZERO);
        assert_eq!(account.held, amount("10.0"));
        assert!(!account.locked);
    }

    #[test]
    fn unknown_transaction_is_rejected_without_locking() {
        let mut account = Account::default();
        let journal = MemoryJournal::default();

        let result = apply(
            &mut account,
            &transaction(TransactionType::Dispute, 1, 99, None),
            &journal,
            JournalPolicy::DeleteOnChargeback,
        );

        assert!(matches!(result, Err(ProcessingError::UnknownTransaction(99))));
        assert!(!account.locked);
    }

    #[test]
    fn foreign_transaction_is_rejected_without_locking() {
        let mut account = Account::default();
        let mut journal = MemoryJournal::default();
        journal.entries.insert(1, settled(1, "10.0"));

        let result = apply(
            &mut account,
            &transaction(TransactionType::Dispute, 2, 1, None),
            &journal,
            JournalPolicy::DeleteOnChargeback,
        );

        assert!(matches!(result, Err(ProcessingError::NotOwner(1))));
        assert!(!account.locked);
        assert_eq!(account.held, Amount::ZERO);
    }

    #[test]
    fn resolve_returns_held_funds_and_marks_the_entry_settled() {
        let mut account = Account::default();
        let mut journal = MemoryJournal::default();
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Deposit, 1, 1, Some("10.0")),
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("deposit succeeds");
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Dispute, 1, 1, None),
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("dispute succeeds");

        let effect = apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Resolve, 1, 1, None),
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("resolve succeeds");

        assert_eq!(account.available, amount("10.0"));
        assert_eq!(account.held, Amount::ZERO);
        assert_eq!(effect, JournalEffect::Insert(settled(1, "10.0")));
    }

    #[test]
    fn chargeback_removes_the_entry_under_the_delete_policy() {
        let mut account = Account::default();
        let mut journal = MemoryJournal::default();
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Deposit, 1, 1, Some("10.0")),
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("deposit succeeds");
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Dispute, 1, 1, None),
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("dispute succeeds");

        let effect = apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Chargeback, 1, 1, None),
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("chargeback succeeds");

        assert_eq!(effect, JournalEffect::Remove);
        assert!(journal.entries.is_empty());
        assert_eq!(account.held, Amount::ZERO);
        assert!(account.locked);
    }

    #[test]
    fn chargeback_retains_a_terminal_entry_under_the_retain_policy() {
        let mut account = Account::default();
        let mut journal = MemoryJournal::default();
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Deposit, 1, 1, Some("10.0")),
            JournalPolicy::RetainChargedBack,
        )
        .expect("deposit succeeds");
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Dispute, 1, 1, None),
            JournalPolicy::RetainChargedBack,
        )
        .expect("dispute succeeds");

        let effect = apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Chargeback, 1, 1, None),
            JournalPolicy::RetainChargedBack,
        )
        .expect("chargeback succeeds");

        assert_eq!(
            effect,
            JournalEffect::Insert(LedgerEntry {
                client: 1,
                amount: amount("10.0"),
                state: EntryState::ChargedBack,
            })
        );
        assert_eq!(journal.entries[&1].state, EntryState::ChargedBack);
        assert!(account.locked);
    }

    #[test]
    fn charged_back_entry_cannot_be_reused() {
        let mut account = Account::default();
        let mut journal = MemoryJournal::default();
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Deposit, 1, 1, Some("10.0")),
            JournalPolicy::RetainChargedBack,
        )
        .expect("deposit succeeds");
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Dispute, 1, 1, None),
            JournalPolicy::RetainChargedBack,
        )
        .expect("dispute succeeds");
        apply_and_store(
            &mut account,
            &mut journal,
            &transaction(TransactionType::Chargeback, 1, 1, None),
            JournalPolicy::RetainChargedBack,
        )
        .expect("chargeback succeeds");

        let result = apply(
            &mut Account::default(),
            &transaction(TransactionType::Deposit, 2, 1, Some("5.0")),
            &journal,
            JournalPolicy::RetainChargedBack,
        );

        assert!(matches!(result, Err(ProcessingError::DuplicateTransaction(1))));
    }

    #[test]
    fn locked_account_is_rejected_before_amount_validation() {
        let mut account = Account {
            locked: true,
            ..Default::default()
        };
        let journal = MemoryJournal::default();

        let result = apply(
            &mut account,
            &transaction(TransactionType::Withdrawal, 1, 1, None),
            &journal,
            JournalPolicy::DeleteOnChargeback,
        );

        assert!(matches!(result, Err(ProcessingError::AccountLocked)));
    }

    #[test]
    fn canonical_bigint_client_ids_use_the_same_rules() {
        struct BigJournal(HashMap<TransactionId, LedgerEntry<i64>>);

        impl Journal<i64> for BigJournal {
            fn get(&self, id: TransactionId) -> Option<LedgerEntry<i64>> {
                self.0.get(&id).copied()
            }
        }

        let client = 4_000_000_042_i64;
        let mut account = Account::default();
        let mut journal = BigJournal(HashMap::new());

        let deposit = LedgerTransaction {
            transaction_type: TransactionType::Deposit,
            client,
            transaction_id: 1,
            amount: Some(amount("10.0")),
        };
        let effect = apply(
            &mut account,
            &deposit,
            &journal,
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("deposit succeeds");
        if let JournalEffect::Insert(entry) = effect {
            journal.0.insert(1, entry);
        }

        apply(
            &mut account,
            &LedgerTransaction {
                transaction_type: TransactionType::Dispute,
                client,
                transaction_id: 1,
                amount: None,
            },
            &journal,
            JournalPolicy::DeleteOnChargeback,
        )
        .expect("dispute succeeds");

        assert_eq!(account.available, Amount::ZERO);
        assert_eq!(account.held, amount("10.0"));
        assert_eq!(journal.0[&1].client, client);
    }
}
