use std::collections::HashMap;

use crate::account::account::Account;
use crate::amount::Amount;
use crate::client::client_id::ClientId;
use crate::processing_error::ProcessingError;
use crate::transaction::transaction::Transaction;
use crate::transaction::transaction_entry::TransactionEntry;
use crate::transaction::transaction_id::TransactionId;
use crate::transaction::transaction_type::TransactionType;

/// Per-worker account and transaction state machine.
#[derive(Debug, Default)]
pub struct TransactionProcessor {
    accounts: HashMap<ClientId, Account>,
    entries: HashMap<TransactionId, TransactionEntry>,
}

impl TransactionProcessor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn process(&mut self, transaction: &Transaction) -> Result<(), ProcessingError> {
        // A parsed row initializes its client before domain validation.
        self.accounts.entry(transaction.client).or_default();

        if self
            .accounts
            .get(&transaction.client)
            .map(|account| account.locked)
            .unwrap_or(false)
        {
            return Err(ProcessingError::AccountLocked);
        }

        match transaction.transaction_type {
            TransactionType::Deposit => self.process_deposit(transaction),
            TransactionType::Withdrawal => self.process_withdrawal(transaction),
            TransactionType::Dispute => self.process_dispute(transaction),
            TransactionType::Resolve => self.process_resolve(transaction),
            TransactionType::Chargeback => self.process_chargeback(transaction),
        }
    }

    fn process_deposit(&mut self, transaction: &Transaction) -> Result<(), ProcessingError> {
        let amount = transaction.amount.ok_or(ProcessingError::MissingAmount)?;
        if self.entries.contains_key(&transaction.transaction_id) {
            return Err(ProcessingError::DuplicateTransaction(
                transaction.transaction_id,
            ));
        }

        self.account_mut(transaction.client).credit(amount)?;
        self.record_settled(transaction, amount);
        Ok(())
    }

    fn process_withdrawal(&mut self, transaction: &Transaction) -> Result<(), ProcessingError> {
        let amount = transaction.amount.ok_or(ProcessingError::MissingAmount)?;
        if self.entries.contains_key(&transaction.transaction_id) {
            return Err(ProcessingError::DuplicateTransaction(
                transaction.transaction_id,
            ));
        }

        self.account_mut(transaction.client).debit(amount)?;
        self.record_settled(transaction, amount);
        Ok(())
    }

    fn record_settled(&mut self, transaction: &Transaction, amount: Amount) {
        self.entries.insert(
            transaction.transaction_id,
            TransactionEntry::Settled {
                client: transaction.client,
                amount,
            },
        );
    }

    fn account_mut(&mut self, client: ClientId) -> &mut Account {
        self.accounts.entry(client).or_default()
    }

    fn lookup(&self, transaction: &Transaction) -> Result<TransactionEntry, ProcessingError> {
        let entry = self.entries.get(&transaction.transaction_id).ok_or(
            ProcessingError::UnknownTransaction(transaction.transaction_id),
        )?;
        let owner = match entry {
            TransactionEntry::Settled { client, .. }
            | TransactionEntry::Disputed { client, .. } => *client,
        };
        if owner != transaction.client {
            return Err(ProcessingError::NotOwner(transaction.transaction_id));
        }
        Ok(*entry)
    }

    fn process_dispute(&mut self, transaction: &Transaction) -> Result<(), ProcessingError> {
        let amount = match self.lookup(transaction)? {
            TransactionEntry::Settled { amount, .. } => amount,
            TransactionEntry::Disputed { .. } => {
                return Err(ProcessingError::AlreadyDisputed(transaction.transaction_id));
            }
        };

        match self.account_mut(transaction.client).hold(amount) {
            Ok(()) => {
                self.entries.insert(
                    transaction.transaction_id,
                    TransactionEntry::Disputed {
                        client: transaction.client,
                        amount,
                    },
                );
                Ok(())
            }
            Err(error @ ProcessingError::DisputeExceedsAvailable { .. }) => {
                self.account_mut(transaction.client).locked = true;
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    fn process_resolve(&mut self, transaction: &Transaction) -> Result<(), ProcessingError> {
        let amount = match self.lookup(transaction)? {
            TransactionEntry::Disputed { amount, .. } => amount,
            TransactionEntry::Settled { .. } => {
                return Err(ProcessingError::NotDisputed(transaction.transaction_id));
            }
        };

        self.account_mut(transaction.client).release(amount)?;
        self.entries.insert(
            transaction.transaction_id,
            TransactionEntry::Settled {
                client: transaction.client,
                amount,
            },
        );
        Ok(())
    }

    fn process_chargeback(&mut self, transaction: &Transaction) -> Result<(), ProcessingError> {
        let amount = match self.lookup(transaction)? {
            TransactionEntry::Disputed { amount, .. } => amount,
            TransactionEntry::Settled { .. } => {
                return Err(ProcessingError::NotDisputed(transaction.transaction_id));
            }
        };

        self.account_mut(transaction.client).chargeback(amount)?;
        self.entries.remove(&transaction.transaction_id);
        Ok(())
    }

    pub(crate) fn take_accounts(&mut self) -> HashMap<ClientId, Account> {
        std::mem::take(&mut self.accounts)
    }

    pub fn accounts(&self) -> &HashMap<ClientId, Account> {
        &self.accounts
    }

    pub fn snapshot(&self) -> HashMap<ClientId, Account> {
        self.accounts.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::amount::Amount;

    fn amount(value: &str) -> Option<Amount> {
        Some(Amount::parse(value).expect("valid amount"))
    }

    fn transaction(
        transaction_type: TransactionType,
        client: ClientId,
        id: TransactionId,
        value: Option<&str>,
    ) -> Transaction {
        Transaction {
            transaction_type,
            client,
            transaction_id: id,
            amount: value.map(|text| Amount::parse(text).expect("valid amount")),
        }
    }

    fn deposit(
        processor: &mut TransactionProcessor,
        client: ClientId,
        id: TransactionId,
        value: &str,
    ) {
        processor
            .process(&transaction(
                TransactionType::Deposit,
                client,
                id,
                Some(value),
            ))
            .expect("deposit succeeds");
    }

    fn account(processor: &TransactionProcessor, client: ClientId) -> Account {
        processor
            .accounts()
            .get(&client)
            .cloned()
            .expect("account exists")
    }

    #[test]
    fn deposit_credits_available() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        assert_eq!(account(&processor, 1).available, amount("10.0").unwrap());
        assert_eq!(account(&processor, 1).total(), amount("10.0").unwrap());
    }

    #[test]
    fn withdrawal_debits_available() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        processor
            .process(&transaction(TransactionType::Withdrawal, 1, 2, Some("4.0")))
            .expect("withdrawal succeeds");
        assert_eq!(account(&processor, 1).available, amount("6.0").unwrap());
    }

    #[test]
    fn insufficient_withdrawal_leaves_balances_unchanged() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "3.0");
        assert!(matches!(
            processor.process(&transaction(TransactionType::Withdrawal, 1, 2, Some("5.0"))),
            Err(ProcessingError::InsufficientFunds)
        ));
        assert_eq!(account(&processor, 1).available, amount("3.0").unwrap());
    }

    #[test]
    fn dispute_holds_funds() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        processor
            .process(&transaction(TransactionType::Dispute, 1, 1, None))
            .expect("dispute succeeds");
        let account = account(&processor, 1);
        assert_eq!(account.available, Amount::ZERO);
        assert_eq!(account.held, amount("10.0").unwrap());
        assert_eq!(account.total(), amount("10.0").unwrap());
        assert!(!account.locked);
    }

    #[test]
    fn dispute_that_exceeds_available_locks_the_account_without_changing_balances() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "100.0");
        processor
            .process(&transaction(
                TransactionType::Withdrawal,
                1,
                2,
                Some("80.0"),
            ))
            .expect("withdrawal succeeds");

        assert!(matches!(
            processor.process(&transaction(TransactionType::Dispute, 1, 1, None)),
            Err(ProcessingError::DisputeExceedsAvailable { .. })
        ));

        let account = account(&processor, 1);
        assert_eq!(account.available, amount("20.0").unwrap());
        assert_eq!(account.held, Amount::ZERO);
        assert_eq!(account.total(), amount("20.0").unwrap());
        assert!(account.locked);
    }

    #[test]
    fn later_operations_after_an_over_available_dispute_are_rejected() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        processor
            .process(&transaction(
                TransactionType::Withdrawal,
                1,
                2,
                Some("10.0"),
            ))
            .expect("withdrawal succeeds");

        assert!(matches!(
            processor.process(&transaction(TransactionType::Dispute, 1, 1, None)),
            Err(ProcessingError::DisputeExceedsAvailable { .. })
        ));
        assert!(matches!(
            processor.process(&transaction(TransactionType::Deposit, 1, 3, Some("5.0"))),
            Err(ProcessingError::AccountLocked)
        ));

        let account = account(&processor, 1);
        assert_eq!(account.available, Amount::ZERO);
        assert_eq!(account.held, Amount::ZERO);
        assert!(account.locked);
    }

    #[test]
    fn held_funds_remain_after_an_over_available_dispute_locks_the_account() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        deposit(&mut processor, 1, 2, "20.0");
        processor
            .process(&transaction(TransactionType::Dispute, 1, 1, None))
            .expect("first dispute succeeds");
        processor
            .process(&transaction(
                TransactionType::Withdrawal,
                1,
                3,
                Some("10.0"),
            ))
            .expect("withdrawal succeeds");

        assert!(matches!(
            processor.process(&transaction(TransactionType::Dispute, 1, 2, None)),
            Err(ProcessingError::DisputeExceedsAvailable { .. })
        ));
        assert!(matches!(
            processor.process(&transaction(TransactionType::Chargeback, 1, 2, None)),
            Err(ProcessingError::AccountLocked)
        ));

        let account = account(&processor, 1);
        assert_eq!(account.available, amount("10.0").unwrap());
        assert_eq!(account.held, amount("10.0").unwrap());
        assert_eq!(account.total(), amount("20.0").unwrap());
        assert!(account.locked);
    }

    #[test]
    fn dispute_of_unknown_transaction_is_rejected() {
        let mut processor = TransactionProcessor::new();
        assert!(matches!(
            processor.process(&transaction(TransactionType::Dispute, 1, 99, None)),
            Err(ProcessingError::UnknownTransaction(99))
        ));
        assert_eq!(account(&processor, 1).available, Amount::ZERO);
        assert!(!account(&processor, 1).locked);
    }

    #[test]
    fn duplicate_dispute_is_rejected() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        processor
            .process(&transaction(TransactionType::Dispute, 1, 1, None))
            .expect("first dispute succeeds");
        assert!(matches!(
            processor.process(&transaction(TransactionType::Dispute, 1, 1, None)),
            Err(ProcessingError::AlreadyDisputed(1))
        ));
        assert_eq!(account(&processor, 1).held, amount("10.0").unwrap());
        assert!(!account(&processor, 1).locked);
    }

    #[test]
    fn resolve_releases_held_funds() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        processor
            .process(&transaction(TransactionType::Dispute, 1, 1, None))
            .unwrap();
        processor
            .process(&transaction(TransactionType::Resolve, 1, 1, None))
            .unwrap();
        let account = account(&processor, 1);
        assert_eq!(account.available, amount("10.0").unwrap());
        assert_eq!(account.held, Amount::ZERO);
    }

    #[test]
    fn resolving_a_settled_transaction_is_rejected() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        assert!(matches!(
            processor.process(&transaction(TransactionType::Resolve, 1, 1, None)),
            Err(ProcessingError::NotDisputed(1))
        ));
    }

    #[test]
    fn resolved_transaction_may_be_disputed_again() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        processor
            .process(&transaction(TransactionType::Dispute, 1, 1, None))
            .unwrap();
        processor
            .process(&transaction(TransactionType::Resolve, 1, 1, None))
            .unwrap();
        processor
            .process(&transaction(TransactionType::Dispute, 1, 1, None))
            .unwrap();
        assert_eq!(account(&processor, 1).held, amount("10.0").unwrap());
    }

    #[test]
    fn chargeback_removes_held_funds_and_locks() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        processor
            .process(&transaction(TransactionType::Dispute, 1, 1, None))
            .unwrap();
        processor
            .process(&transaction(TransactionType::Chargeback, 1, 1, None))
            .unwrap();
        let account = account(&processor, 1);
        assert_eq!(account.available, Amount::ZERO);
        assert_eq!(account.held, Amount::ZERO);
        assert_eq!(account.total(), Amount::ZERO);
        assert!(account.locked);
    }

    #[test]
    fn chargeback_of_settled_transaction_is_rejected() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        assert!(matches!(
            processor.process(&transaction(TransactionType::Chargeback, 1, 1, None)),
            Err(ProcessingError::NotDisputed(1))
        ));
        assert!(!account(&processor, 1).locked);
    }

    #[test]
    fn locked_account_rejects_later_operations() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        processor
            .process(&transaction(TransactionType::Dispute, 1, 1, None))
            .unwrap();
        processor
            .process(&transaction(TransactionType::Chargeback, 1, 1, None))
            .unwrap();
        assert!(matches!(
            processor.process(&transaction(TransactionType::Deposit, 1, 2, Some("5.0"))),
            Err(ProcessingError::AccountLocked)
        ));
        assert_eq!(account(&processor, 1).available, Amount::ZERO);
    }

    #[test]
    fn duplicate_transaction_identifier_is_rejected() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        assert!(matches!(
            processor.process(&transaction(TransactionType::Deposit, 1, 1, Some("5.0"))),
            Err(ProcessingError::DuplicateTransaction(1))
        ));
        assert_eq!(account(&processor, 1).available, amount("10.0").unwrap());
    }

    #[test]
    fn foreign_transaction_reference_is_rejected() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 1, 1, "10.0");
        assert!(matches!(
            processor.process(&transaction(TransactionType::Dispute, 2, 1, None)),
            Err(ProcessingError::NotOwner(1))
        ));
        assert_eq!(account(&processor, 1).available, amount("10.0").unwrap());
        assert_eq!(account(&processor, 2).held, Amount::ZERO);
        assert!(!account(&processor, 2).locked);
    }

    #[test]
    fn rejected_row_still_initializes_the_client() {
        let mut processor = TransactionProcessor::new();
        assert!(matches!(
            processor.process(&transaction(TransactionType::Withdrawal, 1, 1, Some("5.0"))),
            Err(ProcessingError::InsufficientFunds)
        ));
        assert_eq!(account(&processor, 1), Account::default());
    }

    #[test]
    fn missing_amount_leaves_the_transaction_id_available() {
        let mut processor = TransactionProcessor::new();

        assert!(matches!(
            processor.process(&transaction(TransactionType::Deposit, 7, 42, None)),
            Err(ProcessingError::MissingAmount)
        ));
        assert_eq!(account(&processor, 7), Account::default());

        deposit(&mut processor, 7, 42, "3.0");
        assert_eq!(account(&processor, 7).available, amount("3.0").unwrap());
    }

    #[test]
    fn rejected_duplicate_does_not_replace_the_original_entry() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 3, 900, "10.0");

        assert!(matches!(
            processor.process(&transaction(TransactionType::Deposit, 3, 900, Some("5.0"))),
            Err(ProcessingError::DuplicateTransaction(900))
        ));

        processor
            .process(&transaction(TransactionType::Dispute, 3, 900, None))
            .expect("dispute succeeds");
        assert_eq!(account(&processor, 3).held, amount("10.0").unwrap());
    }

    #[test]
    fn take_accounts_moves_the_map_out() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 9, 1, "4.0");

        let accounts = processor.take_accounts();

        assert_eq!(accounts[&9].available, amount("4.0").unwrap());
        assert!(processor.accounts().is_empty());
    }

    #[test]
    fn locked_account_rejects_missing_amount_before_it_is_validated() {
        let mut processor = TransactionProcessor::new();
        deposit(&mut processor, 5, 100, "10.0");
        processor
            .process(&transaction(TransactionType::Dispute, 5, 100, None))
            .expect("dispute succeeds");
        processor
            .process(&transaction(TransactionType::Chargeback, 5, 100, None))
            .expect("chargeback succeeds");

        assert!(matches!(
            processor.process(&transaction(TransactionType::Withdrawal, 5, 101, None)),
            Err(ProcessingError::AccountLocked)
        ));

        let account = account(&processor, 5);
        assert!(account.locked);
        assert_eq!(account.total(), Amount::ZERO);
    }
}
