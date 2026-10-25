use crate::amount::Amount;
use crate::processing_error::ProcessingError;

/// A client's ledger state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Account {
    pub available: Amount,
    pub held: Amount,
    pub locked: bool,
}

impl Account {
    /// `total` is always `available + held`.
    pub fn total(&self) -> Amount {
        self.available
            .checked_add(self.held)
            .expect("account total overflow")
    }

    pub fn credit(&mut self, amount: Amount) -> Result<(), ProcessingError> {
        let available = self
            .available
            .checked_add(amount)
            .ok_or(ProcessingError::Overflow)?;
        available
            .checked_add(self.held)
            .ok_or(ProcessingError::Overflow)?;
        self.available = available;
        Ok(())
    }

    pub fn debit(&mut self, amount: Amount) -> Result<(), ProcessingError> {
        if self.available < amount {
            return Err(ProcessingError::InsufficientFunds);
        }
        self.available = self
            .available
            .checked_sub(amount)
            .ok_or(ProcessingError::Underflow)?;
        Ok(())
    }

    pub fn hold(&mut self, amount: Amount) -> Result<(), ProcessingError> {
        if self.available < amount {
            return Err(ProcessingError::DisputeExceedsAvailable {
                amount,
                available: self.available,
            });
        }

        let available = self
            .available
            .checked_sub(amount)
            .ok_or(ProcessingError::Underflow)?;
        let held = self
            .held
            .checked_add(amount)
            .ok_or(ProcessingError::Overflow)?;
        available
            .checked_add(held)
            .ok_or(ProcessingError::Overflow)?;
        self.available = available;
        self.held = held;
        Ok(())
    }

    pub fn release(&mut self, amount: Amount) -> Result<(), ProcessingError> {
        let held = self
            .held
            .checked_sub(amount)
            .ok_or(ProcessingError::Underflow)?;
        let available = self
            .available
            .checked_add(amount)
            .ok_or(ProcessingError::Overflow)?;
        available
            .checked_add(held)
            .ok_or(ProcessingError::Overflow)?;
        self.held = held;
        self.available = available;
        Ok(())
    }

    pub fn chargeback(&mut self, amount: Amount) -> Result<(), ProcessingError> {
        self.held = self
            .held
            .checked_sub(amount)
            .ok_or(ProcessingError::Underflow)?;
        self.locked = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn amount(value: &str) -> Amount {
        Amount::parse(value).expect("valid amount")
    }

    #[test]
    fn defaults_to_zeroed_balances() {
        let account = Account::default();
        assert_eq!(account.available, Amount::ZERO);
        assert_eq!(account.held, Amount::ZERO);
        assert!(!account.locked);
    }

    #[test]
    fn total_equals_available_plus_held() {
        let mut account = Account::default();
        account.credit(amount("10.0")).expect("credit fits");
        account.hold(amount("4.0")).expect("hold fits");
        assert_eq!(account.total(), amount("10.0"));
    }

    #[test]
    fn credit_increases_available() {
        let mut account = Account::default();
        account.credit(amount("2.5")).expect("credit fits");
        assert_eq!(account.available, amount("2.5"));
    }

    #[test]
    fn debit_rejects_insufficient_funds_without_mutation() {
        let mut account = Account::default();
        account.credit(amount("2.0")).expect("credit fits");
        assert!(matches!(
            account.debit(amount("3.0")),
            Err(ProcessingError::InsufficientFunds)
        ));
        assert_eq!(account.available, amount("2.0"));
    }

    #[test]
    fn hold_moves_funds_to_held() {
        let mut account = Account::default();
        account.credit(amount("10.0")).expect("credit fits");
        account.hold(amount("10.0")).expect("hold fits");
        assert_eq!(account.available, Amount::ZERO);
        assert_eq!(account.held, amount("10.0"));
    }

    #[test]
    fn resolve_moves_funds_back() {
        let mut account = Account::default();
        account.credit(amount("10.0")).expect("credit fits");
        account.hold(amount("10.0")).expect("hold fits");
        account.release(amount("10.0")).expect("release fits");
        assert_eq!(account.available, amount("10.0"));
        assert_eq!(account.held, Amount::ZERO);
    }

    #[test]
    fn chargeback_removes_held_funds_and_locks() {
        let mut account = Account::default();
        account.credit(amount("10.0")).expect("credit fits");
        account.hold(amount("10.0")).expect("hold fits");
        account.chargeback(amount("10.0")).expect("chargeback fits");
        assert_eq!(account.held, Amount::ZERO);
        assert_eq!(account.total(), Amount::ZERO);
        assert!(account.locked);
    }

    #[test]
    fn overflowing_credit_leaves_balances_unchanged() {
        let mut account = Account {
            available: Amount::from_scaled(i64::MAX),
            ..Default::default()
        };
        assert!(matches!(
            account.credit(Amount::from_scaled(1)),
            Err(ProcessingError::Overflow)
        ));
        assert_eq!(account.available, Amount::from_scaled(i64::MAX));
    }

    #[test]
    fn hold_rejects_an_over_available_dispute_without_changing_balances() {
        let mut account = Account::default();
        account.credit(amount("3.0")).expect("credit fits");

        assert!(matches!(
            account.hold(amount("5.0")),
            Err(ProcessingError::DisputeExceedsAvailable { .. })
        ));

        assert_eq!(account.available, amount("3.0"));
        assert_eq!(account.held, Amount::ZERO);
    }

    #[test]
    fn hold_of_exactly_available_moves_funds_to_held() {
        let mut account = Account::default();
        account.credit(amount("5.0")).expect("credit fits");

        account.hold(amount("5.0")).expect("hold fits");

        assert_eq!(account.available, Amount::ZERO);
        assert_eq!(account.held, amount("5.0"));
    }

    #[test]
    fn hold_overflow_is_not_reported_as_an_over_available_dispute() {
        let mut account = Account {
            available: Amount::from_scaled(1),
            held: Amount::from_scaled(i64::MAX),
            ..Default::default()
        };

        assert!(matches!(
            account.hold(Amount::from_scaled(1)),
            Err(ProcessingError::Overflow)
        ));

        assert_eq!(account.available, Amount::from_scaled(1));
        assert_eq!(account.held, Amount::from_scaled(i64::MAX));
    }
}
