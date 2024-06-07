use std::collections::HashMap;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::tx::engine::result::{TxError, TxResult};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AccountSummary {
    pub id: u16,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub is_locked: bool,
}

#[derive(Debug, Clone)]
pub struct Account {
    ledger: HashMap<u32, LedgerEntry>,
    id: u16,
    available: Decimal,
    held: Decimal,
    is_locked: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum LedgerEntryState {
    Normal,
    Disputed,
    ChargedBack,
}

#[derive(Debug, Clone)]
struct LedgerEntry {
    amount: Decimal,
    state: LedgerEntryState,
}

impl Account {
    pub fn new(id: u16) -> Self {
        Self {
            ledger: HashMap::new(),
            available: dec!(0),
            held: dec!(0),
            is_locked: false,
            id,
        }
    }

    pub fn summary(&self) -> AccountSummary {
        AccountSummary {
            id: self.id(),
            available: self.available(),
            held: self.held(),
            total: self.total(),
            is_locked: self.is_locked(),
        }
    }

    pub fn total(&self) -> Decimal {
        self.available + self.held
    }

    pub fn available(&self) -> Decimal {
        self.available
    }

    pub fn held(&self) -> Decimal {
        self.held
    }

    pub fn is_locked(&self) -> bool {
        self.is_locked
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    fn require_unique_transaction(&self, tx_id: u32) -> TxResult<()> {
        if self.ledger.contains_key(&tx_id) {
            Err(TxError::InvalidOperation(format!(
                "Attempt to execute a transaction [{}] twice for account [{}].",
                tx_id, self.id
            )))
        } else {
            Ok(())
        }
    }

    fn require_unlocked(&self) -> TxResult<()> {
        if self.is_locked {
            Err(TxError::InvalidOperation(format!(
                "Attempt to execute a transaction on locked account [{}].",
                self.id
            )))
        } else {
            Ok(())
        }
    }

    pub fn withdraw(&mut self, tx_id: u32, amount: Decimal) -> TxResult<()> {
        self.require_unique_transaction(tx_id)?;
        self.require_unlocked()?;

        if amount < dec!(0) {
            return Err(TxError::InvalidArgument(format!(
                "Attempt to withdraw a negative amount [{}] in transaction [{}] for account [{}].",
                amount, tx_id, self.id
            )));
        }

        if amount > self.available {
            return Err(TxError::InvalidArgument(format!(
                "Attempt to withdraw an amount [{}] greater than balance [{}] in transaction [{}] for account [{}].",
                amount, self.available, tx_id, self.id
            )));
        }

        self.ledger.insert(
            tx_id,
            LedgerEntry {
                amount: -amount,
                state: LedgerEntryState::Normal,
            },
        );
        self.available -= amount;

        Ok(())
    }

    pub fn deposit(&mut self, tx_id: u32, amount: Decimal) -> TxResult<()> {
        self.require_unique_transaction(tx_id)?;
        self.require_unlocked()?;

        if amount < dec!(0) {
            return Err(TxError::InvalidArgument(format!(
                "Attempt to deposit a negative amount [{}] in transaction [{}] for account [{}].",
                amount, tx_id, self.id
            )));
        }

        self.ledger.insert(
            tx_id,
            LedgerEntry {
                amount,
                state: LedgerEntryState::Normal,
            },
        );
        self.available += amount;

        Ok(())
    }

    pub fn dispute(&mut self, tx_id: u32) -> TxResult<()> {
        self.require_unlocked()?;

        if let Ok(entry) = self.get_tx_record(tx_id).cloned() {
            if entry.state != LedgerEntryState::Normal {
                return Ok(());
            }

            // disputing a deposit means the bank doesn't wanna unlock the credited funds yet
            self.get_tx_record(tx_id)?.state = LedgerEntryState::Disputed;
            self.available -= entry.amount;
            self.held += entry.amount;
        }

        Ok(())
    }

    pub fn resolve(&mut self, tx_id: u32) -> TxResult<()> {
        self.require_unlocked()?;

        if let Ok(entry) = self.get_tx_record(tx_id).cloned() {
            if entry.state != LedgerEntryState::Disputed {
                return Ok(());
            }

            // resolving a deposit dispute means the bank doesn't unlocked the credited funds
            self.get_tx_record(tx_id)?.state = LedgerEntryState::Normal;
            self.available += entry.amount;
            self.held -= entry.amount;
        }

        Ok(())
    }

    pub fn chargeback(&mut self, tx_id: u32) -> TxResult<()> {
        self.require_unlocked()?;

        if let Ok(entry) = self.get_tx_record(tx_id).cloned() {
            if entry.state != LedgerEntryState::Disputed {
                return Ok(());
            }

            // deposit charge back means the bank didn't accept the funds
            self.get_tx_record(tx_id)?.state = LedgerEntryState::ChargedBack;
            self.held -= entry.amount;
            self.is_locked = true;
        }

        Ok(())
    }

    fn get_tx_record(&mut self, tx_id: u32) -> TxResult<&mut LedgerEntry> {
        self.ledger.get_mut(&tx_id)
            .ok_or(TxError::InvalidArgument(format!(
                "Transaction [{}] is not known, was not a deposit/withdrawal or does not belong to account [{}].",
                tx_id, self.id
            )))
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use crate::tx::engine::account::Account;

    #[test]
    fn test_disputes_dont_fail_if_tx_does_not_exist() {
        let mut account = Account::new(1);

        account.chargeback(82).unwrap();
        account.resolve(82).unwrap();
        account.dispute(82).unwrap();
    }

    #[test]
    fn test_can_dispute_deposit() {
        let mut account = Account::new(1);

        account.deposit(23, dec!(123.23)).unwrap();

        assert_eq!(account.held(), dec!(0));
        assert_eq!(account.available(), dec!(123.23));
        assert_eq!(account.total(), dec!(123.23));
        assert!(!account.is_locked());

        account.dispute(23).unwrap();

        assert_eq!(account.held(), dec!(123.23));
        assert_eq!(account.available(), dec!(0));
        assert_eq!(account.total(), dec!(123.23));
        assert!(!account.is_locked());
    }

    #[test]
    fn test_accepts_each_transaction_only_once() {
        let mut account = Account::new(1);

        account.deposit(23, dec!(123.23)).unwrap();

        assert_eq!(
            format!("{:?}", account.deposit(23, dec!(123.23)).unwrap_err()),
            "InvalidOperation(\"Attempt to execute a transaction [23] twice for account [1].\")"
        );
    }

    #[test]
    fn test_can_not_withdraw_more_than_balance() {
        let mut account = Account::new(1);

        account.deposit(23, dec!(10)).unwrap();

        assert_eq!(
            format!(
                "{:?}",
                account.withdraw(24, dec!(10.0001)).unwrap_err()
            ),
            "InvalidArgument(\"Attempt to withdraw an amount [10.0001] greater than balance [10] in transaction [24] for account [1].\")"
        );
    }

    #[test]
    fn test_can_not_withdraw_held_funds() {
        let mut account = Account::new(1);

        account.deposit(23, dec!(10)).unwrap();
        account.dispute(23).unwrap();

        assert_eq!(
            format!(
                "{:?}",
                account.withdraw(24, dec!(1)).unwrap_err()
            ),
            "InvalidArgument(\"Attempt to withdraw an amount [1] greater than balance [0] in transaction [24] for account [1].\")"
        );
    }

    #[test]
    fn test_can_dispute_deposit_after_withdrawal() {
        let mut account = Account::new(1);

        account.deposit(23, dec!(100)).unwrap();
        account.withdraw(24, dec!(64)).unwrap();

        assert_eq!(account.held(), dec!(0));
        assert_eq!(account.available(), dec!(36));
        assert_eq!(account.total(), dec!(36));
        assert!(!account.is_locked());

        account.dispute(23).unwrap();

        assert_eq!(account.held(), dec!(100));
        assert_eq!(account.available(), dec!(-64));
        assert_eq!(account.total(), dec!(36));
        assert!(!account.is_locked());
    }

    #[test]
    fn test_can_dispute_deposits_after_withdrawal() {
        let mut account = Account::new(1);

        account.deposit(23, dec!(100)).unwrap();
        account.deposit(24, dec!(200)).unwrap();
        account.withdraw(25, dec!(164)).unwrap();

        assert_eq!(account.held(), dec!(0));
        assert_eq!(account.available(), dec!(136));
        assert_eq!(account.total(), dec!(136));
        assert!(!account.is_locked());

        account.dispute(23).unwrap();
        account.dispute(24).unwrap();

        assert_eq!(account.held(), dec!(300));
        assert_eq!(account.available(), dec!(-164));
        assert_eq!(account.total(), dec!(136));
        assert!(!account.is_locked());
    }

    #[test]
    fn test_can_dispute_withdrawal_after_deposit() {
        let mut account = Account::new(1);

        account.deposit(23, dec!(100)).unwrap();
        account.withdraw(24, dec!(64)).unwrap();

        assert_eq!(account.held(), dec!(0));
        assert_eq!(account.available(), dec!(36));
        assert_eq!(account.total(), dec!(36));
        assert!(!account.is_locked());

        account.dispute(24).unwrap();

        assert_eq!(account.held(), dec!(-64));
        assert_eq!(account.available(), dec!(100));
        assert_eq!(account.total(), dec!(36));
        assert!(!account.is_locked());
    }

    #[test]
    fn test_can_resolve_disputed_deposit() {
        let mut account = Account::new(1);

        account.deposit(23, dec!(123.23)).unwrap();
        account.dispute(23).unwrap();
        account.resolve(23).unwrap();

        assert_eq!(account.held(), dec!(0));
        assert_eq!(account.available(), dec!(123.23));
        assert_eq!(account.total(), dec!(123.23));
        assert!(!account.is_locked());
    }

    #[test]
    fn test_can_resolve_disputed_withdrawal() {
        let mut account = Account::new(1);

        account.deposit(23, dec!(132)).unwrap();
        account.withdraw(24, dec!(32)).unwrap();
        account.dispute(24).unwrap();
        account.resolve(24).unwrap();

        assert_eq!(account.held(), dec!(0));
        assert_eq!(account.available(), dec!(100));
        assert_eq!(account.total(), dec!(100));
        assert!(!account.is_locked());
    }

    #[test]
    fn test_can_chargeback_disputed_deposit() {
        let mut account = Account::new(1);

        account.deposit(23, dec!(123.23)).unwrap();
        account.dispute(23).unwrap();
        account.chargeback(23).unwrap();

        assert_eq!(account.held(), dec!(0));
        assert_eq!(account.available(), dec!(0));
        assert_eq!(account.total(), dec!(0));
        assert!(account.is_locked());

        // ensure no further transactions can be executed
        assert_eq!(
            format!("{:?}", account.dispute(0).unwrap_err()),
            "InvalidOperation(\"Attempt to execute a transaction on locked account [1].\")"
        );

        assert_eq!(
            format!("{:?}", account.chargeback(0).unwrap_err()),
            "InvalidOperation(\"Attempt to execute a transaction on locked account [1].\")"
        );

        assert_eq!(
            format!("{:?}", account.resolve(0).unwrap_err()),
            "InvalidOperation(\"Attempt to execute a transaction on locked account [1].\")"
        );

        assert_eq!(
            format!("{:?}", account.deposit(0, dec!(0)).unwrap_err()),
            "InvalidOperation(\"Attempt to execute a transaction on locked account [1].\")"
        );

        assert_eq!(
            format!("{:?}", account.withdraw(0, dec!(0)).unwrap_err()),
            "InvalidOperation(\"Attempt to execute a transaction on locked account [1].\")"
        );
    }

    #[test]
    fn test_can_chargeback_disputed_withdrawal() {
        let mut account = Account::new(1);

        account.deposit(22, dec!(123.23)).unwrap();
        account.withdraw(23, dec!(100)).unwrap();
        account.dispute(23).unwrap();
        account.chargeback(23).unwrap();

        assert_eq!(account.held(), dec!(0));
        assert_eq!(account.available(), dec!(123.23));
        assert_eq!(account.total(), dec!(123.23));
        assert!(account.is_locked());
    }
}
