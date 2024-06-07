use std::collections::HashMap;

use crate::tx::engine::account::{Account, AccountSummary};
use crate::tx::engine::result::TxResult;
use crate::tx::engine::transaction::{Transaction, TransactionKind};

pub struct TransactionEngine {
    accounts: HashMap<u16, Account>,
}

impl TransactionEngine {
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
        }
    }

    pub fn execute(&mut self, transaction: Transaction) -> TxResult<()> {
        let account = self
            .accounts
            .entry(transaction.client_id())
            .or_insert_with(|| Account::new(transaction.client_id()));

        match transaction.kind() {
            TransactionKind::Withdrawal(amount) => account.withdraw(transaction.tx_id(), amount),
            TransactionKind::Deposit(amount) => account.deposit(transaction.tx_id(), amount),
            TransactionKind::Dispute => account.dispute(transaction.tx_id()),
            TransactionKind::Resolve => account.resolve(transaction.tx_id()),
            TransactionKind::Chargeback => account.chargeback(transaction.tx_id()),
        }
    }

    pub fn account_summary(&self) -> Vec<AccountSummary> {
        let mut accounts = self
            .accounts
            .iter()
            .map(|(_, account)| account.summary())
            .collect::<Vec<_>>();

        accounts.sort_by(|a, b| a.id.cmp(&b.id));

        accounts
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use crate::tx::engine::account::AccountSummary;
    use crate::tx::engine::engine::TransactionEngine;
    use crate::tx::engine::transaction::Transaction;

    #[test]
    fn test_basic_happy_case() {
        let mut engine = TransactionEngine::new();

        engine
            .execute(Transaction::new_deposit(1, 2, dec!(12)))
            .unwrap();
        engine
            .execute(Transaction::new_deposit(2, 3, dec!(32)))
            .unwrap();
        engine
            .execute(Transaction::new_withdrawal(3, 2, dec!(1)))
            .unwrap();
        engine.execute(Transaction::new_dispute(3, 2)).unwrap();

        let accounts = engine.account_summary();
        assert_eq!(accounts.len(), 2);
        assert_eq!(
            accounts[0],
            AccountSummary {
                id: 2,
                available: dec!(12),
                held: dec!(-1),
                total: dec!(11),
                is_locked: false,
            }
        );
        assert_eq!(
            accounts[1],
            AccountSummary {
                id: 3,
                available: dec!(32),
                held: dec!(0),
                total: dec!(32),
                is_locked: false,
            }
        );
    }
}
