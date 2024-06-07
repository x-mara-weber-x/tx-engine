use rust_decimal::Decimal;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TransactionKind {
    Withdrawal(Decimal),
    Deposit(Decimal),
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Transaction {
    kind: TransactionKind,
    client_id: u16,
    tx_id: u32,
}

impl Transaction {
    pub fn new_charge_back(tx_id: u32, client_id: u16) -> Self {
        Transaction {
            kind: TransactionKind::Chargeback,
            client_id,
            tx_id,
        }
    }

    pub fn new_dispute(tx_id: u32, client_id: u16) -> Self {
        Transaction {
            kind: TransactionKind::Dispute,
            client_id,
            tx_id,
        }
    }

    pub fn new_resolve(tx_id: u32, client_id: u16) -> Self {
        Transaction {
            kind: TransactionKind::Resolve,
            client_id,
            tx_id,
        }
    }

    pub fn new_deposit(tx_id: u32, client_id: u16, amount: Decimal) -> Self {
        Transaction {
            kind: TransactionKind::Deposit(amount),
            client_id,
            tx_id,
        }
    }

    pub fn new_withdrawal(tx_id: u32, client_id: u16, amount: Decimal) -> Self {
        Transaction {
            kind: TransactionKind::Withdrawal(amount),
            client_id,
            tx_id,
        }
    }

    pub fn kind(&self) -> TransactionKind {
        self.kind
    }

    pub fn client_id(&self) -> u16 {
        self.client_id
    }
    pub fn tx_id(&self) -> u32 {
        self.tx_id
    }
}
