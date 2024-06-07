use async_trait::async_trait;

use crate::tx::engine::result::TxResult;
use crate::tx::engine::transaction::Transaction;

#[async_trait]
pub trait TransactionSource {
    /// Asynchronously reads a single transaction record from the source. `None` is returned if no
    /// more data is available (i.e. all successive calls will also be `None`). Errors are not
    /// necessarily terminal and it depends on the nature of the error if successive calls can
    /// ever succeed.
    async fn read(&mut self) -> TxResult<Option<Transaction>>;
}
