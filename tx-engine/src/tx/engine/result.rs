pub type TxResult<T> = Result<T, TxError>;

#[derive(Debug, Clone)]
pub enum TxError {
    InvalidArgument(String),
    InvalidOperation(String),
    IoError(String),
}
