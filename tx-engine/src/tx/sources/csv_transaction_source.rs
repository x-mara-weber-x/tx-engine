use std::fmt::Display;

use async_trait::async_trait;
use csv_async::{AsyncReader, StringRecord};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::io::AsyncRead;

use crate::tx::engine::result::{TxError, TxResult};
use crate::tx::engine::transaction::Transaction;
use crate::tx::sources::transaction_source::TransactionSource;

pub struct CsvTransactionSource<R>
where
    R: AsyncRead + Unpin + Send,
{
    reader: AsyncReader<R>,
    indices: CsvHeaderIndices,
}

struct CsvHeaderIndices {
    pub type_index: usize,
    pub tx_index: usize,
    pub client_index: usize,
    pub amount_index: usize,
}

impl<R> CsvTransactionSource<R>
where
    R: AsyncRead + Unpin + Send,
{
    pub async fn from_reader(source: R) -> TxResult<Self> {
        let mut reader = AsyncReader::from_reader(source);
        let headers = reader.headers().await.unwrap();
        let mut type_index = None;
        let mut client_index = None;
        let mut tx_index = None;
        let mut amount_index = None;

        for i in 0..headers.len() {
            if let Some(header) = headers.get(i) {
                match header.trim().to_lowercase().as_str() {
                    "type" => type_index = Some(i),
                    "client" => client_index = Some(i),
                    "tx" => tx_index = Some(i),
                    "amount" => amount_index = Some(i),
                    _ => {}
                }
            }
        }

        let indices = CsvHeaderIndices {
            type_index: type_index.ok_or(Self::error_missing_column("type"))?,
            tx_index: tx_index.ok_or(Self::error_missing_column("tx"))?,
            client_index: client_index.ok_or(Self::error_missing_column("client"))?,
            amount_index: amount_index.ok_or(Self::error_missing_column("amount"))?,
        };

        Ok(Self { reader, indices })
    }

    fn error_missing_column(column: &str) -> TxError {
        TxError::InvalidArgument(format!("Expected a column named [{}].", column))
    }

    fn missing_value_error(&self, column: &str) -> TxError {
        TxError::InvalidArgument(format!(
            "Expected a value for column [{}] ({}).",
            column,
            self.position_to_string()
        ))
    }

    fn parse_tx_id(&self, value: &str) -> TxResult<u32> {
        value
            .trim()
            .to_lowercase()
            .as_str()
            .parse::<u32>()
            .map_err(|e| self.invalid_value_error("tx", value, e))
    }

    fn parse_client_id(&self, value: &str) -> TxResult<u16> {
        value
            .trim()
            .to_lowercase()
            .as_str()
            .parse::<u16>()
            .map_err(|e| self.invalid_value_error("client", value, e))
    }

    fn parse_amount(&self, value: &str) -> TxResult<Decimal> {
        let amount = Decimal::from_str_exact(value.trim().to_lowercase().as_str())
            .map_err(|e| self.invalid_value_error("amount", value, e))?;

        if amount < dec!(0) {
            Err(self.invalid_value_error("amount", value, "Negative values are not allowed"))
        } else {
            Ok(amount)
        }
    }

    fn invalid_value_error<E: Display>(&self, column: &str, value: &str, error: E) -> TxError {
        TxError::InvalidArgument(format!(
            "Could not parse value [{}] for column [{}]: {} ({}).",
            value,
            column,
            error,
            self.position_to_string()
        ))
    }

    fn io_error<E: Display>(&self, error: E) -> TxError {
        TxError::IoError(format!(
            "Unexpected I/O error while reading CSV record: {} ({}).",
            error,
            self.position_to_string()
        ))
    }

    fn position_to_string(&self) -> String {
        format!(
            "line: {}, byte: {}, record: {}",
            self.reader.position().line(),
            self.reader.position().byte(),
            self.reader.position().record()
        )
    }
}

#[async_trait]
impl<R> TransactionSource for CsvTransactionSource<R>
where
    R: AsyncRead + Unpin + Send,
{
    async fn read(&mut self) -> TxResult<Option<Transaction>> {
        let mut csv_record: StringRecord = StringRecord::new();
        if !self
            .reader
            .read_record(&mut csv_record)
            .await
            .map_err(|e| self.io_error(e))?
        {
            return Ok(None);
        }

        let kind_str = csv_record
            .get(self.indices.type_index)
            .ok_or(self.missing_value_error("type"))?;
        let tx_id_str = csv_record
            .get(self.indices.tx_index)
            .ok_or(self.missing_value_error("tx"))?;
        let client_id_str = csv_record
            .get(self.indices.client_index)
            .ok_or(self.missing_value_error("client"))?;
        let amount_str = csv_record.get(self.indices.amount_index);
        let tx_id = self.parse_tx_id(tx_id_str)?;
        let client_id = self.parse_client_id(client_id_str)?;

        match (kind_str.trim().to_lowercase().as_str(), amount_str) {
            ("deposit", Some(amount_str)) => Ok(Some(Transaction::new_deposit(
                tx_id,
                client_id,
                self.parse_amount(amount_str)?,
            ))),
            ("withdrawal", Some(amount_str)) => Ok(Some(Transaction::new_withdrawal(
                tx_id,
                client_id,
                self.parse_amount(amount_str)?,
            ))),
            ("dispute", _) => Ok(Some(Transaction::new_dispute(tx_id, client_id))),
            ("resolve", _) => Ok(Some(Transaction::new_resolve(tx_id, client_id))),
            ("chargeback", _) => Ok(Some(Transaction::new_charge_back(tx_id, client_id))),
            _ => Err(self.invalid_value_error("type", kind_str, "Unsupported value")),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::*;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use tokio::fs::File;

    use crate::test_resource_path;
    use crate::tx::engine::transaction::Transaction;
    use crate::tx::sources::csv_transaction_source::CsvTransactionSource;
    use crate::tx::sources::transaction_source::TransactionSource;

    #[tokio::test]
    async fn test_can_correctly_parse_supplied_demo_file() {
        let mut csv_source = CsvTransactionSource::from_reader(
            File::open(test_resource_path!("sources/valid/given-example.csv"))
                .await
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            csv_source.read().await.unwrap().unwrap(),
            Transaction::new_deposit(1, 1, dec!(1.0))
        );
        assert_eq!(
            csv_source.read().await.unwrap().unwrap(),
            Transaction::new_deposit(2, 2, dec!(2.0))
        );
        assert_eq!(
            csv_source.read().await.unwrap().unwrap(),
            Transaction::new_deposit(3, 1, dec!(2.0))
        );
        assert_eq!(
            csv_source.read().await.unwrap().unwrap(),
            Transaction::new_withdrawal(4, 1, dec!(1.5))
        );
        assert_eq!(
            csv_source.read().await.unwrap().unwrap(),
            Transaction::new_withdrawal(5, 2, dec!(3.0))
        );
        assert!(
            csv_source.read().await.unwrap().is_none(),
            "Did not expect any further record."
        );
    }

    #[tokio::test]
    async fn test_can_correctly_parse_variations_01() {
        let mut csv_source = CsvTransactionSource::from_reader(
            File::open(test_resource_path!("sources/valid/variations-01.csv"))
                .await
                .unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            csv_source.read().await.unwrap().unwrap(),
            Transaction::new_deposit(109183, 183, dec!(1.029837))
        );
        assert_eq!(
            csv_source.read().await.unwrap().unwrap(),
            Transaction::new_deposit(2, 2, dec!(0))
        );
        assert_eq!(
            csv_source.read().await.unwrap().unwrap(),
            Transaction::new_deposit(3, 65535, dec!(2.0))
        );
        assert_eq!(
            csv_source.read().await.unwrap().unwrap(),
            Transaction::new_withdrawal(4294967295, 1, dec!(1792837984777619.5189873))
        );
        assert_eq!(
            csv_source.read().await.unwrap().unwrap(),
            Transaction::new_withdrawal(5, 2, dec!(3.0))
        );
        assert_eq!(
            csv_source.read().await.unwrap().unwrap(),
            Transaction::new_charge_back(1, 238)
        );
        assert_eq!(
            csv_source.read().await.unwrap().unwrap(),
            Transaction::new_resolve(98, 0)
        );
        assert_eq!(
            csv_source.read().await.unwrap().unwrap(),
            Transaction::new_dispute(3787, 1092)
        );
        assert!(
            csv_source.read().await.unwrap().is_none(),
            "Did not expect any further record."
        );
    }

    #[rstest]
    #[case("0", 0)]
    #[case(" 1", 1)]
    #[case("    83     ", 83)]
    #[case(" +2 ", 2)]
    #[case(" 4294967295 ", 4294967295)]
    #[tokio::test]
    async fn test_parse_tx_id_success(#[case] given_value: &str, #[case] expected_result: u32) {
        let csv_source = CsvTransactionSource::from_reader("type,tx,client,amount".as_bytes())
            .await
            .unwrap();

        assert_eq!(
            csv_source.parse_tx_id(given_value).unwrap(),
            expected_result
        );
    }

    #[rstest]
    #[case("0.0", "InvalidArgument(\"Could not parse value [0.0] for column [tx]: invalid digit found in string (line: 1, byte: 21, record: 1).\")")]
    #[case("hello", "InvalidArgument(\"Could not parse value [hello] for column [tx]: invalid digit found in string (line: 1, byte: 21, record: 1).\")")]
    #[case(" -1 ", "InvalidArgument(\"Could not parse value [ -1 ] for column [tx]: invalid digit found in string (line: 1, byte: 21, record: 1).\")")]
    #[case(" 4294967296 ", "InvalidArgument(\"Could not parse value [ 4294967296 ] for column [tx]: number too large to fit in target type (line: 1, byte: 21, record: 1).\")")]
    #[tokio::test]
    async fn test_parse_tx_id_failures(
        #[case] given_value: &str,
        #[case] expected_error_message: &str,
    ) {
        let csv_source = create_empty_csv_source().await;
        let actual_error_message =
            format!("{:?}", csv_source.parse_tx_id(given_value).unwrap_err());

        assert_eq!(actual_error_message, expected_error_message);
    }

    #[rstest]
    #[case("0", 0)]
    #[case(" 1", 1)]
    #[case("    83     ", 83)]
    #[case(" +2 ", 2)]
    #[case(" 65535 ", 65535)]
    #[tokio::test]
    async fn test_parse_client_id_success(#[case] given_value: &str, #[case] expected_result: u16) {
        let csv_source = CsvTransactionSource::from_reader("type,tx,client,amount".as_bytes())
            .await
            .unwrap();

        assert_eq!(
            csv_source.parse_client_id(given_value).unwrap(),
            expected_result
        );
    }

    #[rstest]
    #[case("0.0", "InvalidArgument(\"Could not parse value [0.0] for column [client]: invalid digit found in string (line: 1, byte: 21, record: 1).\")")]
    #[case("hello", "InvalidArgument(\"Could not parse value [hello] for column [client]: invalid digit found in string (line: 1, byte: 21, record: 1).\")")]
    #[case(" -1 ", "InvalidArgument(\"Could not parse value [ -1 ] for column [client]: invalid digit found in string (line: 1, byte: 21, record: 1).\")")]
    #[case(" 65536 ", "InvalidArgument(\"Could not parse value [ 65536 ] for column [client]: number too large to fit in target type (line: 1, byte: 21, record: 1).\")")]
    #[tokio::test]
    async fn test_parse_client_id_failures(
        #[case] given_value: &str,
        #[case] expected_error_message: &str,
    ) {
        let csv_source = create_empty_csv_source().await;
        let actual_error_message =
            format!("{:?}", csv_source.parse_client_id(given_value).unwrap_err());

        assert_eq!(actual_error_message, expected_error_message);
    }

    #[rstest]
    #[case("0", dec!(0))]
    #[case(" 1", dec!(1))]
    #[case("    83     ", dec!(83))]
    #[case(" +2 ", dec!(2))]
    #[case(" 65535.2873 ", dec!(65535.2873))]
    #[tokio::test]
    async fn test_parse_amount_success(
        #[case] given_value: &str,
        #[case] expected_result: Decimal,
    ) {
        let csv_source = CsvTransactionSource::from_reader("type,tx,client,amount".as_bytes())
            .await
            .unwrap();

        assert_eq!(
            csv_source.parse_amount(given_value).unwrap(),
            expected_result
        );
    }

    #[rstest]
    #[case("hello", "InvalidArgument(\"Could not parse value [hello] for column [amount]: Invalid decimal: unknown character (line: 1, byte: 21, record: 1).\")")]
    #[case(" -1 ", "InvalidArgument(\"Could not parse value [ -1 ] for column [amount]: Negative values are not allowed (line: 1, byte: 21, record: 1).\")")]
    #[case(" -1.2902 ", "InvalidArgument(\"Could not parse value [ -1.2902 ] for column [amount]: Negative values are not allowed (line: 1, byte: 21, record: 1).\")")]
    #[case(" -1e2 ", "InvalidArgument(\"Could not parse value [ -1e2 ] for column [amount]: Invalid decimal: unknown character (line: 1, byte: 21, record: 1).\")")]
    #[tokio::test]
    async fn test_parse_amount_failures(
        #[case] given_value: &str,
        #[case] expected_error_message: &str,
    ) {
        let csv_source = create_empty_csv_source().await;
        let actual_error_message =
            format!("{:?}", csv_source.parse_amount(given_value).unwrap_err());

        assert_eq!(actual_error_message, expected_error_message);
    }

    #[rstest]
    #[case(
        "type,client,amount,other column",
        "InvalidArgument(\"Expected a column named [tx].\")"
    )]
    #[case(
        "col1,tx,col2,client,amount,col3",
        "InvalidArgument(\"Expected a column named [type].\")"
    )]
    #[case(
        "type,tx,amount,",
        "InvalidArgument(\"Expected a column named [client].\")"
    )]
    #[case(
        ",type,client,tx",
        "InvalidArgument(\"Expected a column named [amount].\")"
    )]
    #[tokio::test]
    async fn test_constructor_failures(
        #[case] given_csv: &str,
        #[case] expected_error_message: &str,
    ) {
        let error = CsvTransactionSource::from_reader(given_csv.as_bytes())
            .await
            .err()
            .unwrap();
        let actual_error_message = format!("{:?}", error);

        assert_eq!(actual_error_message, expected_error_message);
    }

    async fn create_empty_csv_source<'a>() -> CsvTransactionSource<&'a [u8]> {
        CsvTransactionSource::from_reader("type,tx,client,amount".as_bytes())
            .await
            .unwrap()
    }
}
