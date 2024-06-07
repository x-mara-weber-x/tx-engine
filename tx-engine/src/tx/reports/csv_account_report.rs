use std::fmt::Display;
use std::io::Write;

use csv::Writer;
use rust_decimal::Decimal;

use crate::tx::engine::account::AccountSummary;
use crate::tx::engine::result::{TxError, TxResult};

pub struct CsvAccountReport<W>
where
    W: Write + Unpin + Send,
{
    writer: Option<Writer<W>>,
}

impl<W> CsvAccountReport<W>
where
    W: Write + Unpin + Send,
{
    pub fn from_writer(sink: W) -> TxResult<Self> {
        let mut writer = Writer::from_writer(sink);

        writer
            .write_record(vec!["client", "available", "held", "total", "locked"])
            .map_err(|e| Self::io_error(e))?;

        Ok(Self {
            writer: Some(writer),
        })
    }

    fn io_error<E: Display>(error: E) -> TxError {
        TxError::IoError(format!(
            "Unexpected I/O error while writing CSV record: {}",
            error
        ))
    }

    fn use_after_flush_error() -> TxError {
        TxError::InvalidOperation(
            "The report was already written, no further action possible.".to_string(),
        )
    }

    fn serialize_u16(value: u16) -> String {
        value.to_string()
    }

    fn serialize_decimal(value: Decimal) -> String {
        value.round_dp(4).to_string()
    }

    fn serialize_bool(value: bool) -> String {
        (if value { "true" } else { "false" }).to_string()
    }

    pub fn write_account(&mut self, account: &AccountSummary) -> TxResult<()> {
        self.writer
            .as_mut()
            .ok_or(Self::use_after_flush_error())?
            .write_record(vec![
                Self::serialize_u16(account.id),
                Self::serialize_decimal(account.available),
                Self::serialize_decimal(account.held),
                Self::serialize_decimal(account.total),
                Self::serialize_bool(account.is_locked),
            ])
            .map_err(|e| Self::io_error(e))?;

        Ok(())
    }

    pub fn flush(&mut self) -> TxResult<W> {
        let mut writer = self.writer.take().ok_or(Self::use_after_flush_error())?;

        writer.flush().map_err(|e| Self::io_error(e))?;

        writer.into_inner().map_err(|e| Self::io_error(e))
    }
}

#[cfg(test)]
mod tests {
    use rstest_macros::rstest;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::tx::engine::account::Account;
    use crate::tx::reports::csv_account_report::CsvAccountReport;

    #[tokio::test]
    async fn test_no_accounts() {
        let mut report = CsvAccountReport::from_writer(Vec::new()).unwrap();
        let csv_output = String::from_utf8(report.flush().unwrap()).unwrap();
        assert_eq!(csv_output, "client,available,held,total,locked\n");
    }

    #[tokio::test]
    async fn test_simple_accounts() {
        let mut report = CsvAccountReport::from_writer(Vec::new()).unwrap();
        let mut account_a = Account::new(1);
        let mut account_b = Account::new(2);

        account_a.deposit(2, dec!(13.28973498)).unwrap();
        account_a.deposit(3, dec!(1)).unwrap();
        account_a.dispute(3).unwrap();
        account_a.chargeback(3).unwrap();

        account_b.deposit(3, dec!(13898273)).unwrap();

        report.write_account(&account_a.summary()).unwrap();
        report.write_account(&account_b.summary()).unwrap();

        let csv_output = String::from_utf8(report.flush().unwrap()).unwrap();
        assert_eq!(csv_output, "client,available,held,total,locked\n1,13.2897,0,13.2897,true\n2,13898273,0,13898273,false\n");
    }

    #[rstest]
    #[case(dec!(0), "0")]
    #[case(dec!(0.0), "0.0")]
    #[case(dec!(0.000001), "0.0000")]
    #[case(dec!(0.00009), "0.0001")]
    #[case(dec!(0.0002), "0.0002")]
    #[case(dec!(12893273892792837979823792830), "12893273892792837979823792830")]
    #[case(dec!(1289327389279283797982.3792830), "1289327389279283797982.3793")]
    fn test_decimal_formatting(#[case] given_value: Decimal, #[case] expected_result: &str) {
        assert_eq!(
            CsvAccountReport::<Vec<u8>>::serialize_decimal(given_value).as_str(),
            expected_result
        );
    }

    #[rstest]
    #[case(0, "0")]
    #[case(65535, "65535")]
    fn test_u16_formatting(#[case] given_value: u16, #[case] expected_result: &str) {
        assert_eq!(
            CsvAccountReport::<Vec<u8>>::serialize_u16(given_value).as_str(),
            expected_result
        );
    }

    #[rstest]
    #[case(true, "true")]
    #[case(false, "false")]
    fn test_bool_formatting(#[case] given_value: bool, #[case] expected_result: &str) {
        assert_eq!(
            CsvAccountReport::<Vec<u8>>::serialize_bool(given_value).as_str(),
            expected_result
        );
    }
}
