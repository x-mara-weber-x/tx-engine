use std::env;
use std::io::{stdout, Write};

use tokio::fs::File;

use tx_engine::tx::engine::engine::TransactionEngine;
use tx_engine::tx::engine::result::{TxError, TxResult};
use tx_engine::tx::reports::csv_account_report::CsvAccountReport;
use tx_engine::tx::sources::csv_transaction_source::CsvTransactionSource;
use tx_engine::tx::sources::transaction_source::TransactionSource;

#[tokio::main]
async fn main() {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 2 {
        eprintln!("[ERROR]: This application requires a path to a CSV file with transaction data as argument.");
        return;
    }

    if let Err(err) = run(args[1].as_str(), stdout()).await {
        eprintln!("[ERROR]: {:?}", err);
        return;
    }
}

async fn run<W>(csv_source_path: &str, output_sink: W) -> TxResult<W>
where
    W: Write + Send + Unpin,
{
    let csv_source_file = File::open(csv_source_path).await.map_err(|e| {
        TxError::IoError(format!(
            "Unable to open source file [{}]: {}",
            csv_source_path, e
        ))
    })?;
    let mut csv_source = CsvTransactionSource::from_reader(csv_source_file).await?;
    let mut engine = TransactionEngine::new();
    while let Some(record) = csv_source.read().await? {
        engine.execute(record)?;
    }

    let mut csv_report = CsvAccountReport::from_writer(output_sink)?;
    engine
        .account_summary()
        .iter()
        .try_for_each(|account| csv_report.write_account(account))?;

    csv_report.flush()
}

#[cfg(test)]
mod tests {
    use tx_engine::test_resource_path;

    use crate::run;

    #[tokio::test]
    async fn test_happy_path() {
        let csv_report = String::from_utf8(
            run(
                test_resource_path!("sources/valid/given-example.csv"),
                Vec::<u8>::new(),
            )
            .await
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            csv_report.as_str(),
            "client,available,held,total,locked\n1,1.5,0,1.5,false\n2,1.0,0,1.0,false\n"
        );
    }
}
