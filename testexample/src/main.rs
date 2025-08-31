//! stock-livedata
//! Work with stock data and analyse and predict stuff

use finalytics::prelude::TickerData;
use log::LevelFilter;
//use polars::prelude::*;
use simplelog::{ColorChoice, CombinedLogger, Config, TermLogger, TerminalMode, WriteLogger};
use std::error::Error;
use std::fs::File;

use finalytics::prelude::{DataTable, DataTableDisplay, DataTableFormat, StatementFrequency, StatementType, Interval};


/// convert an OsString (from PathBuf) to a usable String
pub fn osstr_to_string(osstr: std::ffi::OsString) -> String {
    match osstr.to_str() {
        Some(str) => return str.to_string(),
        None => {}
    }
    String::new()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let logfile = "stock_analysis.txt".to_string();

    CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Error,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        ),
        WriteLogger::new(
            LevelFilter::Debug,
            Config::default(),
            File::create(&logfile).unwrap(),
        ),
    ])
    .unwrap();

    let start_date = match chrono::NaiveDateTime::parse_from_str("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S") {
        Ok(d) => d,
        Err(error) => {
            log::error!("Failed to convert string to datetime! {}", error);
            panic!("Ending the program as this is required!");
        }
    };
    let end_date = match chrono::NaiveDateTime::parse_from_str("2100-01-01 00:00:00", "%Y-%m-%d %H:%M:%S") {
        Ok(d) => d,
        Err(error) => {
            log::error!("Failed to convert string to datetime! {}", error);
            panic!("Ending the program as this is required!");
        }
    };
    let symbols = vec![ "AAPL", "ADBE", "AMD", "ARM", "BNP", "BYD", "DELL", "ENR", "GOOGL", "GTLB", "HPE", "MSFT", "MU", "NVDA", "RHM", "SMCI", "META", "DSY", "IBM", "BIDU", "SAP", "OKTA", "NET", "OVH", "IFX", "INTC", "NOW", "YSN", "SSTK", "VRNS" ];
    
    let mut tickers = Vec::new();

    for i in 0..symbols.len() {
        let stock_symbol = symbols[i].to_string();
        let ticker = finalytics::models::ticker::TickerBuilder::new()
            .ticker(&stock_symbol)
            .start_date("1970-01-01")
            .end_date(&chrono::Utc::now().date_naive().to_string())
            .interval(Interval::TwoMinutes)
            .build();

        let df = ticker.get_chart().await.inspect(|x| println!("original: {x}")).expect("extraction of data failed.");
        let table = df.to_datatable("ohlcv", true, DataTableFormat::Number);
        let html = table.to_html()?;
        println!("{}", html);
        tickers.push(ticker);
        table.show()?;
        


        //ticker.get_financials(statement_type, frequency)

    }
    Ok(())
}
