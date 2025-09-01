//! stock-livedata
//! Work with stock data and analyse and predict stuff

use finalytics::prelude::TickerData;
use log::{LevelFilter, info};
//use polars::prelude::*;
use simplelog::{ColorChoice, CombinedLogger, Config, TermLogger, TerminalMode, WriteLogger};
use std::error::Error;
use std::fs::File;

use finalytics::prelude::{DataTable, DataTableDisplay, DataTableFormat, StatementFrequency, StatementType, Interval, Portfolio, PortfolioCharts, ObjectiveFunction};

mod app;
pub mod components;
mod dashboards;
pub mod server;
pub mod tools;
use crate::tools::{financialsProps, newsProps, optionsProps};

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

    let active_tab: usize = 1;
    let symbols = vec![ "AAPL", "ADBE", "AMD", "ARM", "BNP", "BYD", "DELL", "ENR", "GOOGL", "GTLB", "HPE", "MSFT", "MU", "NVDA", "RHM", "SMCI", "META", "DSY", "IBM", "BIDU", "SAP", "OKTA", "NET", "OVH", "IFX", "INTC", "NOW", "YSN", "SSTK", "VRNS" ];
    let portfolio = Portfolio::builder()
            .ticker_symbols(symbols)
            .benchmark_symbol("^GSPC")
            .start_date("2025-08-01")
            .end_date("2025-08-31")
            .interval(Interval::OneDay)
            .confidence_level(0.95)
            .risk_free_rate(0.02)
            .objective_function(ObjectiveFunction::MaxSharpe)
            .build()
            .await
            .map_err(|e| format!("PortfolioBuilder error: {e}"));

    let chart = 
        match portfolio {
        Ok(portfolio) => {
            let chart = match active_tab {
                1 => portfolio.optimization_chart(None, None).map_err(|e| format!("Optimization Chart error: {e}")).unwrap().to_html(),
                2 => portfolio.performance_chart(None, None).map_err(|e| format!("Performance Chart error: {e}")).unwrap().to_html(),
                3 => portfolio.performance_stats_table().await.map_err(|e| format!("Performance Stats Table error: {e}")).unwrap().to_html().unwrap(),
                4 => portfolio.returns_table().map_err(|e| format!("Returns Table error: {e}")).unwrap().to_html().unwrap(),
                5 => portfolio.returns_chart(None, None).map_err(|e| format!("Returns Chart error: {e}")).unwrap().to_html(),
                6 => portfolio.returns_matrix(None, None).map_err(|e| format!("Returns Matrix error: {e}")).unwrap().to_html(),
                _ => "".to_string(),
            };

            Ok(chart)
        }
        Err(e) => {
            log::error!("Failed to get portfolio: {e}");
            Err(e)
        }
    };
    if chart.is_ok() {
        print!("{}", chart.unwrap());
    }
    let start_date = match chrono::NaiveDateTime::parse_from_str("2025-08-01 00:00:00", "%Y-%m-%d %H:%M:%S") {
        Ok(d) => d,
        Err(error) => {
            log::error!("Failed to convert string to datetime! {}", error);
            panic!("Ending the program as this is required!");
        }
    };
    let end_date = match chrono::NaiveDateTime::parse_from_str("2025-08-31 23:59:59", "%Y-%m-%d %H:%M:%S") {
        Ok(d) => d,
        Err(error) => {
            log::error!("Failed to convert string to datetime! {}", error);
            panic!("Ending the program as this is required!");
        }
    };
    let symbols = vec![ "AAPL", "ADBE", "AMD", "ARM", "BNP", "BYD", "DELL", "ENR", "GOOGL", "GTLB", "HPE", "MSFT", "MU", "NVDA", "RHM", "SMCI", "META", "DSY", "IBM", "BIDU", "SAP", "OKTA", "NET", "OVH", "IFX", "INTC", "NOW", "YSN", "SSTK", "VRNS" ];
    
    let mut tickers = Vec::new();
    let start_date = "2025-08-01".to_string();
    let end_date = chrono::Utc::now().date_naive().to_string();

    for i in 0..symbols.len() {
        let stock_symbol = symbols[i].to_string();
        let ticker = finalytics::models::ticker::TickerBuilder::new()
            .ticker(&stock_symbol)
            .start_date(&start_date)
            .end_date(&end_date)
            .interval(Interval::TwoMinutes)
            .build();

        let df = ticker.get_chart().await.inspect(|x| println!("original: {x}")).expect("extraction of data failed.");
        let table = df.to_datatable("ohlcv", true, DataTableFormat::Number);
        let html = table.to_html()?;
        //println!("{}", html);
        tickers.push(ticker);
        //table.show()?;

        /*
        let fin = match crate::tools::financials(financialsProps {symbolstr: stock_symbol.clone(), start_date_str: start_date.clone(), end_date_str: end_date.clone()}) {
            Ok(ret) => ret,
            Err(error) => {
                log::error!("Failed to create financials view: {}", error);
                return Ok(());
            }
        };

        let news = match crate::tools::news(newsProps {symbolstr: stock_symbol.clone()}) {
            Ok(ret) => ret,
            Err(error) => {
                log::error!("Failed to create news view: {}", error);
                return Ok(());
            }
        };

        let opt = match crate::tools::options(optionsProps {symbolstr: stock_symbol.clone()}) {
            Ok(ret) => ret,
            Err(error) => {
                log::error!("Failed to create options view: {}", error);
                return Ok(());
            },
        };

        let perf = match crate::tools::performance() {
            Ok(ret) => ret,
            Err(error) => {
                log::error!("Failed to create performance view: {}", error);
                return Ok(());
            },
        };
        
        let screener = match crate::tools::screener() {
            Ok(ret) => ret,
            Err(error) => {
                log::error!("Failed to create screener view: {}", error);
                return Ok(());
            },
        };

        let portfolio = match crate::tools::portfolio() {
            Ok(ret) => ret,
            Err(error) => {
                log::error!("Failed to create portfolio view: {}", error);
                return Ok(());
            },
        };
        */
        //ticker.get_financials(statement_type, frequency)

    }
    Ok(())
}
