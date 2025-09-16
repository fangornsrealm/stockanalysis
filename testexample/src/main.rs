//! stock-livedata
//! Work with stock data and analyse and predict stuff

use chrono::NaiveDateTime;
use dioxus::prelude::IntoDynNode;
use stockanalysis::prelude::TickerData;
use log::{LevelFilter};
//use polars::prelude::*;
use simplelog::{ColorChoice, CombinedLogger, Config, TermLogger, TerminalMode, WriteLogger};
use std::error::Error;
use std::fs::File;

use stockanalysis::prelude::{DataTable, DataTableDisplay, DataTableFormat, StatementFrequency, StatementType, Interval, Portfolio, PortfolioCharts, ObjectiveFunction};

mod app;
mod components;
mod dashboards;
mod server;
mod tools;

/// convert an OsString (from PathBuf) to a usable String
pub fn osstr_to_string(osstr: std::ffi::OsString) -> String {
    match osstr.to_str() {
        Some(str) => return str.to_string(),
        None => {}
    }
    String::new()
}

pub async fn test_portfolio(portfolio: Result<Portfolio, String>) -> Result<(), Box<dyn Error>> {

    let testportfolio = portfolio.clone();
    let opt_chart = 
        match testportfolio {
        Ok(portfolio) => {
            let chart = 
                portfolio.optimization_chart(None, None).map_err(|e| format!("Optimization Chart error: {e}")).unwrap().to_html();

            Ok(chart)
        },
        Err(e) => {
            log::error!("Failed to get portfolio: {e}");
            Err(e)
        }
    };
    match opt_chart {
        Ok(chart) => std::fs::write("opt_chart.html", &chart).expect("Should be able to write to file"),
        Err(e) => {
            log::error!("Failed to get chart for portfolio: {e}");
            return Ok(());
        }
    }
    let testportfolio = portfolio.clone();
    let perf_chart = 
        match testportfolio {
        Ok(portfolio) => {
            let chart = 
                portfolio.performance_chart(None, None).map_err(|e| format!("Performance Chart error: {e}")).unwrap().to_html();

            Ok(chart)
        }
        Err(e) => {
            log::error!("Failed to get portfolio: {e}");
            Err(e)
        }
    };
    match perf_chart {
        Ok(chart) => std::fs::write("perf_chart.html", &chart).expect("Should be able to write to file"),
        Err(e) => {
            log::error!("Failed to get chart for portfolio: {e}");
            return Ok(());
        }
    }
    let testportfolio = portfolio.clone();
    let perf_stats_chart = 
        match testportfolio {
        Ok(portfolio) => {
            let chart = 
                portfolio.performance_stats_table().await.map_err(|e| format!("Performance Stats Table error: {e}")).unwrap().to_html().unwrap();            Ok(chart)
        }
        Err(e) => {
            log::error!("Failed to get portfolio: {e}");
            Err(e)
        }
    };
    match perf_stats_chart {
        Ok(chart) => std::fs::write("perf_stats_chart.html", &chart).expect("Should be able to write to file"),
        Err(e) => {
            log::error!("Failed to get chart for portfolio: {e}");
            return Ok(());
        }
    }
    let testportfolio = portfolio.clone();
    let returns_table = 
        match testportfolio {
        Ok(portfolio) => {
            let chart = 
                portfolio.returns_table().map_err(|e| format!("Returns Table error: {e}")).unwrap().to_html().unwrap();            Ok(chart)
        }
        Err(e) => {
            log::error!("Failed to get portfolio: {e}");
            Err(e)
        }
    };
    match returns_table {
        Ok(chart) => std::fs::write("returns_table.html", &chart).expect("Should be able to write to file"),
        Err(e) => {
            log::error!("Failed to get chart for portfolio: {e}");
            return Ok(());
        }
    }
    let testportfolio = portfolio.clone();
    let returns_chart = 
        match testportfolio {
        Ok(portfolio) => {
            let chart = 
                portfolio.returns_chart(None, None).map_err(|e| format!("Returns Chart error: {e}")).unwrap().to_html();
            Ok(chart)
        }
        Err(e) => {
            log::error!("Failed to get portfolio: {e}");
            Err(e)
        }
    };
    match returns_chart {
        Ok(chart) => std::fs::write("returns_chart.html", &chart).expect("Should be able to write to file"),
        Err(e) => {
            log::error!("Failed to get chart for portfolio: {e}");
            return Ok(());
        }
    }
    let testportfolio = portfolio.clone();
    let returns_matrix = 
        match testportfolio {
        Ok(portfolio) => {
            let chart = 
                portfolio.returns_matrix(None, None).map_err(|e| format!("Returns Matrix error: {e}")).unwrap().to_html();
            Ok(chart)
        }
        Err(e) => {
            log::error!("Failed to get portfolio: {e}");
            Err(e)
        }
    };
    match returns_matrix {
        Ok(chart) => std::fs::write("returns_matrix.html", &chart).expect("Should be able to write to file"),
        Err(e) => {
            log::error!("Failed to get chart for portfolio: {e}");
            return Ok(());
        }
    }
    Ok(())
}

pub trait ForDisplay {
    type Out: IntoDynNode;

    fn for_display(&self) -> Self::Out;
}

impl<T: ToString> ForDisplay for Option<T> {
    type Out = Option<String>;

    fn for_display(&self) -> Self::Out {
        self.as_ref().map(ToString::to_string)
    }
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
    
    let sql_connection = stockanalysis::data::sql::connect();
    let symbolsstrings = stockanalysis::data::sql::active_symbols(sql_connection.clone());
    //let symbolsstrings = ["NVDA".to_string(), "AMD".to_string()];
    let symbols: Vec<&str> = symbolsstrings.iter().map(|s| &**s).collect();
    //let symbols = vec![ "AAPL", "ADBE", "AMD", "ARM", "BNP", "BYD", "DELL", "ENR", "GOOGL", "GTLB", "HPE", "MSFT", "MU", "NVDA", "RHM", "SMCI", "META", "DSY", "IBM", "BIDU", "SAP", "OKTA", "NET", "OVH", "IFX", "INTC", "NOW", "YSN", "SSTK", "VRNS" ];
    
    let mut tickers = Vec::new();
    let start_date = match NaiveDateTime::parse_from_str("2025-08-01 00:00:00", "%Y-%m-%d %H:%M:%S") {
        Ok(dt) => dt.and_utc(),
        Err(error) => {
            log::error!("Failed to parse fixed datetime!: {}", error);
            std::process::exit(1);
        },
    };
    let end_date = chrono::Utc::now();

    for i in 0..symbols.len() {
        let stock_symbol = symbols[i].to_string();
        let ticker = stockanalysis::models::ticker::TickerBuilder::new()
            .ticker(&stock_symbol)
            .start_date(&start_date.naive_utc().to_string())
            .end_date(&end_date.naive_utc().to_string())
            .benchmark_symbol("0H1C")
            .interval(Interval::OneDay)
            .build();

        let df = ticker.get_chart_daily(start_date.clone(), end_date.clone()).await?;
        let table = df.to_datatable("ohlcv", true, DataTableFormat::Number);
        let html = table.to_html()?;
        let mut file_name = stock_symbol.clone();
        file_name.extend(".html".chars());
        std::fs::write(&file_name, &html).expect("Should be able to write to file");
        //println!("{}", html);
        tickers.push(ticker);
        //table.show()?;

        let fin = match crate::tools::financials(stock_symbol.clone(), start_date.naive_utc().to_string(), end_date.naive_utc().to_string()) {
            Ok(ret) => ret,
            Err(error) => {
                log::error!("Failed to create financials view: {}", error);
                return Ok(());
            }
        };
        //std::fs::write("fin.html", &fin.key().for_display()).expect("Should be able to write to file");

        let news = match crate::tools::news(stock_symbol.clone()) {
            Ok(ret) => ret,
            Err(error) => {
                log::error!("Failed to create news view: {}", error);
                return Ok(());
            }
        };
        //std::fs::write("news.html", &news.revision().for_display()).expect("Should be able to write to file");
        let opt = match crate::tools::options(stock_symbol.clone()) {
            Ok(ret) => ret,
            Err(error) => {
                log::error!("Failed to create options view: {}", error);
                return Ok(());
            },
        };
        //std::fs::write("opt.html", &opt.revision().for_display()).expect("Should be able to write to file");

        let perf = match crate::tools::performance() {
            Ok(ret) => ret,
            Err(error) => {
                log::error!("Failed to create performance view: {}", error);
                return Ok(());
            },
        };
        //std::fs::write("perf.html", &perf.revision().for_display()).expect("Should be able to write to file");
        
        let screener = match crate::tools::screener() {
            Ok(ret) => ret,
            Err(error) => {
                log::error!("Failed to create screener view: {}", error);
                return Ok(());
            },
        };
        //std::fs::write("screener.html", &screener.revision().for_display()).expect("Should be able to write to file");

        let portfolio = match crate::tools::portfolio() {
            Ok(ret) => ret,
            Err(error) => {
                log::error!("Failed to create portfolio view: {}", error);
                return Ok(());
            },
        };
        //ticker.get_financials(statement_type, frequency)

    }
    let portfolio: Result<Portfolio, String> = Portfolio::builder()
            .ticker_symbols(symbols.clone())
            .benchmark_symbol("0H1C")
            .start_date("2025-03-01")
            .end_date("2025-08-31")
            .interval(Interval::OneDay)
            .confidence_level(0.95)
            .risk_free_rate(0.02)
            .objective_function(ObjectiveFunction::MaxSharpe)
            .build()
            .await
            .map_err(|e| format!("PortfolioBuilder error: {e}"));
    test_portfolio(portfolio).await.unwrap();
    
    Ok(())
}
