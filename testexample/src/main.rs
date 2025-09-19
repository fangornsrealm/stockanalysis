//! stock-livedata
//! Work with stock data and analyse and predict stuff

use chrono::NaiveDateTime;
use log::{LevelFilter};
//use polars::prelude::*;
use simplelog::{ColorChoice, CombinedLogger, Config, TermLogger, TerminalMode, WriteLogger};
use std::error::Error;
use std::fs::File;

use api::prelude::*;


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
    
    // old and modifined functionality
    
    // Screen for Large-Cap NASDAQ Stocks
    let screener = Screener::builder()
        .quote_type(QuoteType::Equity)
        .add_filter(ScreenerFilter::EqStr(
            ScreenerMetric::Equity(EquityScreener::Exchange),
            Exchange::NASDAQ.as_ref()
        ))
        .sort_by(
            ScreenerMetric::Equity(EquityScreener::MarketCapIntraday),
            true
        )
        .size(10)
        .build()
        .await?;

    let overview = screener.overview().to_html();
    match overview {
        Ok(chart) => std::fs::write("screener_overview.html", &chart).expect("Should be able to write to file"),
        Err(e) => {
            log::error!("Failed to get overview for screener: {e}");
            return Ok(());
        }
    }

    let metrics = screener.metrics().await?.to_html();
    match metrics {
        Ok(chart) => std::fs::write("screener_metrics.html", &chart).expect("Should be able to write to file"),
        Err(e) => {
            log::error!("Failed to get metrics for screener: {e}");
            return Ok(());
        }
    }

    // Instantiate a Multiple Ticker Object
    let ticker_symbols = screener.symbols.iter()
        .map(|x| x.as_str()).collect::<Vec<&str>>();

    let tickers = api::models::tickers::TickersBuilder::new()
        .tickers(ticker_symbols.clone())
        .start_date("2025-03-01")
        .end_date("2025-09-15")
        .interval(Interval::OneDay)
        .benchmark_symbol("0H1C")
        .confidence_level(0.95)
        .risk_free_rate(0.02)
        .build();
/*
    // Generate a Single Ticker Report
    let symbol = ticker_symbols.first().unwrap();
    let ticker = tickers.clone().get_ticker(symbol).await?;
    let performance = ticker.report(Some(ReportType::Performance)).await?.to_html();
    std::fs::write("screener_performance.html", &performance).expect("Should be able to write to file");
    let financials = ticker.report(Some(ReportType::Financials)).await?.to_html();
    std::fs::write("screener_financials.html", &financials).expect("Should be able to write to file");
    let options = ticker.report(Some(ReportType::Options)).await?.to_html();
    std::fs::write("screener_options.html", &options).expect("Should be able to write to file");
    let news = ticker.report(Some(ReportType::News)).await?.to_html();
    std::fs::write("screener_news.html", &news).expect("Should be able to write to file");

    // Generate a Multiple Ticker Report
    let report = tickers.report(Some(ReportType::Performance)).await?.to_html();
    std::fs::write("screener_report.html", &report).expect("Should be able to write to file");
*/
    // Perform a Portfolio Optimization
    let portfolio = tickers.optimize(Some(ObjectiveFunction::MaxSharpe), None).await?;

    // Generate a Portfolio Report
    let portfolioreport = portfolio.report(Some(ReportType::Performance)).await?.to_html();
    std::fs::write("screener_portfolioreport.html", &portfolioreport).expect("Should be able to write to file");
    // new functionality

    // get a list of symbols from the database
    let sql_connection = api::data::sql::connect();
    let symbolsstrings = api::data::sql::active_symbols(sql_connection.clone());

    // 
    let symbols: Vec<&str> = symbolsstrings.iter().map(|s| &**s).collect();
    
    let mut tickers = Vec::new();
    let start_date = match NaiveDateTime::parse_from_str("2025-03-01 00:00:00", "%Y-%m-%d %H:%M:%S") {
        Ok(dt) => dt.and_utc(),
        Err(error) => {
            log::error!("Failed to parse fixed datetime!: {}", error);
            std::process::exit(1);
        },
    };
    //let end_date = chrono::Utc::now();
    let end_date = match NaiveDateTime::parse_from_str("2025-09-15 00:00:00", "%Y-%m-%d %H:%M:%S") {
        Ok(dt) => dt.and_utc(),
        Err(error) => {
            log::error!("Failed to parse fixed datetime!: {}", error);
            std::process::exit(1);
        },
    };

    for i in 0..symbols.len() {
        let stock_symbol = symbols[i].to_string();
        let ticker = api::models::ticker::TickerBuilder::new()
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
