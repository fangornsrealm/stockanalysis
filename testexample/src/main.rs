//! stock-livedata
//! Work with stock data and analyse and predict stuff

use chrono::{Datelike, NaiveDateTime};
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

pub async fn test_portfolio(portfolio: Result<Portfolio, String>, filepath: &std::path::PathBuf) -> Result<(), Box<dyn Error>> {

    let testportfolio = portfolio.clone();
    let opt_chart = 
        match testportfolio {
        Ok(portfolio) => {
            let chart = 
                portfolio.optimization_chart(None, None).map_err(|e| format!("Optimization Chart error: {e}")).unwrap();

            Ok(chart)
        },
        Err(e) => {
            log::error!("Failed to get portfolio: {e}");
            Err(e)
        }
    };
    match opt_chart {
        Ok(chart) => {
            let file_name = "opt_chart.jpg".to_string();
            let path = filepath.clone().join(file_name);
            chart.to_jpeg(&osstr_to_string(path.into_os_string()), 1200, 800, 1.0);

            let file_name = "opt_chart.html";
            let path = filepath.clone().join(file_name);
            std::fs::write(&osstr_to_string(path.into_os_string()), &chart.to_html()).expect("Should be able to write to file")
        },
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
                portfolio.performance_chart(None, None).map_err(|e| format!("Performance Chart error: {e}")).unwrap();

            Ok(chart)
        }
        Err(e) => {
            log::error!("Failed to get portfolio: {e}");
            Err(e)
        }
    };
    match perf_chart {
        Ok(chart) => {
            let file_name = "perf_chart.jpg".to_string();
            let path = filepath.clone().join(file_name);
            chart.to_jpeg(&osstr_to_string(path.into_os_string()), 1200, 800, 1.0);

            let file_name = "perf_chart.html";
            let path = filepath.clone().join(file_name);
            std::fs::write(&osstr_to_string(path.into_os_string()), &chart.to_html()).expect("Should be able to write to file")
        },
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
                portfolio.performance_stats_table().await.map_err(|e| format!("Performance Stats Table error: {e}")).unwrap().to_html().unwrap();
                Ok(chart)
        }
        Err(e) => {
            log::error!("Failed to get portfolio: {e}");
            Err(e)
        }
    };
    match perf_stats_chart {
        Ok(chart) => {
            let file_name = "performance_stats_table.html";
            let path = filepath.clone().join(file_name);
            std::fs::write(&osstr_to_string(path.into_os_string()), &chart).expect("Should be able to write to file")
        },
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
                portfolio.returns_table().map_err(|e| format!("Returns Table error: {e}")).unwrap().to_html().unwrap();            
                Ok(chart)
        }
        Err(e) => {
            log::error!("Failed to get portfolio: {e}");
            Err(e)
        }
    };
    let file_name = "returns_table.html";
    let path = filepath.clone().join(file_name);
    match returns_table {
        Ok(chart) => std::fs::write(&osstr_to_string(path.into_os_string()), &chart).expect("Should be able to write to file"),
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
                portfolio.returns_chart(None, None).map_err(|e| format!("Returns Chart error: {e}")).unwrap();
            Ok(chart)
        }
        Err(e) => {
            log::error!("Failed to get portfolio: {e}");
            Err(e)
        }
    };
    match returns_chart {
        Ok(chart) => {
            let file_name = "returns_chart.jpg".to_string();
            let path = filepath.clone().join(file_name);
            chart.to_jpeg(&osstr_to_string(path.into_os_string()), 1200, 800, 1.0);

            let file_name = "returns_chart.html";
            let path = filepath.clone().join(file_name);
            std::fs::write(&osstr_to_string(path.into_os_string()), &chart.to_html()).expect("Should be able to write to file")
        },
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
                portfolio.returns_matrix(None, None).map_err(|e| format!("Returns Matrix error: {e}")).unwrap();
            Ok(chart)
        }
        Err(e) => {
            log::error!("Failed to get portfolio: {e}");
            Err(e)
        }
    };
    match returns_matrix {
        Ok(chart) => {
            let file_name = "returns_matrix.jpg".to_string();
            let path = filepath.clone().join(file_name);
            chart.to_jpeg(&osstr_to_string(path.into_os_string()), 1200, 800, 1.0);

            let file_name = "returns_matrix.html";
            let path = filepath.clone().join(file_name);
            std::fs::write(&osstr_to_string(path.into_os_string()), &chart.to_html()).expect("Should be able to write to file")
        },
        Err(e) => {
            log::error!("Failed to get chart for portfolio: {e}");
            return Ok(());
        }
    }
    Ok(())
}

async fn test_ticker_data(filepath: &std::path::PathBuf) -> Result<(), Box<dyn Error>> {
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
    let end_date = chrono::Utc::now()
        .date_naive()
        .and_time(
            chrono::NaiveTime::from_num_seconds_from_midnight_opt(23 * 3600 + 59 * 60, 0)
            .unwrap()
        ).and_utc();

    for i in 0..symbols.len() {
        let stock_symbol = symbols[i].to_string();
        let mut ticker = api::models::ticker::TickerBuilder::new()
            .ticker(&stock_symbol)
            .start_date(&start_date.naive_utc().to_string())
            .end_date(&end_date.naive_utc().to_string())
            .benchmark_symbol("0H1C")
            .interval(Interval::OneDay)
            .build();

        let df = ticker.get_chart_daily().await?;
        let table = df.to_datatable("ohlcv", true, DataTableFormat::Number);
        let html = table.to_html()?;
        let mut file_name = stock_symbol.clone();
        file_name.extend(".html".chars());
        let path = filepath.clone().join(file_name);
        std::fs::write(&path, &html).expect("Should be able to write to file");
        match ticker.candlestick_chart(None, None).await {
            Ok(pl) => {
                let mut file_name = stock_symbol.clone();
                file_name.extend("_chart.jpg".chars());
                let path = filepath.clone().join(file_name);
                pl.to_jpeg(&osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
                let html = pl.to_html();
                let mut file_name = stock_symbol.clone();
                file_name.extend("_chart.html".chars());
                let path = filepath.clone().join(file_name);
                std::fs::write(&path, &html).expect("Should be able to write to file");
            },
            Err(error) => {
                log::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
                continue;
            },
        }
        // get only the last stock day.
        // TODO: Replace by live data
        let day = if chrono::Utc::now().weekday() == chrono::Weekday::Mon {
            chrono::Utc::now()
                .date_naive()
                .checked_sub_days(chrono::Days::new(3)).unwrap()   // Last Friday
        } else {
            chrono::Utc::now().date_naive().checked_sub_days(chrono::Days::new(1)).unwrap()
        };
        let start_date = day.clone().and_time(
            chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0)
            .unwrap()
        ).and_utc();

        let end_date = day.clone().and_time(
            chrono::NaiveTime::from_num_seconds_from_midnight_opt(23 * 3600 + 59 * 60, 0)
            .unwrap()
        ).and_utc();
        ticker.start_date = start_date.naive_utc().to_string();
        ticker.end_date = end_date.naive_utc().to_string();
        if end_date.timestamp_millis() <= start_date.timestamp_millis() {
            log::error!("timestamps are do not span a time span!");
        }
        match ticker.candlestick_chart_live(None, None).await {
            Ok(pl) => {
                let mut file_name = stock_symbol.clone();
                file_name.extend("_chart_live.jpg".chars());
                let path = filepath.clone().join(file_name);
                pl.to_jpeg(&osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
                let html = pl.to_html();
                let mut file_name = stock_symbol.clone();
                file_name.extend("_chart_live.html".chars());
                let path = filepath.clone().join(file_name);
                std::fs::write(&path, &html).expect("Should be able to write to file");
            },
            Err(error) => {
                log::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
                continue;
            },
        }
        //println!("{}", html);
        tickers.push(ticker);
        //table.show()?;
    }
    Ok(())
}

async fn test_screeners(filepath: &std::path::PathBuf)  -> Result<(), Box<dyn Error>> {
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

    let file_name = "screener_overview.html";
    let path = filepath.clone().join(file_name);
    let overview = screener.overview().to_html();
    match overview {
        Ok(chart) => std::fs::write(&osstr_to_string(path.into_os_string()), &chart).expect("Should be able to write to file"),
        Err(e) => {
            log::error!("Failed to get overview for screener: {e}");
            return Ok(());
        }
    }

    let file_name = "screener_metrics.html";
    let path = filepath.clone().join(file_name);
    let metrics = screener.metrics().await?.to_html();
    match metrics {
        Ok(chart) => std::fs::write(&osstr_to_string(path.into_os_string()), &chart).expect("Should be able to write to file"),
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

    // Generate a Single Ticker Report
    let symbol = ticker_symbols.first().unwrap();
    let ticker = tickers.clone().get_ticker(symbol).await?;
    let performance = ticker.report(Some(ReportType::Performance)).await?.to_html();
    let file_name = "screener_performance.html";
    let path = filepath.clone().join(file_name);
    std::fs::write(&osstr_to_string(path.into_os_string()), &performance).expect("Should be able to write to file");
    let financials = ticker.report(Some(ReportType::Financials)).await?.to_html();
    let file_name = "screener_financials.html";
    let path = filepath.clone().join(file_name);
    std::fs::write(&osstr_to_string(path.into_os_string()), &financials).expect("Should be able to write to file");
    let options = ticker.report(Some(ReportType::Options)).await?.to_html();
    let file_name = "screener_options.html";
    let path = filepath.clone().join(file_name);
    std::fs::write(&osstr_to_string(path.into_os_string()), &options).expect("Should be able to write to file");
    let news = ticker.report(Some(ReportType::News)).await?.to_html();
    let file_name = "screescreener_newsner_overview.html";
    let path = filepath.clone().join(file_name);
    std::fs::write(&osstr_to_string(path.into_os_string()), &news).expect("Should be able to write to file");

    // Generate a Multiple Ticker Report
    let report = tickers.report(Some(ReportType::Performance)).await?.to_html();
    let file_name = "screener_report.html";
    let path = filepath.clone().join(file_name);
    std::fs::write(&osstr_to_string(path.into_os_string()), &report).expect("Should be able to write to file");

    // Perform a Portfolio Optimization
    let portfolio = tickers.optimize(Some(ObjectiveFunction::MaxSharpe), None).await?;

    // Generate a Portfolio Report
    let portfolioreport = portfolio.report(Some(ReportType::Performance)).await?.to_html();
    let file_name = "screener_portfolioreport.html";
    let path = filepath.clone().join(file_name);
    std::fs::write(&osstr_to_string(path.into_os_string()), &portfolioreport).expect("Should be able to write to file");
    // new functionality
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

    let filepath = std::path::PathBuf::from("testfiles");
    if !filepath.is_dir() {
        std::fs::create_dir(filepath.clone())?;
    }

    let _ret = test_ticker_data(&filepath).await;
    

    // get a list of symbols from the database
    let sql_connection = api::data::sql::connect();
    let symbolsstrings = api::data::sql::active_symbols(sql_connection.clone());

    // 
    let symbols: Vec<&str> = symbolsstrings.iter().map(|s| &**s).collect();
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
    test_portfolio(portfolio, &filepath).await.unwrap();

    let _ret = test_screeners(&filepath).await;

    
    Ok(())
}
