use polars::prelude::*;
use std::error::Error;
use chrono::Timelike;

use api::prelude::*;

fn get_chart_daily(ticker: &Ticker) -> Result<DataFrame, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.get_chart_daily()
    )
}

fn candlestick_chart_async(ticker: &Ticker) -> Result<plotly::plot::Plot, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.candlestick_chart(None, None)
    )
}

fn candlestick_chart_live_async(ticker: &Ticker) -> Result<plotly::plot::Plot, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.candlestick_chart_live(None, None)
    )
}

pub fn ticker_chart_recent_for_symbol(
    tickers_mutex: Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>>,
    stock_symbol: String,
    filepath: &std::path::PathBuf,
) {
    let archivepath = super::archive_path(filepath);
    let nower = chrono::Utc::now();
    let end_datetime = nower.date_naive().and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(nower.num_seconds_from_midnight(), 0).unwrap()).and_utc();
    let start_datetime = nower.clone().date_naive().and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(nower.num_seconds_from_midnight() - 120*60, 0).unwrap()).and_utc();
    let mut ticker: Ticker;
    let mut tickers = match tickers_mutex.lock() {
        Ok(t) => t,
        Err(error) => {
            tracing::error!("Failed to lock tichers hash for use! {}", error);
            return;
        }
    };
    if !tickers.contains_key(&stock_symbol) {
        ticker= api::models::ticker::TickerBuilder::new()
            .ticker(&stock_symbol)
            .start_date(&start_datetime.naive_utc().to_string())
            .end_date(&end_datetime.naive_utc().to_string())
            .benchmark_symbol("0H1C")
            .interval(Interval::OneDay)
            .build();
        tickers.insert(stock_symbol.clone(), ticker.clone());
    } else {
        ticker = tickers[&stock_symbol].clone();
        ticker.start_date = start_datetime.naive_utc().to_string();
        ticker.end_date = end_datetime.naive_utc().to_string();
    }
    if end_datetime.timestamp_millis() <= start_datetime.timestamp_millis() {
        tracing::error!("timestamps do not span a time span!");
    }
    match candlestick_chart_live_async(&ticker) {
        Ok(pl) => {
            let mut file_name = stock_symbol.clone();
            file_name.extend("_chart_recent.jpg".chars());
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            pl.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
            let html = pl.to_html();
            let mut file_name = stock_symbol.clone();
            file_name.extend("_chart_recent.html".chars());
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&path, &html).expect("Should be able to write to file");
        },
        Err(error) => {
            tracing::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
        },
    }
}

fn run_ticker_charts_livedata(
    symbolsstrings: &Vec<String>,
    filepath: &std::path::PathBuf,
    tickers_mutex: Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>>,
) -> Result<(), Box<dyn Error>> {
    let symbols: Vec<&str> = symbolsstrings.iter().map(|s| &**s).collect();
    let yesterday = super::yesterday();
    let archivepath = super::archive_path(filepath);
    for i in 0..symbols.len() {
        let stock_symbol = symbols[i].to_string();
        let today = yesterday.checked_add_days(chrono::Days::new(1)).unwrap();
        let start_date = today.and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap()).and_utc();
        let end_date = today.and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(23 * 3600 + 59 * 60, 0).unwrap()).and_utc();
        let mut ticker: Ticker;
        let mut tickers = match tickers_mutex.lock() {
            Ok(t) => t,
            Err(error) => {
                tracing::error!("Failed to lock tichers hash for use! {}", error);
                return Ok(());
            }
        };
        if !tickers.contains_key(&stock_symbol) {
            ticker= api::models::ticker::TickerBuilder::new()
                .ticker(&stock_symbol)
                .start_date(&start_date.naive_utc().to_string())
                .end_date(&end_date.naive_utc().to_string())
                .benchmark_symbol("0H1C")
                .interval(Interval::OneDay)
                .build();
            tickers.insert(stock_symbol.clone(), ticker.clone());
        } else {
            ticker = tickers[&stock_symbol].clone();
            ticker.start_date = start_date.naive_utc().to_string();
            ticker.end_date = end_date.naive_utc().to_string();
        }
        if end_date.timestamp_millis() <= start_date.timestamp_millis() {
            tracing::error!("timestamps do not span a time span!");
        }
        match candlestick_chart_live_async(&ticker) {
            Ok(pl) => {
                let mut file_name = stock_symbol.clone();
                file_name.extend("_chart_live.jpg".chars());
                let path = filepath.clone().join(file_name);
                super::move_file_to_archive(filepath, &archivepath, &path);
                pl.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
                let html = pl.to_html();
                let mut file_name = stock_symbol.clone();
                file_name.extend("_chart_live.html".chars());
                let path = filepath.clone().join(file_name);
                super::move_file_to_archive(filepath, &archivepath, &path);
                std::fs::write(&path, &html).expect("Should be able to write to file");
            },
            Err(error) => {
                tracing::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
                continue;
            },
        }
    }
    Ok(())
}

pub fn run_ticker_charts(
    symbolsstrings: &Vec<String>,
    filepath: &std::path::PathBuf,
    tickers_mutex: Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>>,
) -> Result<(), Box<dyn Error>> {
    // 
    let symbols: Vec<&str> = symbolsstrings.iter().map(|s| &**s).collect();
    let three_months_ago = chrono::Local::now().date_naive().checked_sub_days(chrono::Days::new(90)).unwrap();
    let yesterday = super::yesterday();
    let archivepath = super::archive_path(filepath);

    let start_date = three_months_ago.and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap()).and_utc();
    let end_date = yesterday.and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(23 * 3600 + 59 * 60, 0).unwrap()).and_utc();

    for i in 0..symbols.len() {
        let stock_symbol = symbols[i].to_string();
        let mut ticker: Ticker;
        let mut tickers = match tickers_mutex.lock() {
            Ok(t) => t,
            Err(error) => {
                tracing::error!("Failed to lock tichers hash for use! {}", error);
                return Ok(());
            }
        };
        if !tickers.contains_key(&stock_symbol) {
            ticker= api::models::ticker::TickerBuilder::new()
                .ticker(&stock_symbol)
                .start_date(&start_date.naive_utc().to_string())
                .end_date(&end_date.naive_utc().to_string())
                .benchmark_symbol("0H1C")
                .interval(Interval::OneDay)
                .build();
            tickers.insert(stock_symbol.clone(), ticker.clone());
        } else {
            ticker = tickers[&stock_symbol].clone();
            ticker.start_date = start_date.naive_utc().to_string();
            ticker.end_date = end_date.naive_utc().to_string();
        }
        let df = get_chart_daily(&ticker).unwrap();
        let table = df.to_datatable("ohlcv", true, DataTableFormat::Number);
        let html = table.to_html()?;
        let mut file_name = stock_symbol.clone();
        file_name.extend(".html".chars());
        let path = filepath.clone().join(file_name);
        std::fs::write(&path, &html).expect("Should be able to write to file");
        match candlestick_chart_async(&ticker) {
            Ok(pl) => {
                let mut file_name = stock_symbol.clone();
                file_name.extend("_chart.jpg".chars());
                let path = filepath.clone().join(file_name);
                super::move_file_to_archive(filepath, &archivepath, &path);
                pl.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
                let html = pl.to_html();
                let mut file_name = stock_symbol.clone();
                file_name.extend("_chart.html".chars());
                let path = filepath.clone().join(file_name);
                super::move_file_to_archive(filepath, &archivepath, &path);
                std::fs::write(&path, &html).expect("Should be able to write to file");
            },
            Err(error) => {
                tracing::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
                continue;
            },
        }
    }
    run_ticker_charts_livedata(symbolsstrings, filepath, tickers_mutex)?;
    Ok(())
}
