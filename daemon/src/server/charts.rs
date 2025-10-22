use polars::prelude::*;
use std::error::Error;
use chrono::{Datelike, Timelike};
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

fn candlestick_chart_live_async_df(ticker: &Ticker, ohlcv: DataFrame, metadata: &api::data::sql::MetaData) -> Result<plotly::plot::Plot, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.candlestick_chart_live_df(ohlcv, metadata, None, None)
    )
}

fn macd_chart_recent_async(
    ticker: &Ticker,
    ohlcv: DataFrame, 
    metadata: &api::data::sql::MetaData,
) -> Result<plotly::plot::Plot, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.macd_chart_recent(ohlcv, metadata, None, None)
    )
}

fn ppo_chart_recent_async(
    ticker: &Ticker,
    ohlcv: DataFrame, 
    metadata: &api::data::sql::MetaData,
) -> Result<plotly::plot::Plot, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.ppo_chart_recent(ohlcv, metadata, None, None)
    )
}

fn mfi_chart_recent_async(
    ticker: &Ticker,
    ohlcv: DataFrame, 
    metadata: &api::data::sql::MetaData,
) -> Result<plotly::plot::Plot, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.mfi_chart_recent(ohlcv, metadata, None, None)
    )
}

fn stochastic_chart_recent_async(
    ticker: &Ticker,
    ohlcv: DataFrame, 
    metadata: &api::data::sql::MetaData,
) -> Result<plotly::plot::Plot, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.stochastic_chart_recent(ohlcv, metadata, None, None)
    )
}

/// For a single symbol create a hard and HTML file for the last 120 minutes
pub fn ticker_chart_recent_for_symbol(
    tickers_mutex: Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>>,
    stock_symbol: String,
    filepath: &std::path::PathBuf,
    start_datetime: chrono::DateTime<chrono::Utc>,
    end_datetime: chrono::DateTime<chrono::Utc>,
) {
    let archivepath = super::archive_path(filepath);
    let mut ticker: Ticker;
    let mut tickers = match tickers_mutex.lock() {
        Ok(t) => t,
        Err(error) => {
            tracing::error!("Failed to lock tichers hash for use! {}", error);
            return;
        }
    };
    if !tickers.contains_key(&stock_symbol) {
        ticker = api::models::ticker::TickerBuilder::new()
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
    let sql_connection = api::data::sql::connect();
    let ohlcv = match api::data::sql::to_dataframe::ohlcv_to_dataframe(
            sql_connection.clone(), &stock_symbol, 
            start_datetime.naive_utc(), end_datetime.naive_utc()
    ) {
        Ok(v) => {
            if v.len() == 0 {
                return;
            }
            let df = v[0].clone();
            df
        },
        Err(e) => {
            tracing::error!("Failed to retrieve data for {}: {}", stock_symbol, e);
            return;
        },
    };
    let metadata = api::data::sql::live_data::get_stock_metadata(sql_connection.clone(), &stock_symbol);

    match candlestick_chart_live_async_df(&ticker, ohlcv.clone(), &metadata) {
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
    match macd_chart_recent_async(&ticker, ohlcv.clone(), &metadata) {
        Ok(pl) => {
            let mut file_name = stock_symbol.clone();
            file_name.extend("_macd_recent.jpg".chars());
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            pl.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
            let html = pl.to_html();
            let mut file_name = stock_symbol.clone();
            file_name.extend("_macd_recent.html".chars());
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&path, &html).expect("Should be able to write to file");
        },
        Err(error) => {
            tracing::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
        },
    }
    match ppo_chart_recent_async(&ticker, ohlcv.clone(), &metadata) {
        Ok(pl) => {
            let mut file_name = stock_symbol.clone();
            file_name.extend("_ppo_recent.jpg".chars());
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            pl.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
            let html = pl.to_html();
            let mut file_name = stock_symbol.clone();
            file_name.extend("_ppo_recent.html".chars());
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&path, &html).expect("Should be able to write to file");
        },
        Err(error) => {
            tracing::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
        },
    }
    match mfi_chart_recent_async(&ticker, ohlcv.clone(), &metadata) {
        Ok(pl) => {
            let mut file_name = stock_symbol.clone();
            file_name.extend("_mfi_recent.jpg".chars());
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            pl.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
            let html = pl.to_html();
            let mut file_name = stock_symbol.clone();
            file_name.extend("_mfi_recent.html".chars());
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&path, &html).expect("Should be able to write to file");
        },
        Err(error) => {
            tracing::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
        },
    }
    match stochastic_chart_recent_async(&ticker, ohlcv.clone(), &metadata) {
        Ok(pl) => {
            let mut file_name = stock_symbol.clone();
            file_name.extend("_stochastic_recent.jpg".chars());
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            pl.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
            let html = pl.to_html();
            let mut file_name = stock_symbol.clone();
            file_name.extend("_stochastic_recent.html".chars());
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&path, &html).expect("Should be able to write to file");
        },
        Err(error) => {
            tracing::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
        },
    }

}

/// Create charts and HTML files for minutely data for the last day
pub fn run_ticker_charts_livedata(
    symbolsstrings: &Vec<String>,
    filepath: &std::path::PathBuf,
    tickers_mutex: Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>>,
) -> Result<(), Box<dyn Error>> {
    let symbols: Vec<&str> = symbolsstrings.iter().map(|s| &**s).collect();
    // find the last day for which we actually have data in the database
    let yesterday = super::yesterday();
    let now = chrono::Local::now();
    let today = if now.hour() > 22 {
        if now.weekday().num_days_from_monday() == 6 {
            yesterday.checked_sub_days(chrono::Days::new(1)).unwrap()
        } else if now.weekday().num_days_from_monday() == 5 {
            yesterday
        } else {
            yesterday.checked_add_days(chrono::Days::new(1)).unwrap()
        }
    } else {
        if now.weekday().num_days_from_monday() == 6 {
            yesterday.checked_sub_days(chrono::Days::new(2)).unwrap()
        } else if now.weekday().num_days_from_monday() == 5 {
            yesterday.checked_sub_days(chrono::Days::new(1)).unwrap()
        } else {
            yesterday
        }
    };
    let archivepath = super::archive_path(filepath);
    for i in 0..symbols.len() {
        let stock_symbol = symbols[i].to_string();
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

/// Create images and HTML charts for daily data
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
    Ok(())
}

#[allow(unreachable_code, unused_variables, dead_code)]
pub fn run_charts_on_updated_dataframe(
    symbols: &Vec<String>,
    sql_connection: Arc<std::sync::Mutex<rusqlite::Connection>>,
    tickers_mutex: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>>,
    dataframes_mutex: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, DataFrame>>>,
    filepath: &std::path::PathBuf,
    archivepath: &std::path::PathBuf,
) {
    return;
    let now = chrono::Local::now();

    for i in 0..symbols.len() {
        let stock_symbol = symbols[i].to_string();
        let today = now.date_naive();
        let end_date = api::data::dataframes::round_time(now.to_utc());
        // start_date should be 24 hours before. If not possible start at midnight UTC
        let start_date = match end_date.checked_sub_signed(chrono::TimeDelta::hours(24)) {
            Some(t) => t,
            None => now.date_naive().and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap()).and_utc(),
        };
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
        let dataframes = match dataframes_mutex.lock() {
            Ok(mutex) => mutex,
            Err(e) => {
                tracing::error!("Failed to lock the mutex on dataframes: {}", e);
                return;
            }
        };
        if !dataframes.contains_key(&stock_symbol) {
            continue;
        }

        let ohlcv = dataframes[&stock_symbol].clone();
        let metadata = api::data::sql::live_data::get_stock_metadata(sql_connection.clone(), &stock_symbol);

        match candlestick_chart_live_async_df(&ticker, ohlcv.clone(), &metadata) {
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
        match macd_chart_recent_async(&ticker, ohlcv.clone(), &metadata) {
            Ok(pl) => {
                let mut file_name = stock_symbol.clone();
                file_name.extend("_macd_recent.jpg".chars());
                let path = filepath.clone().join(file_name);
                super::move_file_to_archive(filepath, &archivepath, &path);
                pl.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
                let html = pl.to_html();
                let mut file_name = stock_symbol.clone();
                file_name.extend("_macd_recent.html".chars());
                let path = filepath.clone().join(file_name);
                super::move_file_to_archive(filepath, &archivepath, &path);
                std::fs::write(&path, &html).expect("Should be able to write to file");
            },
            Err(error) => {
                tracing::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
            },
        }
        match ppo_chart_recent_async(&ticker, ohlcv.clone(), &metadata) {
            Ok(pl) => {
                let mut file_name = stock_symbol.clone();
                file_name.extend("_ppo_recent.jpg".chars());
                let path = filepath.clone().join(file_name);
                super::move_file_to_archive(filepath, &archivepath, &path);
                pl.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
                let html = pl.to_html();
                let mut file_name = stock_symbol.clone();
                file_name.extend("_ppo_recent.html".chars());
                let path = filepath.clone().join(file_name);
                super::move_file_to_archive(filepath, &archivepath, &path);
                std::fs::write(&path, &html).expect("Should be able to write to file");
            },
            Err(error) => {
                tracing::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
            },
        }
        match mfi_chart_recent_async(&ticker, ohlcv.clone(), &metadata) {
            Ok(pl) => {
                let mut file_name = stock_symbol.clone();
                file_name.extend("_mfi_recent.jpg".chars());
                let path = filepath.clone().join(file_name);
                super::move_file_to_archive(filepath, &archivepath, &path);
                pl.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
                let html = pl.to_html();
                let mut file_name = stock_symbol.clone();
                file_name.extend("_mfi_recent.html".chars());
                let path = filepath.clone().join(file_name);
                super::move_file_to_archive(filepath, &archivepath, &path);
                std::fs::write(&path, &html).expect("Should be able to write to file");
            },
            Err(error) => {
                tracing::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
            },
        }
        match stochastic_chart_recent_async(&ticker, ohlcv.clone(), &metadata) {
            Ok(pl) => {
                let mut file_name = stock_symbol.clone();
                file_name.extend("_stochastic_recent.jpg".chars());
                let path = filepath.clone().join(file_name);
                super::move_file_to_archive(filepath, &archivepath, &path);
                pl.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
                let html = pl.to_html();
                let mut file_name = stock_symbol.clone();
                file_name.extend("_stochastic_recent.html".chars());
                let path = filepath.clone().join(file_name);
                super::move_file_to_archive(filepath, &archivepath, &path);
                std::fs::write(&path, &html).expect("Should be able to write to file");
            },
            Err(error) => {
                tracing::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
            },
        }
    }
}
