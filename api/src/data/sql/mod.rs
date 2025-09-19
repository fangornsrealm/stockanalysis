//! Database connection to the data stored by stock-livedata
//!
use chrono::{DateTime, Utc, Months};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use lazy_static::lazy_static;

pub mod events;
pub mod init;
pub mod live_data;
pub mod symbols;
//pub use metadata::{metadata, check_equity_exists};
pub mod time_series;
pub mod to_dataframe;
pub use to_dataframe::{ohlcv_to_dataframe, daily_ohlcv_to_dataframe, i64_to_datetime_vec};

/// Metadata stock metadata
#[derive(Debug, Deserialize, Serialize)]
pub struct MetaData {
    symbol: String,
    #[allow(dead_code)]
    currency: String,
    #[allow(dead_code)]
    exchange_timezone: String,
    #[allow(dead_code)]
    exchange: String,
    #[allow(dead_code)]
    exchange_code: String,
    #[allow(dead_code)]
    r#type: String,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
}

impl Default for MetaData {
    fn default() -> MetaData {
        MetaData {
            symbol: String::new(),
            currency: String::new(),
            exchange_timezone: String::new(),
            exchange: String::new(),
            exchange_code: String::new(),
            r#type: String::new(),
            start_date: Utc::now().checked_sub_months(Months::new(3)).unwrap(),
            end_date: Utc::now(),
        }
    }
}

/// Exchange metadata
#[derive(Clone, Debug, Deserialize)]
pub struct Exchange {
    /// title
    pub title: String,
    ///name
    pub name: String,
    /// code
    pub code: String,
    /// country
    pub country: String,
    /// timezone
    pub timezone: String,
}

impl Default for Exchange {
    fn default() -> Exchange {
        Exchange {
            title: String::new(),
            name: String::new(),
            code: String::new(),
            country: String::new(),
            timezone: String::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Exchanges {
    pub data: Vec<Exchange>,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Equities {
    pub data: Vec<StockEquity>,
    pub count: u64,
    pub status: String,
}

/// Stock Equity metadata
#[derive(Clone, Debug, Deserialize)]
pub struct StockEquity {
    /// stock symbol
    pub symbol: String,
    /// Full name
    pub name: String,
    /// Currency
    pub currency: String,
    /// Exchange title
    pub exchange: String,
    /// Exchange code
    pub mic_code: String,
    /// country
    pub country: String,
    /// Stock type
    pub r#type: String,
    /// FIGI style code
    pub figi_code: String,
    /// CFI style code
    pub cfi_code: String,
    /// ISIN code
    pub isin: String,
    /// CUSIP
    pub cusip: String,
}

impl Default for StockEquity {
    fn default() -> StockEquity {
        StockEquity {
            symbol: String::new(),
            name: String::new(),
            currency: String::new(),
            exchange: String::new(),
            mic_code: String::new(),
            country: String::new(),
            r#type: String::new(),
            figi_code: String::new(),
            cfi_code: String::new(),
            isin: String::new(),
            cusip: String::new(),
        }
    }
}


#[derive(Clone, Debug, Deserialize)]
pub struct YahooSymbols {
    pub data: Vec<YahooSymbol>,
    pub count: u64,
    pub status: String,
}

/// Stock Equity metadata
#[derive(Clone, Debug, Deserialize)]
pub struct YahooSymbol {
    /// stock symbol
    pub ysymbol: String,
    /// Full name
    pub name: String,
}

impl Default for YahooSymbol {
    fn default() -> YahooSymbol {
        YahooSymbol {
            ysymbol: String::new(),
            name: String::new(),
        }
    }
}

/// Return Metadata
pub fn metadata(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    exchange_code: &str,
    stock_symbol: &str,
) -> MetaData {
    let mut m = MetaData {
        symbol: stock_symbol.to_string(),
        ..Default::default()
    };
    let mut exchange_string = exchange_code.to_string();
    let equity_list = symbols::equity(sql_connection.clone(), stock_symbol);
    let mut desired_found = false;
    for e in equity_list.iter() {
        if e.mic_code == exchange_code {
            desired_found = true;
        }
    }
    if !desired_found {
        for e in equity_list.iter() {
            if e.currency == "EUR" {
                desired_found = true;
                exchange_string = e.mic_code.clone();
                m.currency = e.currency.clone();
                m.r#type = e.r#type.clone();
                m.exchange_code = e.mic_code.clone();
            }
        }
    }
    if !desired_found {
        for e in equity_list.iter() {
            if e.currency == "USD" {
                desired_found = true;
                exchange_string = e.mic_code.clone();
                m.currency = e.currency.clone();
                m.r#type = e.r#type.clone();
                m.exchange_code = e.mic_code.clone();
            }
        }
    }
    if !desired_found {
        if equity_list.len() > 0 {
            exchange_string = equity_list[0].mic_code.clone();
            m.currency = equity_list[0].currency.clone();
            m.r#type = equity_list[0].r#type.clone();
            m.exchange_code = equity_list[0].mic_code.clone();
        } else {
            log::error!("Failed to find stock symbol {}!", m.symbol);
            return m;
        }
    }
    let exchange = symbols::exchange(sql_connection.clone(), &exchange_string);
    m.exchange = exchange.title.clone();
    m.exchange_timezone = exchange.timezone.clone();
    m
}

/// Stock data time series
#[derive(Debug, Deserialize, Serialize)]
pub struct TimeSeriesData {
    /// Datetime stored as i64
    pub datetime: i64,
    /// open value of time frame
    pub open: f64,
    /// highest value of time frame
    pub high: f64,
    /// lowest value of time frame
    pub low: f64,
    /// close value of time frame
    pub close: f64,
    /// trade volume of time frame
    pub volume: f64,
}

impl Default for TimeSeriesData {
    fn default() -> TimeSeriesData {
        TimeSeriesData {
            datetime: 0,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
        }
    }
}

/// Stock event jump up or down by x percent
#[derive(Debug, Deserialize, Serialize)]
pub struct JumpEventData {
    /// Datetime stored as i64
    pub datetime: i64,
    /// open value of time frame
    pub symbol: String,
    /// highest value of time frame
    pub percent: f64,
}

impl Default for JumpEventData {
    fn default() -> JumpEventData {
        JumpEventData {
            datetime: 0,
            symbol: String::new(),
            percent: 0.0,
        }
    }
}

/// Stock event recurring every x minutes by +- y percent
#[derive(Debug, Deserialize, Serialize)]
pub struct RecurringEventData {
    /// symbol name
    pub symbol: String,
    /// period in minutes i64
    pub minutes_period: i64,
    /// percent of change and sign
    pub percent: f64,
}

impl Default for RecurringEventData {
    fn default() -> RecurringEventData {
        RecurringEventData {
            symbol: String::new(),
            minutes_period: 0,
            percent: 0.0,
        }
    }
}

fn sql_file_path() -> std::path::PathBuf {
    let sqlite_file;
    match dirs::data_local_dir() {
        Some(pb) => {
            let mut dir = pb.join("stock-livedata");
            if !dir.exists() {
                let ret = std::fs::create_dir_all(dir.clone());
                if ret.is_err() {
                    log::warn!("Failed to create directory {}", dir.display());
                    dir = dirs::home_dir().unwrap();
                }
            }
            sqlite_file = dir.join("time_series.sqlite");
        }
        None => {
            let dir = dirs::home_dir().unwrap();
            sqlite_file = dir.join("time_series.sqlite");
        }
    }
    if !sqlite_file.exists() {
        init::init_database(sqlite_file.clone());
    }

    sqlite_file
}

/// connect to the database
pub fn connect() -> Arc<std::sync::Mutex<rusqlite::Connection>> {
    lazy_static! {
        static ref SQL_CONNECTION: Arc<std::sync::Mutex<rusqlite::Connection>> =
            std::sync::Arc::new(std::sync::Mutex::new(
                Connection::open(sql_file_path().as_path()).unwrap()
            ));
    }

    SQL_CONNECTION.clone()
}

