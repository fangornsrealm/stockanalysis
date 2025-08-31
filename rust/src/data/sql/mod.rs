//! Database connection to the data stored by stock-livedata
//! 
use std::sync::Arc;
use rusqlite::{params, Connection, Result};
use serde::{Deserialize, Serialize};

pub mod to_dataframe;

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

/// return STock Equity
pub fn equity(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    stock_symbol: &str,
) -> Vec<StockEquity> {
    let mut v = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return v;
        }
    };
    let query = "SELECT symbol, name, currency, exchange, mic_code, country, type, figi_code, cfi_code, isin, cusip 
                                FROM stocks 
                                    WHERE (symbol = ?1)";
    match connection.prepare(query) {
        Ok(mut statement) => match statement.query(params![&stock_symbol]) {
            Ok(mut rows) => loop {
                match rows.next() {
                    Ok(Some(row)) => {
                        let mut s = StockEquity {..Default::default()};
                        match row.get(0) {
                            Ok(val) => {
                                let st: String = val;
                                s.symbol = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read symbol for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(1) {
                            Ok(val) => {
                                let st: String = val;
                                s.name = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read name for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(2) {
                            Ok(val) => {
                                let st: String = val;
                                s.currency = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read currency for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(3) {
                            Ok(val) => {
                                let st: String = val;
                                s.exchange = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read exchange for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(4) {
                            Ok(val) => {
                                let st: String = val;
                                s.mic_code = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read mic_code for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(5) {
                            Ok(val) => {
                                let st: String = val;
                                s.r#type = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read type for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(6) {
                            Ok(val) => {
                                let st: String = val;
                                s.figi_code = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read figi_code for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(7) {
                            Ok(val) => {
                                let st: String = val;
                                s.cfi_code = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read cfi_code for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(8) {
                            Ok(val) => {
                                let st: String = val;
                                s.isin = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read isin for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(9) {
                            Ok(val) => {
                                let st: String = val;
                                s.cusip = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read cusip for equities: {}", error);
                                continue;
                            }
                        }
                        v.push(s);
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(error) => {
                        log::error!("Failed to read a row from indices: {}", error);
                        break;
                    }
                }
            },
            Err(err) => {
                log::error!(
                    "could not read line from videostore_indices database: {}",
                    err
                );
            }
        },
        Err(err) => {
            log::error!("could not prepare SQL statement: {}", err);
        }
    }

    v
}

/// Return Stock Exchange
pub fn exchange(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    exchange_code: &str,
) -> Exchange {
    let mut s = Exchange {..Default::default()};
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return s;
        }
    };
    let query = "SELECT title, name, code, country, timezone
                                FROM exchanges s 
                                    WHERE (code = ?1 )";
    match connection.prepare(query) {
        Ok(mut statement) => match statement.query(params![&exchange_code]) {
            Ok(mut rows) => loop {
                match rows.next() {
                    Ok(Some(row)) => {
                        match row.get(0) {
                            Ok(val) => {
                                let st: String = val;
                                s.title = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read title for exchanges: {}", error);
                                continue;
                            }
                        }
                        match row.get(1) {
                            Ok(val) => {
                                let st: String = val;
                                s.name = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read name for exchanges: {}", error);
                                continue;
                            }
                        }
                        match row.get(2) {
                            Ok(val) => {
                                let st: String = val;
                                s.code = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read code for exchanges: {}", error);
                                continue;
                            }
                        }
                        match row.get(3) {
                            Ok(val) => {
                                let st: String = val;
                                s.country = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read country for exchanges: {}", error);
                                continue;
                            }
                        }
                        match row.get(4) {
                            Ok(val) => {
                                let st: String = val;
                                s.timezone = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read timezone for exchanges: {}", error);
                                continue;
                            }
                        }
                        return s;
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(error) => {
                        log::error!("Failed to read a row from indices: {}", error);
                        break;
                    }
                }
            },
            Err(err) => {
                log::error!(
                    "could not read line from videostore_indices database: {}",
                    err
                );
            }
        },
        Err(err) => {
            log::error!("could not prepare SQL statement: {}", err);
        }
    }

    s
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
    let equity_list = equity(sql_connection.clone(), stock_symbol);
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
    let exchange = exchange(sql_connection.clone(), &exchange_string);
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

/// return the number of time series data for the stock
pub fn live_data_count(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &MetaData,
) -> usize {
    let mut num = 0_usize;
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return 0;
        }
    };
    let query = "SELECT COUNT(timestamp) FROM time_series WHERE symbol = ?1";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![&metadata.symbol]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => match row.get(0) {
                                Ok(val) => num = val,
                                Err(error) => {
                                    log::error!("Failed to read datetime for file: {}", error);
                                    continue;
                                }
                            },
                            Ok(None) => {
                                //log::warn!("No data read from indices.");
                                break;
                            }
                            Err(error) => {
                                log::error!("Failed to read a row from indices: {}", error);
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    log::error!(
                        "could not read line from videostore_indices database: {}",
                        err
                    );
                }
            }
        }
        Err(err) => {
            log::error!("could not prepare SQL statement: {}", err);
        }
    }
    num
}

pub fn live_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &MetaData,
) -> Vec<TimeSeriesData> {
    let mut t = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return t;
        }
    };
    let query = "SELECT timestamp, open, high, low, close, volume FROM live_data WHERE symbol = ?1 ORDER BY timestamp ASC";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![&metadata.symbol]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => {
                                let mut s = TimeSeriesData {
                                    ..Default::default()
                                };
                                match row.get(0) {
                                    Ok(val) => s.datetime = val,
                                    Err(error) => {
                                        log::error!(
                                            "Failed to read datetime for live_data: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(1) {
                                    Ok(val) => s.open = val,
                                    Err(error) => {
                                        log::error!("Failed to read open for live_data: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(2) {
                                    Ok(val) => s.high = val,
                                    Err(error) => {
                                        log::error!("Failed to read high for live_data: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(3) {
                                    Ok(val) => s.low = val,
                                    Err(error) => {
                                        log::error!("Failed to read low for live_data: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(4) {
                                    Ok(val) => s.close = val,
                                    Err(error) => {
                                        log::error!(
                                            "Failed to read close for live_data: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(4) {
                                    Ok(val) => s.volume = val,
                                    Err(error) => {
                                        log::error!("Failed to read volume for file: {}", error);
                                        continue;
                                    }
                                }
                                t.push(s);
                            }
                            Ok(None) => {
                                //log::warn!("No data read from indices.");
                                break;
                            }
                            Err(error) => {
                                log::error!("Failed to read a row from live_data: {}", error);
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    log::error!("could not read line from live_data database: {}", err);
                }
            }
        }
        Err(err) => {
            log::error!("could not prepare SQL statement: {}", err);
        }
    }

    t
}

pub fn insert_live_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &MetaData,
    timeseries: &market_data::EnhancedMarketSeries,
) -> Vec<TimeSeriesData> {
    let mut series = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return series;
        }
    };
    let num_values = timeseries.series.len();
    let base_timestamp = chrono::Utc::now().timestamp();
    for i in 0..num_values {
        let timestamp = base_timestamp - (num_values - i) as i64 * 60;
        let mut sma = 0.0_f32;
        let mut ema = 0.0_f32;
        let mut rsi = 0.0_f32;
        let mut stochastic = 0.0_f32;
        let mut macd_value = 0.0_f32;
        let mut signal_value = 0.0_f32;
        let mut hist_value = 0.0_f32;
        for (_indicator_name, indicator_values) in &timeseries.indicators.sma {
            if let Some(value) = indicator_values.get(i) {
                sma = value.to_owned();
            }
        }

        for (_indicator_name, indicator_values) in &timeseries.indicators.ema {
            if let Some(value) = indicator_values.get(i) {
                ema = value.to_owned();
            }
        }

        for (_indicator_name, indicator_values) in &timeseries.indicators.rsi {
            if let Some(value) = indicator_values.get(i) {
                rsi = value.to_owned();
            }
        }

        for (_indicator_name, indicator_values) in &timeseries.indicators.stochastic {
            if let Some(value) = indicator_values.get(i) {
                stochastic = value.to_owned();
            }
        }
        for (_indicator_name, (macd, signal, histogram)) in &timeseries.indicators.macd {
            if let Some(val1) = macd.get(i) {
                if let Some(val2) = signal.get(i) {
                    if let Some(val3) = histogram.get(i) {
                        macd_value = val1.to_owned();
                        signal_value = val2.to_owned();
                        hist_value = val3.to_owned();
                    }
                }
            }
        }
        match connection.execute(
            "INSERT INTO live_data (timestamp, symbol, currency, exchange, open, high, low, close, volume, sma, ema, rsi, stochastic, macd_value, signal_value, hist_value ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![&timestamp, &metadata.symbol, &metadata.currency, &metadata.exchange, &timeseries.series[i].open, &timeseries.series[i].high, &timeseries.series[i].low, &timeseries.series[i].close, &timeseries.series[i].volume, &sma, &ema, &rsi, &stochastic, &macd_value, &signal_value, &hist_value ],
        ) {
            Ok(_retval) => {} //log::warn!("Inserted {} video with ID {} and location {} into candidates.", video.id, video.index, candidate_id),
            Err(error) => {
                log::error!("Failed insert live_data! {}", error);
                return series;
            }
        }
        let v = TimeSeriesData {
            datetime: timestamp,
            open: timeseries.series[i].open as f64,
            high: timeseries.series[i].high as f64,
            low: timeseries.series[i].low as f64,
            close: timeseries.series[i].close as f64,
            volume: timeseries.series[i].volume as f64,
        };
        series.push(v);
    }
    series
}

pub fn _delete_live_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &MetaData,
    _timeseries: &market_data::EnhancedMarketSeries,
) {
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return;
        }
    };
    let _ret = connection.execute(
        "DELETE FROM live_data WHERE symbol = ?1",
        params![&metadata.symbol],
    );
}

pub fn _update_live_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &MetaData,
    timeseries: &market_data::EnhancedMarketSeries,
) {
    _delete_live_data(sql_connection.clone(), metadata, timeseries);
    insert_live_data(sql_connection.clone(), metadata, timeseries);
}


/// connect to the database
pub fn connect() -> Result<Arc<std::sync::Mutex<rusqlite::Connection>>, rusqlite::Error> {

    let sqlite_file;
    let connection;
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

    if !sqlite_file.is_file() {
        return Err(rusqlite::Error::InvalidQuery);
    } else {
        connection = Connection::open(sqlite_file)?;
    }
    Ok(std::sync::Arc::new(std::sync::Mutex::new(connection)))
}
