use polars::prelude::*;
use crate::prelude::*;
use std::error::Error;
use chrono::{DateTime, NaiveDateTime, NaiveTime, offset::Local, Timelike, Utc};
use crate::data::sql::live_data::timestamp_from_datetime_local;

/// Converts a date string in YYYY-MM-DD format to a Unix Timestamp milliseconds since the Epoch
pub fn str_to_timestamp(datetime_str: &str) -> Result<i64, Box<dyn Error>> {
    let datetime = NaiveDateTime::parse_from_str(datetime_str, "%Y-%m-%d %H:%M:%S")?;
    let unix_timestamp = datetime.and_utc().timestamp_millis();
    Ok(unix_timestamp)
}

/// Converts a datetime to a Unix Timestamp milliseconds since the Epoch
pub fn to_timestamp(datetime: NaiveDateTime) -> i64 {
    datetime.and_utc().timestamp_millis()
}

/// Converts a Unix Timestamp to a date string in YYYY-MM-DD format
pub fn to_datetime(unix_timestamp: i64) -> NaiveDateTime {
    let datetime = DateTime::from_timestamp_millis(unix_timestamp).unwrap();
    datetime.naive_utc()
}

/// Converts a Unix Timestamp to a date string in YYYY-MM-DD format
pub fn to_date(unix_timestamp: i64) -> NaiveDateTime {
    let datetime = DateTime::from_timestamp_millis(unix_timestamp).unwrap();
    datetime.date_naive().into()
}

pub fn date_to_timestamp_millis(dt: NaiveDateTime) -> i64 {
    let datetime = dt.date().and_time(chrono::NaiveTime::default());
    let date = datetime.and_utc();
    date.timestamp_millis()
}

/// converts a string into a NaiveDateTime
pub fn str_to_datetime(datetime_str: &str) -> Result<NaiveDateTime, Box<dyn Error>> {
    let datetime = NaiveDateTime::parse_from_str(datetime_str, "%Y-%m-%d %H:%M:%S")?;
    Ok(datetime)
}

/// converts a vector of UNIX Timestamps to a vector of NaiveDateTime
pub fn i64_column_to_datetime_vec(df: &DataFrame) -> Result<Vec<NaiveDateTime>, Box<dyn Error>> {
    let df2 = df.column("timestamp")?.i64()?
            .into_no_null_iter().map(|x| DateTime::from_timestamp_millis(x).unwrap()
            .naive_utc()).collect::<Vec<NaiveDateTime>>();
    Ok(df2)
}

pub fn f64_column_to_vec(
    df: &DataFrame, 
    columnname: &str
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    let v = df.column(columnname)?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
    Ok(v)
}

pub fn i64_column_to_vec(
    df: &DataFrame, 
    columnname: &str
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let v = df.column(columnname)?.i64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<i64>>();
    Ok(v)
}


#[allow(unreachable_code, unused_variables, dead_code)]
pub fn run_analysis_on_updated_dataframe(
    symbols: &Vec<String>,
    tickers_mutex: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>>,
    dataframes: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, DataFrame>>>,
    filepath: &std::path::PathBuf,
) {
    let now = Local::now();
    
    for symbol in symbols.iter() {
    }
}

fn to_dataframe(e: market_data::MarketSeries, m: &crate::data::sql::MetaData) -> Result<DataFrame, Box<dyn Error>> {
    let timestamp = e.data
        .iter()
        .map(|o| timestamp_from_datetime_local(o.date, &m.exchange_timezone) * 1000)
        .collect::<Vec<i64>>();

    let open = e.data.iter().map(|o| o.open as f64).collect::<Vec<f64>>();

    let high = e.data.iter().map(|o| o.high as f64).collect::<Vec<f64>>();

    let low = e.data.iter().map(|o| o.low as f64).collect::<Vec<f64>>();

    let close = e.data.iter().map(|o| o.close as f64).collect::<Vec<f64>>();

    let volume = e.data.iter().map(|o| o.volume as f64).collect::<Vec<f64>>();

    let adjclose = e.data.iter().map(|o| o.close as f64).collect::<Vec<f64>>();
    
    if timestamp.len() == 0 || open.len() == 0 || high.len() == 0 || low.len() == 0 || close.len() == 0 || volume.len() == 0 || adjclose.len() == 0 {
        return Err(Box::new(std::io::Error::other("Lists differ in length!")));
    }
    
    let df = match df!(
        "timestamp" => &timestamp.clone(),
        "open" => &open,
        "high" => &high,
        "low" => &low,
        "close" => &close,
        "volume" => &volume,
        "adjclose" => &adjclose
    ) {
        Ok(df) => df,
        Err(e) => {
            tracing::error!("Failed to create DataFrame from data: {}", e);
            return Err(Box::new(e));
        }
    };

    tracing::debug!("Retrieved {} lines of data.", df.height());
    // check if any adjclose values are 0.0
    //let mask = df.column("adjclose")?.as_series().unwrap().gt(0.0)?;
    //let df = df.filter(&mask)?;

    // check if any returned dates smaller than start date or greater than end date
    let start = m.start_date.timestamp_millis();
    let end = m.end_date.timestamp_millis();
    let mask = timestamp.iter()
                .map(|x| {
                    start < *x && *x < end
                })
                .collect();
    let df = df.filter(&mask)?;
    Ok(df)
}

fn concatenate_dataframe(df1: DataFrame, df2: DataFrame) -> Result<DataFrame, Box<dyn Error>> {
    let df = concat([df1.lazy(), df2.lazy()], UnionArgs::default())?.collect()?;
    Ok(df)
}

fn filter_dataframe_24hours(df: DataFrame) -> Result<DataFrame, Box<dyn Error>> {
    let timestamp = i64_column_to_vec(&df, "timestamp")?;
    let now = chrono::Utc::now();
    let num_seconds_from_midnight = now.num_seconds_from_midnight();
    let start = now.checked_sub_days(chrono::Days::new(1)).unwrap().timestamp_millis();
    let end = now.date_naive().and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(num_seconds_from_midnight, 0).unwrap()).and_utc().timestamp_millis();
    let mask = timestamp.iter()
                .map(|x| {
                    start < *x && *x < end
                })
                .collect();
    let df = df.filter(&mask)?;
    Ok(df)
}

fn append_new_data_to_dataframe(df1: DataFrame, m: &mut crate::data::sql::MetaData) -> Result<DataFrame, Box<dyn Error>> {
    if df1.height() == 0 {
        return Err(Box::new(std::io::Error::other("Lists not long enough!")))
    }
    let timestamp = i64_column_to_vec(&df1, "timestamp")?;
    if timestamp.len() == 0 {
        return Err(Box::new(std::io::Error::other("Lists not long enough!")))
    }
    let last = timestamp[timestamp.len()-1];
    let start_date = match DateTime::from_timestamp_millis(last * 1000) {
        Some(d) => d,
        None => return Err(Box::new(std::io::Error::other("Failed to convert timestamp!"))),
    };
    let now = Utc::now();
    let end_date = now.date_naive().and_time(NaiveTime::from_num_seconds_from_midnight_opt(now.num_seconds_from_midnight(), 0).unwrap());
    m.start_date = start_date;
    m.end_date = end_date.and_utc();
    let df2 = match new_dataframe(m) {
        Ok(df) => df,
        Err(e) => {
            tracing::error!("Failed to get new dataframe: {}", e);
            return Err(Box::new(std::io::Error::other("Failed to get new dataframe!")));
        }
    };
    let df = match concatenate_dataframe(df1, df2) {
        Ok(df) => df,
        Err(e) => {
            tracing::error!("Failed to concatenate dataframes: {}", e);
            return Err(Box::new(std::io::Error::other("Failed to concatenate dataframes!")));
        }
    };
    Ok(df)
}

fn new_dataframe(m: &mut crate::data::sql::MetaData) -> Result<DataFrame, Box<dyn Error>> {
    let symbol = &m.symbol.to_string();
    let serieses = match crate::data::livedata::live_data(&symbol, m.start_date.naive_utc(), m.end_date.naive_utc(), m) {
        Ok(res) => res,
        Err(e) => {
            tracing::error!("Failed to get data updated data for symbol {}: {}", &m.symbol, e);
            return Err(Box::new(std::io::Error::other("Failed to get data updated data for symbol!")));
        },
    };
    if serieses.len() == 0 {
        return Err(Box::new(std::io::Error::other("No Elements in list!")));
    }
    let mut ohlcv = match to_dataframe(serieses[0].clone(), m) {
        Ok(df) => df,
        Err(e) => {
            tracing::error!("Failed to create dataframe from returned data: {}", e);
            return Err(Box::new(std::io::Error::other("Failed to create dataframe from returned data!")));
        }
    };
    if serieses.len() > 1 {
        for i in 1..serieses.len() {
            let series = serieses[i].clone();
            let df = match to_dataframe(series, m) {
                Ok(df) => df,
                Err(e) => {
                    tracing::error!("Failed to create dataframe from returned data: {}", e);
                    return Err(Box::new(std::io::Error::other("Failed to create dataframe from returned data!")));
                }
            };
            ohlcv = match concatenate_dataframe(ohlcv.clone(), df) {
                Ok(df) => df,
                Err(e) => {
                    tracing::error!("Failed to concatenate dataframes: {}", e);
                    return Err(Box::new(std::io::Error::other("Failed to concatenate dataframes!")));
                }
            }
        }
    }
    Ok(ohlcv)
}

#[allow(unreachable_code, unused_variables, dead_code)]
pub fn get_dataframe_for_active_symbols(
    symbols: &Vec<String>,
    tickers_mutex: Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>>,
    dataframes_mutex: Arc<std::sync::Mutex<std::collections::HashMap<String, DataFrame>>>,
) {
    return;
    let nowish = Local::now();
    let start_time = NaiveTime::from_num_seconds_from_midnight_opt(0, 0).expect("That should never fail!");
    let end_time = NaiveTime::from_num_seconds_from_midnight_opt(23*3600 + 59*60, 0).expect("That should never fail!");
    let start_date = nowish.clone().date_naive().and_time(start_time);
    let end_date = nowish.clone().date_naive().and_time(end_time);
    let mut dataframes = match dataframes_mutex.lock() {
        Ok(mutex) => mutex,
        Err(e) => {
            tracing::error!("Failed to lock the mutex on dataframes: {}", e);
            return;
        }
    };
    for symbol in symbols.iter() {
        let mut metadata = crate::data::sql::MetaData {
            symbol: symbol.to_string(),
            start_date: start_date.and_utc(),
            end_date: end_date.and_utc(),
            ..Default::default()};
        let mut ohlcv;
        if dataframes.contains_key(symbol) {
            // update the existing entry
            ohlcv = dataframes[symbol].clone();
            ohlcv = match append_new_data_to_dataframe(ohlcv.clone(), &mut metadata) {
                Ok(df) => df,
                Err(e) => {
                    tracing::error!("Failed to concatenate dataframes: {}", e);
                    continue;
                }
            };
            dataframes.remove(symbol);
        } else {
            ohlcv = match new_dataframe(&mut metadata) {
                Ok(df) => df,
                Err(e) => {
                    tracing::error!("Failed to concatenate dataframes: {}", e);
                    continue;
                }
            }

        }
        ohlcv = match filter_dataframe_24hours(ohlcv.clone()) {
            Ok(df) => df,
            Err(e) => {
                tracing::error!("Failed to concatenate dataframes: {}", e);
                continue;
            }
        };
        // limit date dataframe to the last 24 hours
        dataframes.insert(symbol.to_string(), ohlcv);

    }
}
