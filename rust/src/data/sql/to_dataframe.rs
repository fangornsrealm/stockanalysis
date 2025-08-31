//! data retrieval from database or website
//! 
use polars::prelude::*;
use std::error::Error;
use chrono::{DateTime, NaiveDateTime};
//use finalytics::utils::date_utils::{round_datetime_to_day, round_datetime_to_hour, round_datetime_to_minute};

/// Converts a date string in YYYY-MM-DD format to a Unix Timestamp
pub fn to_timestamp(datetime_str: &str) -> Result<i64, Box<dyn Error>> {
    let datetime = NaiveDateTime::parse_from_str(datetime_str, "%Y-%m-%d %H:%M:%S")?;
    let unix_timestamp = datetime.and_utc().timestamp();
    Ok(unix_timestamp)
}

/// Converts a Unix Timestamp to a date string in YYYY-MM-DD format
pub fn to_datetime(unix_timestamp: i64) -> NaiveDateTime {
    let datetime = DateTime::from_timestamp(unix_timestamp, 0).unwrap();
    datetime.naive_utc()
}

/// converts a string into a NaiveDateTime
pub fn str_to_datetime(datetime_str: &str) -> Result<NaiveDateTime, Box<dyn Error>> {
    let datetime = NaiveDateTime::parse_from_str(datetime_str, "%Y-%m-%d %H:%M:%S")?;
    Ok(datetime)
}

/// Returns the Ticker OHLCV Data from the database for a given time range
pub fn ohlcv_to_dataframe(
    sql_connection: Arc<std::sync::Mutex<rusqlite::Connection>>, 
    stock_symbol: &str, 
    start_date: NaiveDateTime, 
    end_date: NaiveDateTime
) -> Result<DataFrame, Box<dyn Error>> {
    let exchange_code = "XFRA";
    let metadata = super::metadata(sql_connection.clone(), &exchange_code, stock_symbol);

    let series: Vec<super::TimeSeriesData> = super::live_data(sql_connection.clone(), &metadata);

    let timestamp = series
        .iter()
        .map(|o| to_datetime(o.datetime))
        .collect::<Vec<NaiveDateTime>>();

    let open = series
        .iter()
        .map(|o| o.open)
        .collect::<Vec<f64>>();

    let high = series
        .iter()
        .map(|o| o.high)
        .collect::<Vec<f64>>();

    let low = series
        .iter()
        .map(|o| o.low)
        .collect::<Vec<f64>>();

    let close = series
        .iter()
        .map(|o| o.close)
        .collect::<Vec<f64>>();

    let volume = series
        .iter()
        .map(|o| o.volume)
        .collect::<Vec<f64>>();
    
    let adjclose = series
        .iter()
        .map(|o| o.close)
        .collect::<Vec<f64>>();

    let df = df!(
        "timestamp" => &timestamp,
        "open" => &open,
        "high" => &high,
        "low" => &low,
        "close" => &close,
        "volume" => &volume,
        "adjclose" => &adjclose
    )?;

    // check if any adjclose values are 0.0
    let mask = df.column("adjclose")?.as_series().unwrap().gt(0.0)?;
    let df = df.filter(&mask)?;

    // check id any returned dates smaller than start date or greater than end date
    let mask = df["timestamp"]
        .datetime()?
        .as_datetime_iter()
        .map(|x| {
            let v = x.unwrap(); 
            start_date > v && v < end_date
        })
        .collect();
    let df = df.filter(&mask)?;
    Ok(df)
}

