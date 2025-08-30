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

    let result = match serde_json::to_string(&series) {
        Ok(out) => out,
        Err(error) => {
            log::error!("Failed to turn Timeseries for {} into JSON: {}", stock_symbol, error);
            std::process::exit(1)
        }
    };

    let value = match serde_json::from_str::<serde_json::Value>(&result){
        Ok(val) => val,
        Err(error) => {
            log::error!("Failed to turn Timeseries for {} into Value: {}", stock_symbol, error);
            std::process::exit(1)
        }
    };

    let timestamp = series
        .iter()
        .map(|o| to_datetime(o.datetime))
        .collect::<Vec<NaiveDateTime>>();

    let open = value["open"]
        .as_array()
        .ok_or(format!("open array not found for {stock_symbol}: {result}"))?
        .iter()
        .map(|o| o.as_f64().unwrap_or(0.0))
        .collect::<Vec<f64>>();

    let high = value["high"]
        .as_array()
        .ok_or(format!("high array not found for {stock_symbol}: {result}"))?
        .iter()
        .map(|h| h.as_f64().unwrap_or(0.0))
        .collect::<Vec<f64>>();

    let low = value["low"]
        .as_array()
        .ok_or(format!("low array not found for {stock_symbol}: {result}"))?
        .iter()
        .map(|l| l.as_f64().unwrap_or(0.0))
        .collect::<Vec<f64>>();

    let close = value["close"]
        .as_array()
        .ok_or(format!("close array not found for {stock_symbol}: {result}"))?
        .iter()
        .map(|c| c.as_f64().unwrap_or(0.0))
        .collect::<Vec<f64>>();

    let volume = value["volume"]
        .as_array()
        .ok_or(format!("volume array not found for {stock_symbol}: {result}"))?
        .iter()
        .map(|v| v.as_f64().unwrap_or(0.0))
        .collect::<Vec<f64>>();

    let adjclose = &value["indicators"]["adjclose"][0]["adjclose"]
        .as_array()
        .unwrap_or_else(|| {
            value["close"]
                .as_array()
                .ok_or(format!("close array not found for {stock_symbol}: {result}"))
                .unwrap_or_else(|_| {
                    value["close"]
                        .as_array()
                        .expect("close array not found")
                })
        })
        .iter()
        .map(|c| c.as_f64().unwrap_or(0.0))
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

