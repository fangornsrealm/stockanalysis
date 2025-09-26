//! data retrieval from database or website
//!
use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use polars::prelude::*;
use std::error::Error;
//use finalytics::utils::date_utils::{round_datetime_to_day, round_datetime_to_hour, round_datetime_to_minute};

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
pub fn i64_column_to_datetime_vec(df: DataFrame) -> Result<Vec<NaiveDateTime>, Box<dyn Error>> {
    let df2 = df.column("timestamp")?.i64()?
            .into_no_null_iter().map(|x| DateTime::from_timestamp_millis(x).unwrap()
            .naive_local()).collect::<Vec<NaiveDateTime>>();
    Ok(df2)
}

pub fn f64_column_to_vec(
    df: &polars::prelude::DataFrame, 
    columnname: &str
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    let v = df.column(columnname)?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
    Ok(v)
}

pub fn i64_column_to_vec(
    df: &polars::prelude::DataFrame, 
    columnname: &str
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let v = df.column(columnname)?.i64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<i64>>();
    Ok(v)
}

/// Returns the Ticker OHLCV Data from the database for a given time range
pub fn ohlcv_to_dataframe(
    sql_connection: Arc<std::sync::Mutex<rusqlite::Connection>>,
    stock_symbol: &str,
    start_date: NaiveDateTime,
    end_date: NaiveDateTime,
) -> Result<Vec<DataFrame>, Box<dyn Error>> {
    let mut v = Vec::new();
    let exchange_code = "XFRA";
    let mut metadata = super::metadata(sql_connection.clone(), &exchange_code, stock_symbol);
    metadata.start_date = start_date.clone().and_utc();
    metadata.end_date = end_date.clone().and_utc();
    let serieses = super::live_data::live_data(sql_connection.clone(), &metadata);
    for series in serieses {
        // timestamps are expected to be number of milliseconds since 1.1. 1970.
        let timestamp = series
            .iter()
            .map(|o| o.datetime * 1000)
            .collect::<Vec<i64>>();

        let open = series.iter().map(|o| o.open).collect::<Vec<f64>>();

        let high = series.iter().map(|o| o.high).collect::<Vec<f64>>();

        let low = series.iter().map(|o| o.low).collect::<Vec<f64>>();

        let close = series.iter().map(|o| o.close).collect::<Vec<f64>>();

        let volume = series.iter().map(|o| o.volume).collect::<Vec<f64>>();

        let adjclose = series.iter().map(|o| o.close).collect::<Vec<f64>>();

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

        // check if any returned dates smaller than start date or greater than end date
        match i64_column_to_datetime_vec(df.clone()) {
            Ok(dt) => {
                let mask = dt.iter()
                    .map(|x| {
                        &start_date < x && x < &end_date
                    })
                    .collect();
                let df = df.filter(&mask)?;
                v.push(df);
            }
            Err(error) => {
                log::error!("Unable to turn timestamps into dates to create a date filter mask! {:?}", error);
            }
        }
    }
    Ok(v)
}

/// Returns the Ticker OHLCV Data from the database for a given time range
pub async fn daily_ohlcv_to_dataframe(
    sql_connection: Arc<std::sync::Mutex<rusqlite::Connection>>,
    stock_symbol: &str,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<DataFrame, Box<dyn Error>> {
    let exchange_code = "XFRA";
    let mut metadata = super::metadata(sql_connection.clone(), &exchange_code, stock_symbol);
    metadata.start_date = start_date;
    metadata.end_date = end_date;
    let series: Vec<super::TimeSeriesData> = super::time_series::timeseries(sql_connection.clone(), &metadata);
    if series.len() > 0 {
        // timestamps are expected to be number of milliseconds since 1.1. 1970.
        let timestamp = series
            .iter()
            .map(|o| date_to_timestamp_millis(to_date(o.datetime * 1000)))
            .collect::<Vec<i64>>();

        let open = series.iter().map(|o| o.open).collect::<Vec<f64>>();

        let high = series.iter().map(|o| o.high).collect::<Vec<f64>>();

        let low = series.iter().map(|o| o.low).collect::<Vec<f64>>();

        let close = series.iter().map(|o| o.close).collect::<Vec<f64>>();

        let volume = series.iter().map(|o| o.volume).collect::<Vec<f64>>();

        let adjclose = series.iter().map(|o| o.close).collect::<Vec<f64>>();

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

        Ok(df)
    } else {
        // No entries in database. Get from Yahoo financial
        crate::data::yahoo::api::get_chart(
            &stock_symbol, 
            &start_date.date_naive().to_string(),
            &end_date.date_naive().to_string(),
            crate::data::yahoo::config::Interval::OneDay).await
    }
}

pub fn ohlcv_hourly(ohlcv: DataFrame) -> Result<DataFrame, Box<dyn Error>> {
    let datetime = ohlcv.column("timestamp")?.i64()?.to_vec().iter().map(|x|
        DateTime::from_timestamp_millis( x.unwrap()).unwrap().naive_local()).collect::<Vec<NaiveDateTime>>();
    let timestamp = ohlcv.column("timestamp")?.i64()?.to_vec().iter().map(|x| x.unwrap()).collect::<Vec<i64>>();
    let open = ohlcv.column("open")?.f64()?.to_vec().iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
    let high = ohlcv.column("high")?.f64()?.to_vec().iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
    let low = ohlcv.column("low")?.f64()?.to_vec().iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
    let close = ohlcv.column("close")?.f64()?.to_vec().iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
    let volume = ohlcv.column("volume")?.f64()?.to_vec().iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
    let adjclose = ohlcv.column("adjclose")?.f64()?.to_vec().iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
    
    let mut timestamp_filtered = Vec::new();
    let mut open_filtered = Vec::new();
    let mut high_filtered = Vec::new();
    let mut low_filtered = Vec::new();
    let mut close_filtered = Vec::new();
    let mut volume_filtered = Vec::new();
    let mut adjclose_filtered = Vec::new();

    for i in 0..datetime.len() {
        let dt = datetime[i].clone();
        if dt.minute() == 0 {
            timestamp_filtered.push(timestamp[i]);
            open_filtered.push(open[i]);
            high_filtered.push(high[i]);
            low_filtered.push(low[i]);
            close_filtered.push(close[i]);
            volume_filtered.push(volume[i]);
            adjclose_filtered.push(adjclose[i]);
        }
    }

    let df = df!(
        "timestamp" => timestamp_filtered,
        "open" => open_filtered,
        "high" => high_filtered,
        "low" => low_filtered,
        "close" => close_filtered,
        "volume" => volume_filtered,
        "adjclose" => adjclose_filtered,
    )?;
    Ok(df)
}
