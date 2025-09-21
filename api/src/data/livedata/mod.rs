//! Module to retrieve live stock data aka intra-day series
//! Requires API keys as Environment variable of one of the supported providers.

mod alphavantage;
mod polygon_io;
mod twelvedata;

use super::sql::TimeSeriesData;

use std::env::var;
use polars::prelude::*;
use std::error::Error;
use chrono::NaiveDateTime;
use market_data::EnhancedMarketSeries;

// find out which provider is used and use the according function
pub fn live_data(
    symbol: &str,
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
)  -> Result<Vec<EnhancedMarketSeries>, Box<dyn std::error::Error>> {

    match var("Twelvedata_TOKEN") {
        Ok(_val) => twelvedata::live_data(symbol, start_time, end_time),
        Err(_e) => match var("AlphaVantage_TOKEN") {
            Ok(_val) => alphavantage::live_data(symbol, start_time, end_time),
            Err(_e) => match var("Polygon_APIKey") {
                Ok(_val) => polygon_io::live_data(symbol, start_time, end_time),
                Err(e) => {
                    print!("Please use one of the supported suppliers for live stock data!");
                    Err(Box::new(e))
                }
            }
        }
    }
}

// find out which provider is used and use the according function
pub fn update_nightly(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>, 
    symbols: &Vec<String>
) {

    match var("Twelvedata_TOKEN") {
        Ok(_val) => twelvedata::update_nightly(sql_connection.clone(), symbols),
        Err(_e) => match var("AlphaVantage_TOKEN") {
            Ok(_val) => alphavantage::update_nightly(sql_connection.clone(), symbols),
            Err(_e) => match var("Polygon_APIKey") {
                Ok(_val) => polygon_io::update_nightly(sql_connection.clone(), symbols),
                Err(_e) => {
                    print!("Please use one of the supported supplier for live stock data!");
                }
            }
        }
    }
}


pub fn marketdata_to_timeseries(
    timeseries: &market_data::EnhancedMarketSeries,
) -> Vec<TimeSeriesData> {
    let mut series = Vec::new();
    let num_values = timeseries.series.len();
    let base_timestamp = chrono::Utc::now().timestamp();
    for i in 0..num_values {
        let timestamp = base_timestamp - (num_values - i) as i64 * 60;
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

pub fn update_dataframe(
    df: &DataFrame,
    stock_symbol: &str, 
) -> Result<DataFrame, Box<dyn Error>> {
    let col_val = df.column("timestamp")?.i64()?.to_vec().iter().map(|x| x.unwrap()).collect::<Vec<i64>>();
    let start_timestamp = col_val[col_val.len()-1];
    let start_time = chrono::DateTime::from_timestamp_millis(start_timestamp).unwrap().naive_utc();
    let end_time = chrono::Utc::now().naive_utc();
    let enhanced_data: Vec<EnhancedMarketSeries> = match live_data(&stock_symbol, start_time, end_time) {
        Ok(res) => res,
        Err(error) => {
            log::error!("Failed to update timeseries data for {}: {}", stock_symbol, error);
            std::process::exit(1)
        }
    };
    let mut series = Vec::new();
    for data in enhanced_data.iter() {
        series.extend(marketdata_to_timeseries(data));
    }

    let timestamp = series
        .iter()
        .map(|o| super::sql::to_dataframe::to_datetime(o.datetime))
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

    let df2 = df!(
        "timestamp" => &timestamp,
        "open" => &open,
        "high" => &high,
        "low" => &low,
        "close" => &close,
        "volume" => &volume,
        "adjclose" => &adjclose
    )?;

    // concat the new dataframe to the existing one
    let df3 = match df.vstack(&df2) {
        Ok(res) => res,
        Err(error) => {
            log::error!("Failed to append new data to the existing dataframe for {}: {}", stock_symbol, error);
            std::process::exit(1)
        }
    };

    Ok(df3)
}