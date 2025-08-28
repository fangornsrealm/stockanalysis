//! Module to retrieve live stock data aka intra-day series
//! Requires API keys as Environment variable of one of the supported providers.

mod alphavantage;
mod polygon_io;
mod twelvedata;

use super::sql::TimeSeriesData;

use std::env::var;

// find out which provider is used and use the according function
pub fn live_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbol: &str,
    exchange_code: &str,
)  -> Result<Vec<TimeSeriesData>, Box<dyn std::error::Error>> {

    match var("Twelvedata_TOKEN") {
        Ok(_val) => twelvedata::live_data(sql_connection, symbol, exchange_code),
        Err(_e) => match var("AlphaVantage_TOKEN") {
            Ok(_val) => alphavantage::live_data(sql_connection, symbol, exchange_code),
            Err(_e) => match var("Polygon_APIKey") {
                Ok(_val) => polygon_io::live_data(sql_connection, symbol, exchange_code),
                Err(e) => {
                    print!("Please use one of the supported supplier for live stock data!");
                    Err(Box::new(e))
                }
            }
        }
    }
}