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

// find out which provider is used and use the according function
pub fn live_data(
    symbol: &str,
)  -> Result<Vec<TimeSeriesData>, Box<dyn std::error::Error>> {

    match var("Twelvedata_TOKEN") {
        Ok(_val) => twelvedata::live_data(symbol),
        Err(_e) => match var("AlphaVantage_TOKEN") {
            Ok(_val) => alphavantage::live_data(symbol),
            Err(_e) => match var("Polygon_APIKey") {
                Ok(_val) => polygon_io::live_data(symbol),
                Err(e) => {
                    print!("Please use one of the supported supplier for live stock data!");
                    Err(Box::new(e))
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
        let v = TimeSeriesData {
            datetime: timestamp,
            open: timeseries.series[i].open as f64,
            high: timeseries.series[i].high as f64,
            low: timeseries.series[i].low as f64,
            close: timeseries.series[i].close as f64,
            volume: timeseries.series[i].volume as f64,
            sma: sma as f64, 
            ema: ema as f64, 
            rsi: rsi as f64, 
            stochastic: stochastic as f64, 
            macd_value: macd_value as f64, 
            signal_value: signal_value as f64, 
            hist_value: hist_value as f64
        };
        series.push(v);
    }
    series
}

pub fn update_dataframe(
    df: &DataFrame,
    stock_symbol: &str, 
) -> Result<DataFrame, Box<dyn Error>> {

    let series: Vec<super::sql::TimeSeriesData> = match live_data(&stock_symbol) {
        Ok(res) => res,
        Err(error) => {
            log::error!("Failed to update timeseries data for {}: {}", stock_symbol, error);
            std::process::exit(1)
        }
    };

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
        .map(|o| super::sql::to_dataframe::to_datetime(o.datetime))
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