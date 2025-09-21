use anyhow::Result;
use chrono::DateTime;
use lazy_static::lazy_static;
use market_data::{AlphaVantage, EnhancedMarketSeries, MarketClient, OutputSize, Interval};
use std::env::var;

use crate::data::sql;

lazy_static! {
    static ref TOKEN: String =
        var("AlphaVantage_TOKEN").expect("AlphaVantage_TOKEN env variable is required");
}

pub fn live_data(
    symbol: &str,
    _start_time: chrono::NaiveDateTime,
    _end_time: chrono::NaiveDateTime,
)  -> Result<Vec<sql::TimeSeriesData>, Box<dyn std::error::Error>> {
    let mut site = AlphaVantage::new(TOKEN.to_string());
    // retrieve per minute data
    match site.intraday_series(symbol, OutputSize::Compact, Interval::Min1) {
        Ok(()) => {},
        Err(error) => return Err(Box::new(error))
    }
    // create the MarketClient
    let mut client = MarketClient::new(site);

    // creates the query URL & download the raw data
    client = client.create_endpoint()?.get_data()?;
    // transform into MarketSeries, that can be used for further processing
    let resvec = client.transform_data();

    //data.iter().for_each(|output| match output {
    //    Ok(data) =>  {
    //        log::debug!("{}\n\n", data);
    //        let _ret = crate::sql::insert_timeseries(sql_connection.clone(), &metadata, data);
    //    },
    //    Err(err) => log::error!("{}", err),
    //});
    // the data can be enhanced with the calculation of a number of  market indicators
    let enhanced_data: Vec<EnhancedMarketSeries> = resvec
        .into_iter()
        .filter_map(|series| series.ok())
        .map(|series| {
            series
                .enhance_data()
                .with_sma(10)
                .with_ema(20)
                .with_ema(6)
                .with_rsi(14)
                .calculate()
        })
        .collect();

    // store the data
    let mut retvec = Vec::new();
    for data in enhanced_data.iter() {
        retvec.extend(super::marketdata_to_timeseries(data));
    }
    Ok(retvec)
}

/// Update the database at night for all active symbols with missing daily data
pub fn update_nightly(symbols: &Vec<String>) {
    let site = AlphaVantage::new(TOKEN.to_string());
    let sql_connection = crate::data::sql::connect();
    let exchange_code = "XFRA";
     // create the MarketClient
    let mut client = MarketClient::new(site);

    // check if we have data for this symbol
    let mut first_day = chrono::Utc::now();
    let today = chrono::Utc::now();
    for i in 0..symbols.len() {
        let stock_symbol = symbols[i].clone();
        let metadata = sql::metadata(sql_connection.clone(), exchange_code, &stock_symbol);
        let daily_data = sql::timeseries(sql_connection.clone(), &metadata);
        if daily_data.len() == 0 {
            // start a new series\
            client.site.daily_series(stock_symbol.clone(), OutputSize::Full);
            // creates the query URL & download the raw data
            client = match client.create_endpoint() {
                Ok(client) => client,
                Err(error) => {
                    log::error!("Failed to create the endpoint: {}", error);
                    break;
                }
            };
            client = match client.get_data() {
                Ok(client) => client,
                Err(error) => {
                    log::error!("Failed to create the endpoint: {}", error);
                    break;
                }
            };

            // transform into MarketSeries, that can be used for further processing
            let data = client.transform_data();

            // store the data
            for res in data {
                match res {
                    Ok(data) =>  {
                        log::debug!("{}\n\n", data);
                        let _ret = sql::insert_timeseries(sql_connection.clone(), &metadata, &data);
                    },
                    Err(err) => log::error!("{}", err),
                }
            }

        } else {
            let stock_first_date = match DateTime::from_timestamp_millis(daily_data[0].datetime * 1000) {
                Some(d) => d,
                None => {
                    continue;
                }
            };
            if stock_first_date < first_day {
                first_day = stock_first_date;
            }
            let stock_last_date = match DateTime::from_timestamp_millis(daily_data[daily_data.len() - 1].datetime * 1000) {
                Some(d) => d,
                None => {
                    continue;
                }
            };
            let num_days = (today - stock_last_date).num_days();
            if num_days == 0 {
                continue;
            }
            client.site.daily_series(stock_symbol.clone(), OutputSize::Full);
            // creates the query URL & download the raw data
            client = match client.create_endpoint() {
                Ok(client) => client,
                Err(error) => {
                    log::error!("Failed to create the endpoint: {}", error);
                    break;
                }
            };
            client = match client.get_data() {
                Ok(client) => client,
                Err(error) => {
                    log::error!("Failed to create the endpoint: {}", error);
                    break;
                }
            };

            // transform into MarketSeries, that can be used for further processing
            let data = client.transform_data();

            // store the data
            for res in data {
                match res {
                    Ok(data) =>  {
                        log::debug!("{}\n\n", data);
                        let _ret = sql::insert_timeseries(sql_connection.clone(), &metadata, &data);
                    },
                    Err(err) => log::error!("{}", err),
                }
            }
        }
    }
}