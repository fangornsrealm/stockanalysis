use anyhow::Result;
use chrono::{DateTime, Datelike};
use lazy_static::lazy_static;
use market_data::{EnhancedMarketSeries, Interval, MarketClient, Polygon};
use std::env::var;

use super::super::sql;

lazy_static! {
    static ref TOKEN: String =
        var("Polygon_APIKey").expect("Polygon_APIKey env variable is required");
}

pub fn live_data(
    symbol: &str,
    start_time: chrono::NaiveDateTime,
    end_time: chrono::NaiveDateTime,
)  -> Result<Vec<EnhancedMarketSeries>, Box<dyn std::error::Error>> {
    let site: Polygon = Polygon::new(TOKEN.to_string());
    // create the MarketClient
    let mut client: MarketClient<Polygon> = MarketClient::new(site);

    // check if we have data for this symbol
    let stock_symbol = symbol.to_string();
    // retrieve per minute data for the last 10 minutes
    client.site.intraday_series(
        stock_symbol, 
        &start_time.to_string(), 
        &end_time.to_string(), 
        Interval::Min1, 
        960
    )?;
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
    Ok(enhanced_data)
}


/// Update the database at night for all active symbols with missing daily data
pub fn update_nightly(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>, 
    symbols: &Vec<String>
) {
    let site = Polygon::new(TOKEN.to_string());
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
            let num_days;
            if first_day.day() == today.day() {
                num_days = 2000;
            } else {
                let diff = today - first_day;
                num_days = diff.num_days();
            }
            client.site.daily_series(stock_symbol.clone(), &first_day.date_naive().to_string(), &today.date_naive().to_string(), num_days as i32);
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
            client.site.daily_series(stock_symbol.clone(), &stock_last_date.date_naive().to_string(), &today.date_naive().to_string(), num_days as i32);
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