use polars::prelude::*;
use std::error::Error;

use crate::data::yahoo;
use crate::data::google;
use crate::models::ticker::Ticker;
use crate::data::yahoo::config::{Options, Quote, StatementFrequency, StatementType, TickerSummaryStats};

pub trait TickerData {
    fn get_quote(&self) -> impl std::future::Future<Output = Result<Quote, Box<dyn Error>>>;
    fn get_ticker_stats(&self) -> impl std::future::Future<Output = Result<TickerSummaryStats, Box<dyn Error>>>;
    fn get_chart(&self) -> impl std::future::Future<Output =  Result<DataFrame, Box<dyn Error>>>;
    fn get_chart_daily(&self) -> impl std::future::Future<Output =  Result<DataFrame, Box<dyn Error>>>;
    fn get_options(&self) -> impl std::future::Future<Output = Result<Options, Box<dyn Error>>>;
    fn get_financials(&self, statement_type: StatementType, frequency: StatementFrequency) -> impl std::future::Future<Output = Result<DataFrame, Box<dyn Error>>>;
    fn get_news(&self) -> impl std::future::Future<Output = Result<DataFrame, Box<dyn Error>>>;
}

impl TickerData for Ticker {
    /// Fetches Current Ticker Price from Yahoo Finance
    async fn get_quote(&self) -> Result<Quote, Box<dyn Error>> {
        yahoo::api::get_quote(&self.ticker).await
    }

    /// Fetches Ticker Current Summary Stats from Yahoo Finance
    async fn get_ticker_stats(&self) -> Result<TickerSummaryStats, Box<dyn Error>> {
        yahoo::api::get_ticker_stats(&self.ticker).await
    }

    /// Returns the Ticker OHLCV Data from database or updates them if already loaded
    async fn get_chart(&self) -> Result<DataFrame, Box<dyn Error>> {
        if let Some(ticker_data) = &self.ticker_data {
            ticker_data.clone().to_dataframe()
            // deactivated until there is a subscription for live data Germany
            //super::livedata::update_dataframe(&ticker_data.to_dataframe()?, &self.ticker)
        } else {
            let sql_connection = crate::data::sql::connect();
            let start_date = match chrono::NaiveDate::parse_from_str(&self.start_date, "%Y-%m-%d") {
                    Ok(dt) => dt,
                    Err(_e) => {
                        chrono::NaiveDate::parse_from_str(&self.start_date, "%Y-%m-%d %H:%M:%S")?
                    }
                }
                    .and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap())
                    .and_utc();
            let end_date = match chrono::NaiveDate::parse_from_str(&self.end_date, "%Y-%m-%d") {
                    Ok(dt) => dt,
                    Err(_e) => {
                        chrono::NaiveDate::parse_from_str(&self.end_date, "%Y-%m-%d %H:%M:%S")?
                    }
                }
                    .and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap())
                    .and_utc();
            match super::sql::to_dataframe::ohlcv_to_dataframe(sql_connection, &self.ticker, start_date.naive_utc(), end_date.naive_utc()) {
                Ok(ohlcv) => {
                    if ohlcv.height() > 0 {
                        return Ok(ohlcv);
                    } else {
                        // no entries in database or symbol not found, search yahoo instead
                        let yahoo_ohlcv = yahoo::api::get_chart(
                            &self.ticker, 
                            &start_date.to_string(), 
                            &end_date.to_string(),
                            yahoo::config::Interval::OneDay
                        ).await;
                        return yahoo_ohlcv;
                    }
                },
                Err(error) => {
                    log::error!("{}", error);
                    // no entries in database or symbol not found, search yahoo instead
                    let yahoo_ohlcv = yahoo::api::get_chart(
                        &self.ticker, 
                            &start_date.to_string(), 
                            &end_date.to_string(),
                            yahoo::config::Interval::OneDay
                        ).await;
                    return yahoo_ohlcv;
                }
            }
        }
    }

    /// Returns the Ticker OHLCV Data from database or updates them if already loaded
    async fn get_chart_daily(&self) -> Result<DataFrame, Box<dyn Error>> {
        
        if let Some(ticker_data) = &self.ticker_data {
            ticker_data.clone().to_dataframe()
            // deactivated until there is a subscription for live data Germany
            //super::livedata::update_dataframe(&ticker_data.to_dataframe()?, &self.ticker)
        } else {
            let sql_connection = crate::data::sql::connect();
            let start_date = match chrono::NaiveDate::parse_from_str(&self.start_date, "%Y-%m-%d") {
                    Ok(dt) => dt,
                    Err(_e) => {
                        chrono::NaiveDate::parse_from_str(&self.start_date, "%Y-%m-%d %H:%M:%S")?
                    }
                }
                    .and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap())
                    .and_utc();
            let end_date = match chrono::NaiveDate::parse_from_str(&self.end_date, "%Y-%m-%d") {
                    Ok(dt) => dt,
                    Err(_e) => {
                        chrono::NaiveDate::parse_from_str(&self.end_date, "%Y-%m-%d %H:%M:%S")?
                    }
                }
                    .and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap())
                    .and_utc();
            match super::sql::to_dataframe::daily_ohlcv_to_dataframe(sql_connection, &self.ticker, start_date, end_date).await {
                Ok(ohlcv) => {
                    if ohlcv.height() > 0 {
                        return Ok(ohlcv);
                    } else {
                        // no entries in database or symbol not found, search yahoo instead
                        let yahoo_ohlcv = yahoo::api::get_chart(
                            &self.ticker, 
                            &start_date.date_naive().to_string(), 
                            &end_date.date_naive().to_string(),
                            yahoo::config::Interval::OneDay
                        ).await;
                        return yahoo_ohlcv;
                    }
                },
                Err(error) => {
                    log::error!("{}", error);
                    // no entries in database or symbol not found, search yahoo instead
                    let yahoo_ohlcv = yahoo::api::get_chart(
                        &self.ticker, 
                            &start_date.date_naive().to_string(), 
                            &end_date.date_naive().to_string(),
                            yahoo::config::Interval::OneDay
                        ).await;
                    return yahoo_ohlcv;
                }
            }
        }
    }

    /// Returns Ticker Option Chain Data from Yahoo Finance for all available expirations
    async fn get_options(&self) -> Result<Options, Box<dyn Error>> {
        yahoo::api::get_options(&self.ticker).await
    }
    
    /// Returns Ticker Financials from Yahoo Finance for a given statement type and frequency
    async fn get_financials(
        &self,
        statement_type: StatementType,
        frequency: StatementFrequency
    ) -> Result<DataFrame, Box<dyn Error>> {
        yahoo::api::get_financials(&self.ticker, statement_type, frequency).await
    }

    /// Returns Ticker News from Google Web Search
    async fn get_news(&self) -> Result<DataFrame, Box<dyn Error>> {
        google::api::get_news(&self.ticker, &self.start_date, &self.end_date).await
    }
}