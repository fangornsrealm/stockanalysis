use chrono::{Datelike, NaiveTime, offset::Local, Timelike};
use eyre::{bail, ensure, Result as EyreResult, WrapErr as _};
use structopt::StructOpt;
use tokio::{
    sync::broadcast,
    time::{self, Duration}
};

#[derive(Debug, PartialEq, StructOpt)]
pub struct Options {
    /// API Server url
    #[structopt(long, env = "HELP", default_value = "no")]
    pub help: String,
}

pub fn get_livedata_active_symbols(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>, 
    symbols: &Vec<String>
) {
    let now = Local::now();
    
    for symbol in symbols.iter() {
        let metadata = api::data::sql::metadata(sql_connection.clone(), "XFRA", symbol);
        let start_time = NaiveTime::from_num_seconds_from_midnight_opt(7*60, 0).expect("That should never fail!");
        let end_time = NaiveTime::from_num_seconds_from_midnight_opt(23*60, 0).expect("That should never fail!");
        let start_date = now.clone().date_naive().and_time(start_time);
        let end_date = now.clone().date_naive().and_time(end_time);
        match api::data::livedata::live_data(symbol, start_date, end_date) {
            Ok(enhanced_data) => {
                // store the data
                for data in enhanced_data.iter() {
                    let _ret = api::data::sql::insert_live_data(sql_connection.clone(), &metadata, data);
                }
            },
            Err(e) => {
                log::error!("Failed to retrieve data for symbol {} from provider! {}", symbol, e);
                continue;
            },
        }
    }
}

pub fn run_analysis_on_updated_dataframe(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>, 
    symbols: &Vec<String>
) {
    let now = Local::now();
    
    for symbol in symbols.iter() {
        let start_time = NaiveTime::from_num_seconds_from_midnight_opt(7*60, 0).expect("That should never fail!");
        let end_time = NaiveTime::from_num_seconds_from_midnight_opt(23*60, 0).expect("That should never fail!");
        let start_date = now.clone().date_naive().and_time(start_time);
        let end_date = now.clone().date_naive().and_time(end_time);
        let ohlcv: polars::prelude::DataFrame = match api::data::sql::to_dataframe::ohlcv_to_dataframe(
            sql_connection.clone(),
            symbol,
            start_date,
            end_date,
        ) {
            Ok(df) => df,
            Err(e) => {
                log::error!("Failed to get dataframe from database for symbol {}: {}", symbol, e);
                continue;
            }
        };
        let timestamps = match api::data::sql::to_dataframe::i64_column_to_vec(&ohlcv, "timestamp") {
            Ok(df) => df,
            Err(error) => {
                log::error!("Unable to turn get column timestamp! {:?}", error);
                continue;
            }
        };
        let datetimes = match api::data::sql::to_dataframe::i64_to_datetime_vec(ohlcv.clone()) {
            Ok(df) => df,
            Err(error) => {
                log::error!("Unable to turn timestamps into dates! {:?}", error);
                continue;
            }
        };
        let adjclose = match api::data::sql::to_dataframe::f64_column_to_vec(&ohlcv, "adjclose") {
            Ok(df) => df,
            Err(error) => {
                log::error!("Unable to turn get column adjclose! {:?}", error);
                continue;
            }
        };

    }
}

pub async fn update_database() -> EyreResult<()> {
    let now = Local::now();
    let sql_connection = api::data::sql::connect();
    let symbols = api::data::sql::symbols::active_symbols(sql_connection.clone());
    if now.hour() == 23 && now.minute() == 0 {
        // run daily jobs.
        api::data::livedata::update_nightly(sql_connection.clone(), &symbols);
        // temporarily get the minutely data also once per day until there is a subscription with live-data access
        for symbol in symbols.iter() {
            let mut metadata: api::data::sql::MetaData = api::data::sql::metadata(sql_connection.clone(), "XFRA", symbol);
            let start_time = NaiveTime::from_num_seconds_from_midnight_opt(7*60, 0).expect("That should never fail!");
            let end_time = NaiveTime::from_num_seconds_from_midnight_opt(22*60, 0).expect("That should never fail!");
            let start_date = now.clone().date_naive().and_time(start_time);
            let end_date = now.clone().date_naive().and_time(end_time);
            metadata.start_date = start_date.clone().and_utc();
            metadata.end_date = end_date.clone().and_utc();
            
            match api::data::livedata::live_data(symbol, start_date, end_date) {
                Ok(enhanced_data) => {
                    // store the data
                    for data in enhanced_data.iter() {
                        let _ret = api::data::sql::insert_live_data(sql_connection.clone(), &metadata, data);
                    }
                },
                Err(e) => {
                    log::error!("Failed to retrieve data for symbol {} from provider! {}", symbol, e);
                    continue;
                },
            }
        }
    } else {
        // run live updates every minute on Weekdays
        if now.weekday().num_days_from_monday() < 5 {
            if now.hour() > 6 || now.hour() < 22 {
                //get_livedata_active_symbols(sql_connection.clone(), &symbols);

                // triger the live analysis and event detection

            }
        }
    }
    

    Ok(())
}

pub async fn main(_options: Options, shutdown: broadcast::Sender<()>) -> EyreResult<()> {

    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await; // This should go first.
            tokio::spawn(update_database());
        }
    });
    // Wait for shutdown
    shutdown.subscribe().recv().await?;
    
    Ok(())
}

#[cfg(test)]
#[allow(unused_imports)]
mod test {
    use super::*;
    use hyper::{body::to_bytes, Request};
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn test_hello_world() {
        assert_eq!(1, 1);
    }
}

#[cfg(feature = "bench")]
#[allow(clippy::wildcard_imports, unused_imports)]
pub mod bench {
    use super::*;
    use crate::bench::runtime;
    use criterion::{black_box, Criterion};
    use hyper::body::to_bytes;

    pub fn group(c: &mut Criterion) {
        bench_hello_world(c);
    }

    fn bench_hello_world(c: &mut Criterion) {
        c.bench_function("bench_hello_world", |b| {
            b.to_async(runtime()).iter(|| async {
            });
        });
    }
}
