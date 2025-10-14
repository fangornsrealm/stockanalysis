
use polars::prelude::*;
use chrono::{Datelike, NaiveTime, offset::Local, Timelike};
use eyre::Result as EyreResult;
use structopt::StructOpt;
use tokio::{
    sync::broadcast,
    time::{self, Duration}
};

use api::prelude::*;

mod charts;
pub use charts::run_ticker_charts;
mod daily_data;
pub use daily_data::run_analysis_on_historical_data;
mod live_data;
//pub use live_data::{run_analysis_on_updated_dataframe, get_livedata_for_active_symbols};
mod screener;
pub use screener::run_screener_process;
mod portfolio;
pub use portfolio::run_portfolio_analysis;

/// convert an OsString (from PathBuf) to a usable String
pub fn osstr_to_string(osstr: std::ffi::OsString) -> String {
    match osstr.to_str() {
        Some(str) => return str.to_string(),
        None => {}
    }
    String::new()
}

#[derive(Debug, PartialEq, StructOpt)]
pub struct Options {
    /// API Server url
    #[structopt(long, env = "HELP", default_value = "no")]
    pub help: String,
}

fn yesterday() -> chrono::NaiveDate {
    let days = chrono::Utc::now().weekday().num_days_from_monday();
    if days < 5 {
        chrono::Local::now().date_naive().checked_sub_days(chrono::Days::new(1)).unwrap()
    } else if days == 5 {
        chrono::Local::now().date_naive().checked_sub_days(chrono::Days::new(2)).unwrap()
    } else {
        chrono::Local::now().date_naive().checked_sub_days(chrono::Days::new(3)).unwrap()
    } 
}

fn archive_path(filepath: &std::path::PathBuf) -> std::path::PathBuf {
    let days = chrono::Local::now().weekday().num_days_from_monday();
    let yesterday = chrono::Local::now().date_naive().checked_sub_days(chrono::Days::new(1)).unwrap();
    let date_based_name = if days < 5 {
        format!("archive_{}", yesterday.to_string())
    } else {
        "archive_test".to_string()
    };
    filepath.clone().join(date_based_name)
}

fn move_file_to_archive(filepath: &std::path::PathBuf, archivepath: &std::path::PathBuf, file: &std::path::PathBuf) {
    let oldpath = filepath.join(file);
    let newpath = archivepath.join(file);
    if !oldpath.exists() {
        return;
    }
    if !archivepath.is_dir() {
        match std::fs::create_dir_all(archivepath) {
            Ok(()) => (),
            Err(e) => {
                tracing::error!("Failed to create directory: {}", e);
                return;
            },
        }
    }
    match std::fs::hard_link(oldpath.clone(), newpath) {
        Ok(()) => (),
        Err(e) => {
            tracing::error!("Failed to link file: {}", e);
            return;
        },
    }
    match std::fs::remove_file(oldpath) {
        Ok(()) => (),
        Err(e) => {
            tracing::error!("Failed to remove file: {}", e);
            return;
        },
    }

}

pub async fn run_jobs(
    tickers: Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>>, 
    sql_connection: Arc<std::sync::Mutex<rusqlite::Connection>>,
    last_nightly_update_mutex: Arc<std::sync::Mutex<chrono::DateTime<Local>>>,
) -> EyreResult<()> {
    let now = Local::now();
    let symbols = api::data::sql::symbols::active_symbols(sql_connection.clone());
    let mut filepath = dirs::home_dir().unwrap().join("stock-analysis-reports");
    if !filepath.is_dir() {
        match std::fs::create_dir_all(filepath.clone()) {
            Ok(()) => (),
            Err(e) => {
                tracing::error!("Failed to create directory: {}", e);
            },
        }
        filepath = dirs::home_dir().unwrap();
    }
    let mut last_nightly_update = last_nightly_update_mutex.lock().unwrap();
    if now.hour() == 23 && last_nightly_update.date_naive() < now.date_naive() {
        tracing::warn!("{} Running nightly updates.", now.naive_local().to_string());
        // run daily jobs.
        api::data::livedata::update_nightly(sql_connection.clone(), &symbols);
        
        // temporarily get the minutely data also once per day until there is a subscription with live-data access
        let start_time = NaiveTime::from_num_seconds_from_midnight_opt(0, 0).expect("That should never fail!");
        let end_time = NaiveTime::from_num_seconds_from_midnight_opt(23*3600, 0).expect("That should never fail!");
        for symbol in symbols.iter() {
            let mut metadata: api::data::sql::MetaData = api::data::sql::metadata(sql_connection.clone(), "XFRA", symbol);
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
                    tracing::error!("Failed to retrieve data for symbol {} from provider! {}", symbol, e);
                    continue;
                },
            }
        }
        run_analysis_on_historical_data(sql_connection.clone(), &symbols);

        let _ret = run_screener_process(&filepath);

        let _ret = run_ticker_charts(&symbols, &filepath, tickers.clone());

        let _ret = run_portfolio_analysis(&symbols, &filepath);
    
        *last_nightly_update = now.clone();
    } else {
        // run live updates every minute on Weekdays
        if now.weekday().num_days_from_monday() < 5 {
            if now.hour() > 6 || now.hour() < 22 {
                tracing::warn!("{} Skipping operation until there is live data.", now.naive_local().to_string());
                /*
                get_livedata_for_active_symbols(sql_connection.clone(), &symbols);

                // create recent charts for active stock symbols
                let mut live_filepath = filepath.clone().join("live_charts");
                if !live_filepath.is_dir() {
                    match std::fs::create_dir_all(live_filepath.clone()) {
                        Ok(()) => (),
                        Err(e) => {
                            tracing::error!("Failed to create directory: {}", e);
                        },
                    }
                    live_filepath = dirs::home_dir().unwrap();
                }

                let _ret = run_ticker_charts_livedata(&symbols, &live_filepath, tickers.clone());

                // triger the live analysis and event detection
                run_analysis_on_updated_dataframe(sql_connection.clone(), &symbols);
                */

                
            }
        }
    }

    Ok(())
}

pub async fn main(_options: Options, shutdown: broadcast::Sender<()>) -> EyreResult<()> {
    let sql_connection = api::data::sql::connect();

    let tickers: Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>> =
            std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new()
            ));
    let last_nightly_update: Arc<std::sync::Mutex<chrono::DateTime<Local>>> =
            std::sync::Arc::new(std::sync::Mutex::new(
                chrono::DateTime::from_timestamp(24 * 3600 * 100,0).unwrap().naive_local().and_local_timezone(chrono::Local).unwrap()
            ));
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await; // This should go first.
            tokio::spawn(run_jobs(tickers.clone(), sql_connection.clone(), last_nightly_update.clone()));
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
    use tracing::{info, warn, error};
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_analysis_on_updated_frames() {
        let sql_connection = api::data::sql::connect();
        let symbols = api::data::sql::symbols::active_symbols(sql_connection.clone());
        live_data::run_analysis_on_updated_dataframe(sql_connection.clone(), &symbols);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_analysis_on_historical_data() {
        let sql_connection = api::data::sql::connect();
        let symbols = api::data::sql::symbols::active_symbols(sql_connection.clone());
        run_analysis_on_historical_data(sql_connection.clone(), &symbols);
    }

    #[tokio::test]
    async fn test_charts() {
        let sql_connection = api::data::sql::connect();
        let symbols = api::data::sql::symbols::active_symbols(sql_connection.clone());
        let mut filepath = dirs::home_dir().unwrap().join("stock-analysis-reports");
        let tickers: Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>> =
            std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new()
            ));
        if !filepath.is_dir() {
            match std::fs::create_dir_all(filepath.clone()) {
                Ok(()) => (),
                Err(e) => {
                    tracing::error!("Failed to create directory: {}", e);
                },
            }
            filepath = dirs::home_dir().unwrap();
        }
        match run_ticker_charts(&symbols, &filepath, tickers.clone()) {
            Ok(()) => {},
            Err(e) => tracing::error!("screener process threw error: {}", e),
        }
    }

    #[tokio::test]
    async fn test_portfolio() {
        let sql_connection = api::data::sql::connect();
        let symbols = api::data::sql::symbols::active_symbols(sql_connection.clone());
        let mut filepath = dirs::home_dir().unwrap().join("stock-analysis-reports");
        if !filepath.is_dir() {
            match std::fs::create_dir_all(filepath.clone()) {
                Ok(()) => (),
                Err(e) => {
                    tracing::error!("Failed to create directory: {}", e);
                },
            }
            filepath = dirs::home_dir().unwrap();
        }
        match run_portfolio_analysis(&symbols, &filepath) {
            Ok(()) => {},
            Err(e) => tracing::error!("screener process threw error: {}", e),
        }
    }


    #[tokio::test]
    async fn test_screener() {
        let mut filepath = dirs::home_dir().unwrap().join("stock-analysis-reports");
        if !filepath.is_dir() {
            match std::fs::create_dir_all(filepath.clone()) {
                Ok(()) => (),
                Err(e) => {
                    tracing::error!("Failed to create directory: {}", e);
                },
            }
            filepath = dirs::home_dir().unwrap();
        }
        match super::run_screener_process(&filepath) {
            Ok(()) => {},
            Err(e) => tracing::error!("screener process threw error: {}", e),
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn test_run_jobs() {
        let sql_connection = api::data::sql::connect();

        let tickers: Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>> =
                std::sync::Arc::new(std::sync::Mutex::new(
                    std::collections::HashMap::new()
                ));
        let last_nightly_update: Arc<std::sync::Mutex<chrono::DateTime<Local>>> =
                std::sync::Arc::new(std::sync::Mutex::new(
                    chrono::DateTime::from_timestamp(24 * 3600 * 100,0).unwrap().naive_local().and_local_timezone(chrono::Local).unwrap()
                ));
        match run_jobs(tickers.clone(), sql_connection.clone(), last_nightly_update.clone()).await {
            Ok(()) => {},
            Err(e) => tracing::error!("screener process threw error: {}", e),
        }
    }

}
