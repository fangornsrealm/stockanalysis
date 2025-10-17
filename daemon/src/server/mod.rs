
use polars::prelude::*;
use chrono::{Datelike, offset::Local, Timelike};
use eyre::Result as EyreResult;
use structopt::StructOpt;
use tokio::{
    sync::broadcast,
    time::{self, Duration}
};

use api::prelude::*;

mod charts;
pub use charts::{run_ticker_charts, run_ticker_charts_livedata};
mod daily_data;
pub use daily_data::run_analysis_on_historical_data;
mod live_data;
pub use live_data::{run_analysis_on_updated_dataframe, get_livedata_for_active_symbols};
mod screener;
//pub use screener::run_screener_process;
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
    let mut oldpath = filepath.join(file.to_path_buf());
    let mut newpath = archivepath.join(file.to_path_buf());
    if file.is_absolute() {
        let file_new = match file.file_name() {
            Some(str) => str.to_str().unwrap().to_owned(),
            None => return,
        };
        oldpath = filepath.join(std::path::PathBuf::from(file_new.clone()));
        newpath = archivepath.join(std::path::PathBuf::from(file_new));
    }
    if !oldpath.exists() {
        return;
    }
    if oldpath == newpath {
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
    if newpath.exists() {
        match std::fs::remove_file(newpath.clone()) {
            Ok(()) => (),
            Err(e) => {
                tracing::error!("Failed to remove file: {}", e);
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
    tickers_mutex: Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>>, 
    sql_connection: Arc<std::sync::Mutex<rusqlite::Connection>>,
    last_nightly_update_mutex: Arc<std::sync::Mutex<chrono::DateTime<Local>>>,
) -> EyreResult<()> {
    let now = Local::now();
    if now.weekday().num_days_from_monday() > 4 {
        tracing::debug!("{} Only work on weekdays.", now.naive_local().to_string());
        return Ok(());
    }
    let symbols = api::data::sql::symbols::active_symbols(sql_connection.clone());
    let mut filepath = dirs::home_dir().unwrap().join("stock-analysis-reports");
    if !filepath.is_dir() {
        match std::fs::create_dir_all(filepath.clone()) {
            Ok(()) => (),
            Err(e) => {
                tracing::error!("Failed to create directory: {}", e);
                filepath = dirs::home_dir().unwrap();
            },
        }
    }
    let mut last_nightly_update = match last_nightly_update_mutex.lock() {
        Ok(mutex) => mutex,
        Err(e) => {
            tracing::error!("Failed to lock the mutex on last nighthly update: {}", e);
            return Ok(());
        }
    };
    // Monday to Friday at 23:00 run nightly updates, but only once per day.
    if now.hour() == 23 && last_nightly_update.date_naive() < now.date_naive() {
        tracing::debug!("{} Running nightly updates.", now.naive_local().to_string());
        *last_nightly_update = now.clone();
        std::mem::drop(last_nightly_update);
        tracing::debug!("This update job is marked as done for today.");

        // run daily jobs.
        api::data::livedata::update_nightly(sql_connection.clone(), &symbols);
        tracing::debug!("Done updating the database with daily data.");
        
        //tracing::debug!("For now retrieve the intra day data once a day.");
        //get_livedata_for_active_symbols( sql_connection.clone(), &symbols);
        //run_ticker_charts_livedata(symbolsstrings, filepath, tickers_mutex)?;

        tracing::debug!("Starting generation of ticker charts.");
        match run_ticker_charts(&symbols, &filepath, tickers_mutex.clone()) {
            Ok(()) => {},
            Err(e) => tracing::error!("Update of ticker charts failed: {}", e)
        }
        //tracing::debug!("Starting screener process.");
        //match run_screener_process(&filepath) {
        //    Ok(()) => {},
        //    Err(e) => tracing::error!("Screening for new stocks failed: {}", e)
        //}
        tracing::debug!("Starting portfolio analysis.");
        match run_portfolio_analysis(&symbols, &filepath) {
            Ok(()) => {},
            Err(e) => tracing::error!("Portfolio optimization failed: {}", e)
        }
        tracing::debug!("Starting analysis of historical data.");
        run_analysis_on_historical_data(sql_connection.clone(), &symbols);
    } else {
        // run live updates every minute on Weekdays
        if now.hour() > 6 || now.hour() < 23 {
            //tracing::debug!("{} Skipping operation until there is live data.", now.naive_local().to_string());
            tracing::debug!("Retrieve the intra day data for all active symbols.");
            get_livedata_for_active_symbols( sql_connection.clone(), &symbols);
            
            tracing::debug!("Create all live data charts for all active symbols.");
            let _ret = run_ticker_charts_livedata(&symbols, &filepath, tickers_mutex.clone());
            
            tracing::debug!("Run live analysis on updated data.");
            run_analysis_on_updated_dataframe(sql_connection.clone(), &symbols, tickers_mutex.clone(), &filepath);
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
        let mut interval = time::interval(Duration::from_secs(300)); // update of data takes over four minutes, so we cannot get faster than every five minutes
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
    use tracing_subscriber::{filter, fmt, layer::SubscriberExt, Layer};
    
    #[tokio::test]
    async fn test_get_livedata_for_active_symbols() {
        let journald_layer = tracing_journald::layer().unwrap();
        let stdout_layer = fmt::Layer::default()
            .with_writer(std::io::stdout)
            .with_ansi(false)
            .with_filter(filter::LevelFilter::INFO);
        // Route events to both tokio-console and stdout
        let subscriber = tracing_subscriber::Registry::default()
            .with(journald_layer)
            .with(stdout_layer);

        tracing::subscriber::set_global_default(subscriber).unwrap();
        tracing::info!("test_get_livedata_for_active_symbols");
        let sql_connection = api::data::sql::connect();
        let symbols = api::data::sql::symbols::active_symbols(sql_connection.clone());
        live_data::get_livedata_for_active_symbols(sql_connection.clone(), &symbols);
    }

    #[tokio::test]
    async fn test_analysis_on_updated_frames() {
        let journald_layer = tracing_journald::layer().unwrap();
        let stdout_layer = fmt::Layer::default()
            .with_writer(std::io::stdout)
            .with_ansi(false)
            .with_filter(filter::LevelFilter::INFO);
        // Route events to both tokio-console and stdout
        let subscriber = tracing_subscriber::Registry::default()
            .with(journald_layer)
            .with(stdout_layer);

        tracing::subscriber::set_global_default(subscriber).unwrap();
        tracing::info!("test_analysis_on_updated_frames");
        let tickers: Arc<std::sync::Mutex<std::collections::HashMap<String, Ticker>>> =
                std::sync::Arc::new(std::sync::Mutex::new(
                    std::collections::HashMap::new()
                ));
        let sql_connection = api::data::sql::connect();
        let symbols = api::data::sql::symbols::active_symbols(sql_connection.clone());
        let filepath = dirs::home_dir().unwrap().join("stock-analysis-reports");
        live_data::run_analysis_on_updated_dataframe(sql_connection.clone(), &symbols, tickers.clone(), &filepath);
    }

    #[tokio::test]
    async fn test_analysis_on_historical_data() {
        let journald_layer = tracing_journald::layer().unwrap();
        let stdout_layer = fmt::Layer::default()
            .with_writer(std::io::stdout)
            .with_ansi(false)
            .with_filter(filter::LevelFilter::INFO);
        // Route events to both tokio-console and stdout
        let subscriber = tracing_subscriber::Registry::default()
            .with(journald_layer)
            .with(stdout_layer);

        tracing::subscriber::set_global_default(subscriber).unwrap();
        tracing::info!("test_analysis_on_historical_data");
        let sql_connection = api::data::sql::connect();
        let symbols = api::data::sql::symbols::active_symbols(sql_connection.clone());
        daily_data::run_analysis_on_historical_data(sql_connection.clone(), &symbols);
    }

    #[tokio::test]
    async fn test_charts() {
        let journald_layer = tracing_journald::layer().unwrap();
        let stdout_layer = fmt::Layer::default()
            .with_writer(std::io::stdout)
            .with_ansi(false)
            .with_filter(filter::LevelFilter::INFO);
        // Route events to both tokio-console and stdout
        let subscriber = tracing_subscriber::Registry::default()
            .with(journald_layer)
            .with(stdout_layer);

        tracing::subscriber::set_global_default(subscriber).unwrap();
        tracing::info!("test_charts");
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
        let journald_layer = tracing_journald::layer().unwrap();
        let stdout_layer = fmt::Layer::default()
            .with_writer(std::io::stdout)
            .with_ansi(false)
            .with_filter(filter::LevelFilter::INFO);
        // Route events to both tokio-console and stdout
        let subscriber = tracing_subscriber::Registry::default()
            .with(journald_layer)
            .with(stdout_layer);

        tracing::subscriber::set_global_default(subscriber).unwrap();
        tracing::info!("test_portfolio");
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
        let journald_layer = tracing_journald::layer().unwrap();
        let stdout_layer = fmt::Layer::default()
            .with_writer(std::io::stdout)
            .with_ansi(false)
            .with_filter(filter::LevelFilter::INFO);
        // Route events to both tokio-console and stdout
        let subscriber = tracing_subscriber::Registry::default()
            .with(journald_layer)
            .with(stdout_layer);

        tracing::subscriber::set_global_default(subscriber).unwrap();
        tracing::info!("test_screener");
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
        match screener::run_screener_process(&filepath) {
            Ok(()) => {},
            Err(e) => tracing::error!("screener process threw error: {}", e),
        }
    }

    #[tokio::test]
    async fn test_run_jobs() {
        let journald_layer = tracing_journald::layer().unwrap();
        let stdout_layer = fmt::Layer::default()
            .with_writer(std::io::stdout)
            .with_ansi(false)
            .with_filter(filter::LevelFilter::INFO);
        // Route events to both tokio-console and stdout
        let subscriber = tracing_subscriber::Registry::default()
            .with(journald_layer)
            .with(stdout_layer);

        tracing::subscriber::set_global_default(subscriber).unwrap();
        tracing::info!("test_run_jobs");
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
