use polars::prelude::*;
use chrono::{Datelike, NaiveTime, offset::Local, Timelike};
use eyre::Result as EyreResult;
use std::error::Error;
use structopt::StructOpt;
use tokio::{
    sync::broadcast,
    time::{self, Duration}
};

use api::prelude::*;

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
                log::error!("Failed to create directory: {}", e);
                return;
            },
        }
    }
    match std::fs::hard_link(oldpath.clone(), newpath) {
        Ok(()) => (),
        Err(e) => {
            log::error!("Failed to link file: {}", e);
            return;
        },
    }
    match std::fs::remove_file(oldpath) {
        Ok(()) => (),
        Err(e) => {
            log::error!("Failed to remove file: {}", e);
            return;
        },
    }

}

fn get_chart_daily(ticker: &Ticker) -> Result<DataFrame, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.get_chart_daily()
    )
}

fn candlestick_chart_async(ticker: &Ticker) -> Result<plotly::plot::Plot, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.candlestick_chart(None, None)
    )
}

fn candlestick_chart_live_async(ticker: &Ticker) -> Result<plotly::plot::Plot, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.candlestick_chart_live(None, None)
    )
}

fn run_ticker_charts(
    symbolsstrings: &Vec<String>,
    filepath: &std::path::PathBuf
) -> Result<(), Box<dyn Error>> {
    // 
    let symbols: Vec<&str> = symbolsstrings.iter().map(|s| &**s).collect();
    let days = chrono::Local::now().weekday().num_days_from_monday();
    let three_months_ago = chrono::Local::now().date_naive().checked_sub_days(chrono::Days::new(90)).unwrap();
    let yesterday = chrono::Local::now().date_naive().checked_sub_days(chrono::Days::new(1)).unwrap();
    let date_based_name = if days < 5 {
        format!("archive_{}", yesterday.to_string())
    } else {
        return Ok(());
    };
    let archivepath = filepath.clone().join(date_based_name);

    let mut tickers = Vec::new();
    let start_date = three_months_ago.and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap()).and_utc();
    let end_date = yesterday.and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(23 * 3600 + 59 * 60, 0).unwrap()).and_utc();

    for i in 0..symbols.len() {
        let stock_symbol = symbols[i].to_string();
        let mut ticker: Ticker = api::models::ticker::TickerBuilder::new()
            .ticker(&stock_symbol)
            .start_date(&start_date.naive_utc().to_string())
            .end_date(&end_date.naive_utc().to_string())
            .benchmark_symbol("0H1C")
            .interval(Interval::OneDay)
            .build();

        let df = get_chart_daily(&ticker).unwrap();
        let table = df.to_datatable("ohlcv", true, DataTableFormat::Number);
        let html = table.to_html()?;
        let mut file_name = stock_symbol.clone();
        file_name.extend(".html".chars());
        let path = filepath.clone().join(file_name);
        std::fs::write(&path, &html).expect("Should be able to write to file");
        match candlestick_chart_async(&ticker) {
            Ok(pl) => {
                let mut file_name = stock_symbol.clone();
                file_name.extend("_chart.jpg".chars());
                let path = filepath.clone().join(file_name);
                move_file_to_archive(filepath, &archivepath, &path);
                pl.to_jpeg(&osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
                let html = pl.to_html();
                let mut file_name = stock_symbol.clone();
                file_name.extend("_chart.html".chars());
                let path = filepath.clone().join(file_name);
                move_file_to_archive(filepath, &archivepath, &path);
                std::fs::write(&path, &html).expect("Should be able to write to file");
            },
            Err(error) => {
                log::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
                continue;
            },
        }
        // get only the last stock day.
        // TODO: Replace by live data
        let start_date = yesterday.and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap()).and_utc();
        let end_date = yesterday.and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(23 * 3600 + 59 * 60, 0).unwrap()).and_utc();
        ticker.start_date = start_date.naive_utc().to_string();
        ticker.end_date = end_date.naive_utc().to_string();
        if end_date.timestamp_millis() <= start_date.timestamp_millis() {
            log::error!("timestamps are do not span a time span!");
        }
        match candlestick_chart_live_async(&ticker) {
            Ok(pl) => {
                let mut file_name = stock_symbol.clone();
                file_name.extend("_chart_live.jpg".chars());
                let path = filepath.clone().join(file_name);
                move_file_to_archive(filepath, &archivepath, &path);
                pl.to_jpeg(&osstr_to_string(path.into_os_string()), 1200, 800, 1.0);
                let html = pl.to_html();
                let mut file_name = stock_symbol.clone();
                file_name.extend("_chart_live.html".chars());
                let path = filepath.clone().join(file_name);
                move_file_to_archive(filepath, &archivepath, &path);
                std::fs::write(&path, &html).expect("Should be able to write to file");
            },
            Err(error) => {
                log::error!("Failed to crate chart for ticker {}!: {}", stock_symbol, error);
                continue;
            },
        }
        //println!("{}", html);
        tickers.push(ticker);
        //table.show()?;
    }
    Ok(())
}

fn build_screener(screener: ScreenerBuilder) -> Result<Screener, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        screener.build()
    )
}

fn metrics(screener: Screener) -> Result<DataTable, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        screener.metrics()
    )
}

fn get_ticker(tickers: Tickers, symbol: &str) -> Result<Ticker, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        tickers.get_ticker(symbol)
    )
}

fn optimize(tickers: Tickers, objective: Option<api::prelude::ObjectiveFunction>) -> Result<api::prelude::Portfolio, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        tickers.optimize(objective, None)
    )
}

fn report(ticker: Ticker, reporttype: Option<ReportType>) -> Result<api::reports::tabs::TabbedHtml, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.report(reporttype)
    )
}

fn report_portfolio(ticker: api::prelude::Portfolio, reporttype: Option<ReportType>) -> Result<api::reports::tabs::TabbedHtml, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.report(reporttype)
    )
}

pub fn run_screener_process(filepath: &std::path::PathBuf) -> Result<(), Box<dyn Error>> {
    let days = chrono::Local::now().weekday().num_days_from_monday();
    let yesterday = chrono::Local::now().date_naive().checked_sub_days(chrono::Days::new(1)).unwrap();
    let date_based_name = if days < 5 {
        format!("archive_{}", yesterday.to_string())
    } else {
        return Ok(());
    };
    let archivepath = filepath.clone().join(date_based_name);
    // Screen for Large-Cap NASDAQ Stocks
    let screener = Screener::builder()
        .quote_type(QuoteType::Equity)
        .add_filter(ScreenerFilter::EqStr(
            ScreenerMetric::Equity(EquityScreener::Exchange),
            Exchange::NASDAQ.as_ref()
        ))
        .sort_by(
            ScreenerMetric::Equity(EquityScreener::MarketCapIntraday),
            true
        )
        .size(10);
    let screener = build_screener(screener)?;
    let file_name = "screener_overview.html";
    let path = filepath.clone().join(file_name);
    let overview = screener.clone().overview().to_html();
    match overview {
        Ok(chart) => {
            move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&osstr_to_string(path.into_os_string()), &chart).expect("Should be able to write to file")
        },
        Err(e) => {
            log::error!("Failed to get overview for screener: {e}");
            return Ok(());
        }
    }

    let file_name = "screener_metrics.html";
    let path = filepath.clone().join(file_name);
    let metrics = metrics(screener.clone())?.to_html();
    match metrics {
        Ok(chart) => {
            move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&osstr_to_string(path.into_os_string()), &chart).expect("Should be able to write to file")
        },
        Err(e) => {
            log::error!("Failed to get metrics for screener: {e}");
            return Ok(());
        }
    }

    // Instantiate a Multiple Ticker Object
    let ticker_symbols = screener.symbols.iter()
        .map(|x| x.as_str()).collect::<Vec<&str>>();

    let tickers = api::models::tickers::TickersBuilder::new()
        .tickers(ticker_symbols.clone())
        .start_date("2025-03-01")
        .end_date("2025-09-15")
        .interval(Interval::OneDay)
        .benchmark_symbol("0H1C")
        .confidence_level(0.95)
        .risk_free_rate(0.02)
        .build();

    // Generate a Single Ticker Report
    let symbol = ticker_symbols.first().unwrap();
    let ticker = get_ticker(tickers.clone(), symbol)?;
    let performance = report(ticker.clone(), Some(ReportType::Performance))?.to_html();
    let file_name = "screener_top_performance.html";
    let path = filepath.clone().join(file_name);
    move_file_to_archive(filepath, &archivepath, &path);
    std::fs::write(&osstr_to_string(path.into_os_string()), &performance).expect("Should be able to write to file");
    let financials = report(ticker.clone(), Some(ReportType::Financials))?.to_html();
    let file_name = "screener_financials.html";
    let path = filepath.clone().join(file_name);
    move_file_to_archive(filepath, &archivepath, &path);
    std::fs::write(&osstr_to_string(path.into_os_string()), &financials).expect("Should be able to write to file");
    let options = report(ticker.clone(), Some(ReportType::Options))?.to_html();
    let file_name = "screener_options.html";
    let path = filepath.clone().join(file_name);
    move_file_to_archive(filepath, &archivepath, &path);
    std::fs::write(&osstr_to_string(path.into_os_string()), &options).expect("Should be able to write to file");
    let news = report(ticker.clone(), Some(ReportType::News))?.to_html();
    let file_name = "screescreener_newsner_overview.html";
    let path = filepath.clone().join(file_name);
    move_file_to_archive(filepath, &archivepath, &path);
    std::fs::write(&osstr_to_string(path.into_os_string()), &news).expect("Should be able to write to file");

    // Generate a Multiple Ticker Report
    let report = report(ticker.clone(), Some(ReportType::Performance))?.to_html();
    let file_name = "screener_report.html";
    let path = filepath.clone().join(file_name);
    move_file_to_archive(filepath, &archivepath, &path);
    std::fs::write(&osstr_to_string(path.into_os_string()), &report).expect("Should be able to write to file");

    // Perform a Portfolio Optimization
    let portfolio = optimize(tickers.clone(), Some(ObjectiveFunction::MaxSharpe))?;

    // Generate a Portfolio Report
    let portfolioreport = report_portfolio(portfolio.clone(), Some(ReportType::Performance))?.to_html();
    let file_name = "screener_portfolioreport.html";
    let path = filepath.clone().join(file_name);
    move_file_to_archive(filepath, &archivepath, &path);
    std::fs::write(&osstr_to_string(path.into_os_string()), &portfolioreport).expect("Should be able to write to file");

    // TODO write a HTML file with links to the written HTML files

    // TODO send it via notification and Apple Push notification

    Ok(())
}

pub fn run_analysis_on_updated_dataframe(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>, 
    symbols: &Vec<String>
) {
    let now = Local::now();
    
    for symbol in symbols.iter() {
        let mut vt = Vec::new();
        let mut vv = Vec::new();
        let start_time = NaiveTime::from_num_seconds_from_midnight_opt(0, 0).expect("That should never fail!");
        let end_time = NaiveTime::from_num_seconds_from_midnight_opt(23*3600 + 59*60, 0).expect("That should never fail!");
        let start_date = now.clone().date_naive().and_time(start_time);
        let end_date = now.clone().date_naive().and_time(end_time);
        let ohlcv: polars::prelude::DataFrame = match api::data::sql::to_dataframe::ohlcv_to_dataframe(
            sql_connection.clone(),
            symbol,
            start_date,
            end_date,
        ) {
            Ok(vec) => {
                if vec.len() == 0 {
                    continue;
                }
                let mut df = vec[0].clone();
                match api::data::sql::to_dataframe::i64_column_to_datetime_vec(&df) {
                    Ok(tv) => vt.push(tv),
                    Err(error) => {
                        log::error!("Unable to turn get column timestamp! {:?}", error);
                        continue;
                    }
                };
                match api::data::sql::to_dataframe::f64_column_to_vec(&df, "adjclose") {
                    Ok(av) => vv.push(av),
                    Err(error) => {
                        log::error!("Unable to turn get column adjclose! {:?}", error);
                        continue;
                    }
                };
                if vec.len() > 1 {
                    for i in 1..vec.len() {
                        let dftmp = vec[i].clone();
                        match api::data::sql::to_dataframe::i64_column_to_datetime_vec(&dftmp) {
                            Ok(tv) => vt.push(tv),
                            Err(error) => {
                                log::error!("Unable to turn get column timestamp! {:?}", error);
                                continue;
                            }
                        };
                        match api::data::sql::to_dataframe::f64_column_to_vec(&dftmp, "adjclose") {
                            Ok(av) => vv.push(av),
                            Err(error) => {
                                log::error!("Unable to turn get column adjclose! {:?}", error);
                                continue;
                            }
                        };
                        df = concat([df.lazy(), vec[i].clone().lazy()], UnionArgs::default()).unwrap().collect().unwrap();
                    }
                }
                if df.height() > 0 {
                    df
                } else {
                    // no entries in database or symbol not found, search yahoo instead
                    continue;
                }

            },
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
        let datetimes = match api::data::sql::to_dataframe::i64_column_to_datetime_vec(&ohlcv) {
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
        
        let jumps = api::analytics::detectors::jumps_in_series(symbol, &timestamps, &adjclose, 0.5, 0.3);
        api::data::sql::events::insert_jump_events(sql_connection.clone(), &jumps);
        
        // detect a increasing or decreasing slope and raise a notification
        let slope = api::analytics::detectors::increasing_slope(&vv[vv.len()-1], 0.5, 0.3);
        if slope != 0.0 {
            // send alarm
            let text;
            if slope > 0.0 {
                text = format!("Symbol {} increased by {} at {}!", symbol, slope, datetimes[datetimes.len()-1].to_string());
            } else {
                text = format!("Symbol {} dropped by {} at {}!", symbol, slope, datetimes[datetimes.len()-1].to_string());
            }
            log::warn!("{}", &text);
            match notify_rust::Notification::new()
                .summary("stock-analysis")
                .body(&text)
                .icon("alarm")
                .show()
            {
                Ok(_h) => {},
                Err(e) => log::error!("Failed to notify the desktop user: {}", e),
            }
        }
    }
}

pub fn run_analysis_on_historical_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>, 
    symbols: &Vec<String>
) {
    let now = Local::now();
    
    for symbol in symbols.iter() {
        let mut vt = Vec::new();
        let mut vv = Vec::new();
        let start_time = NaiveTime::from_num_seconds_from_midnight_opt(0, 0).expect("That should never fail!");
        let end_time = NaiveTime::from_num_seconds_from_midnight_opt(23*3600 + 59*60, 0).expect("That should never fail!");
        let start_date = now.clone().date_naive().checked_sub_days(chrono::Days::new(90)).unwrap().and_time(start_time);
        let end_date = now.clone().date_naive().and_time(end_time);
        let ohlcv: polars::prelude::DataFrame = match api::data::sql::to_dataframe::ohlcv_to_dataframe(
            sql_connection.clone(),
            symbol,
            start_date,
            end_date,
        ) {
            Ok(vec) => {
                if vec.len() == 0 {
                    continue;
                }
                let mut df = vec[0].clone();
                match api::data::sql::to_dataframe::i64_column_to_datetime_vec(&df) {
                    Ok(tv) => vt.push(tv),
                    Err(error) => {
                        log::error!("Unable to turn get column timestamp! {:?}", error);
                        continue;
                    }
                };
                match api::data::sql::to_dataframe::f64_column_to_vec(&df, "adjclose") {
                    Ok(av) => vv.push(av),
                    Err(error) => {
                        log::error!("Unable to turn get column adjclose! {:?}", error);
                        continue;
                    }
                };
                if vec.len() > 1 {
                    for i in 1..vec.len() {
                        let dftmp = vec[i].clone();
                        match api::data::sql::to_dataframe::i64_column_to_datetime_vec(&dftmp) {
                            Ok(tv) => vt.push(tv),
                            Err(error) => {
                                log::error!("Unable to turn get column timestamp! {:?}", error);
                                continue;
                            }
                        };
                        match api::data::sql::to_dataframe::f64_column_to_vec(&dftmp, "adjclose") {
                            Ok(av) => vv.push(av),
                            Err(error) => {
                                log::error!("Unable to turn get column adjclose! {:?}", error);
                                continue;
                            }
                        };
                        df = concat([df.lazy(), vec[i].clone().lazy()], UnionArgs::default()).unwrap().collect().unwrap();
                    }
                }
                if df.height() > 0 {
                    df
                } else {
                    // no entries in database or symbol not found, search yahoo instead
                    continue;
                }

            },
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
        let _datetimes = match api::data::sql::to_dataframe::i64_column_to_datetime_vec(&ohlcv) {
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
        // start with a series split per business day
        api::analytics::detectors::cluster_seasonal_data(api::analytics::detectors::vecs_to_slices(&vv));
        let outliers = api::analytics::detectors::outliers(api::analytics::detectors::vecs_to_slices(&vv));
        if outliers.len() > 0 {
            // analyze outliers to find critical events
        }

        let seasonality = api::analytics::detectors::seasonality(&adjclose, 10, 9600, 0.2, false);
        for season_length in seasonality {
            let _s = api::analytics::detectors::split_series_into_seasons(&adjclose, season_length as i64, 1);
            let _outliers = api::analytics::detectors::outliers(api::analytics::detectors::vecs_to_slices(&vv));
        }
        
        let changepoints = api::analytics::detectors::changepoints(&adjclose, true);
        for _changepoint in changepoints {
            // analyze changepoints
        }

        let jumps = api::analytics::detectors::jumps_in_series(symbol, &timestamps, &adjclose, 0.5, 0.3);
        api::data::sql::events::insert_jump_events(sql_connection.clone(), &jumps);
        
    }
}

pub async fn run_jobs() -> EyreResult<()> {
    let now = Local::now();
    let sql_connection = api::data::sql::connect();
    let symbols = api::data::sql::symbols::active_symbols(sql_connection.clone());
    let mut filepath = dirs::home_dir().unwrap().join("stock-analysis-reports");
    if !filepath.is_dir() {
        match std::fs::create_dir_all(filepath.clone()) {
            Ok(()) => (),
            Err(e) => {
                log::error!("Failed to create directory: {}", e);
            },
        }
        filepath = dirs::home_dir().unwrap();
    }
    if now.hour() == 23 && now.minute() == 0 {
        // run daily jobs.
        api::data::livedata::update_nightly(sql_connection.clone(), &symbols);
        
        // temporarily get the minutely data also once per day until there is a subscription with live-data access
        let start_time = NaiveTime::from_num_seconds_from_midnight_opt(7*3600, 0).expect("That should never fail!");
        let end_time = NaiveTime::from_num_seconds_from_midnight_opt(22*3600, 0).expect("That should never fail!");
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
                    log::error!("Failed to retrieve data for symbol {} from provider! {}", symbol, e);
                    continue;
                },
            }
        }
        run_analysis_on_historical_data(sql_connection.clone(), &symbols);

        let _ret = run_screener_process(&filepath);

        let _ret = run_ticker_charts(&symbols, &filepath);

    } else {
        // run live updates every minute on Weekdays
        if now.weekday().num_days_from_monday() < 5 {
            if now.hour() > 6 || now.hour() < 22 {
                //get_livedata_active_symbols(sql_connection.clone(), &symbols);

                // triger the live analysis and event detection
                run_analysis_on_updated_dataframe(sql_connection.clone(), &symbols);

                
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
            tokio::spawn(run_jobs());
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
    async fn test_analysis_on_updated_frames() {
        let sql_connection = api::data::sql::connect();
        let symbols = api::data::sql::symbols::active_symbols(sql_connection.clone());
        run_analysis_on_updated_dataframe(sql_connection.clone(), &symbols);
    }

    #[tokio::test]
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
        if !filepath.is_dir() {
            match std::fs::create_dir_all(filepath.clone()) {
                Ok(()) => (),
                Err(e) => {
                    log::error!("Failed to create directory: {}", e);
                },
            }
            filepath = dirs::home_dir().unwrap();
        }
        match run_ticker_charts(&symbols, &filepath) {
            Ok(()) => {},
            Err(e) => log::error!("screener process threw error: {}", e),
        }
    }

        #[tokio::test]
    async fn test_screener() {
        let mut filepath = dirs::home_dir().unwrap().join("stock-analysis-reports");
        if !filepath.is_dir() {
            match std::fs::create_dir_all(filepath.clone()) {
                Ok(()) => (),
                Err(e) => {
                    log::error!("Failed to create directory: {}", e);
                },
            }
            filepath = dirs::home_dir().unwrap();
        }
        match run_screener_process(&filepath) {
            Ok(()) => {},
            Err(e) => log::error!("screener process threw error: {}", e),
        }
    }

    #[tokio::test]
    async fn test_run_jobs() {
        match run_jobs().await {
            Ok(()) => {},
            Err(e) => log::error!("screener process threw error: {}", e),
        }
    }

}
