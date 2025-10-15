use polars::prelude::*;
use chrono::{NaiveTime, offset::Local};

#[allow(unreachable_code, unused_variables, dead_code)]
pub fn run_analysis_on_updated_dataframe(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>, 
    symbols: &Vec<String>
) {
    return; // TODO disable this function until we have a live data subscription
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
                        tracing::error!("Unable to turn get column timestamp! {:?}", error);
                        continue;
                    }
                };
                match api::data::sql::to_dataframe::f64_column_to_vec(&df, "adjclose") {
                    Ok(av) => vv.push(av),
                    Err(error) => {
                        tracing::error!("Unable to turn get column adjclose! {:?}", error);
                        continue;
                    }
                };
                if vec.len() > 1 {
                    for i in 1..vec.len() {
                        let dftmp = vec[i].clone();
                        match api::data::sql::to_dataframe::i64_column_to_datetime_vec(&dftmp) {
                            Ok(tv) => vt.push(tv),
                            Err(error) => {
                                tracing::error!("Unable to turn get column timestamp! {:?}", error);
                                continue;
                            }
                        };
                        match api::data::sql::to_dataframe::f64_column_to_vec(&dftmp, "adjclose") {
                            Ok(av) => vv.push(av),
                            Err(error) => {
                                tracing::error!("Unable to turn get column adjclose! {:?}", error);
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
                tracing::error!("Failed to get dataframe from database for symbol {}: {}", symbol, e);
                continue;
            }
        };
        let timestamps = match api::data::sql::to_dataframe::i64_column_to_vec(&ohlcv, "timestamp") {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn get column timestamp! {:?}", error);
                continue;
            }
        };
        let datetimes = match api::data::sql::to_dataframe::i64_column_to_datetime_vec(&ohlcv) {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn timestamps into dates! {:?}", error);
                continue;
            }
        };
        let adjclose = match api::data::sql::to_dataframe::f64_column_to_vec(&ohlcv, "adjclose") {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn get column adjclose! {:?}", error);
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
            tracing::debug!("{}", &text);
            match notify_rust::Notification::new()
                .summary("stock-analysis")
                .body(&text)
                .icon("alarm")
                .show()
            {
                Ok(_h) => {},
                Err(e) => tracing::error!("Failed to notify the desktop user: {}", e),
            }
        }
    }
}

#[allow(unreachable_code, unused_variables, dead_code)]
pub fn get_livedata_for_active_symbols(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>, 
    symbols: &Vec<String>
) {
    return;  // TODO disable this function until we have a live data subscription
    let nowish = Local::now();
    let start_time = NaiveTime::from_num_seconds_from_midnight_opt(0, 0).expect("That should never fail!");
    let end_time = NaiveTime::from_num_seconds_from_midnight_opt(23*3600 + 59*60, 0).expect("That should never fail!");
    let start_date = nowish.clone().date_naive().checked_sub_days(chrono::Days::new(90)).unwrap().and_time(start_time);
    let end_date = nowish.clone().date_naive().and_time(end_time);
    for symbol in symbols.iter() {
        let mut metadata = api::data::sql::metadata(sql_connection.clone(), "XFRA", symbol);
        metadata.start_date = start_date.and_utc();
        metadata.end_date = end_date.and_utc();
        let livedata = api::data::sql::live_data::live_data(sql_connection.clone(), &metadata);
        if livedata.len() > 0 {
            if livedata[0].len() > 0 {
                let last_timestamp = livedata[0][livedata[0].len() - 1].datetime * 1000;
                let last_timestamp = chrono::DateTime::from_timestamp_millis(last_timestamp).unwrap();
                let nower = Local::now().naive_utc().and_utc();
                let steps = (nower - last_timestamp).num_minutes() / 60;
                let start_time = chrono::DateTime::from_timestamp_millis(last_timestamp.timestamp_millis() + 60).unwrap();
                let end_time = chrono::DateTime::from_timestamp_millis(last_timestamp.timestamp_millis() + 60 * steps).unwrap();
                let serieses = api::data::livedata::live_data(symbol, start_time.naive_utc(), end_time.naive_utc()).unwrap();
                for series in serieses {
                    api::data::sql::live_data::insert_live_data(sql_connection.clone(), &metadata, &series);
                }
            }
        }
    }
}
