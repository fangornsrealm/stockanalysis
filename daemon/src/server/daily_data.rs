use chrono::{NaiveTime, offset::Local};
use api::data::dataframes::{i64_column_to_datetime_vec, i64_column_to_vec, f64_column_to_vec};

#[allow(unreachable_code, unused_variables, dead_code)]
pub fn run_analysis_on_historical_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>, 
    symbols: &Vec<String>
) {
    let now = Local::now();
    
    for symbol in symbols.iter() {
        let start_time = NaiveTime::from_num_seconds_from_midnight_opt(0, 0).expect("That should never fail!");
        let end_time = NaiveTime::from_num_seconds_from_midnight_opt(23*3600 + 59*60, 0).expect("That should never fail!");
        let start_date = now.clone().date_naive().checked_sub_days(chrono::Days::new(90)).unwrap().and_time(start_time).and_utc();
        let end_date = now.clone().date_naive().and_time(end_time).and_utc();
        let ohlcv: polars::prelude::DataFrame = match api::data::sql::to_dataframe::daily_ohlcv_to_dataframe(
            sql_connection.clone(),
            symbol,
            start_date,
            end_date,
        ) {
            Ok(df) => {
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
        let timestamps = match i64_column_to_vec(&ohlcv, "timestamp") {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn get column timestamp! {:?}", error);
                continue;
            }
        };
        let _datetimes = match i64_column_to_datetime_vec(&ohlcv) {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn timestamps into dates! {:?}", error);
                continue;
            }
        };
        let adjclose = match f64_column_to_vec(&ohlcv, "adjclose") {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn get column adjclose! {:?}", error);
                continue;
            }
        };
        /*
        // start with a series split per business day
        let _clusters = api::analytics::detectors::cluster_seasonal_data(api::analytics::detectors::vecs_to_slices(&vv));

        let outliers = api::analytics::detectors::outliers(api::analytics::detectors::vecs_to_slices(&vv));
        if outliers.len() > 0 {
            // analyze outliers to find critical events
        }

        let seasonality = api::analytics::detectors::seasonality(&adjclose, 10, 9600, 0.1, false);
        for season_length in seasonality {
            let _s = api::analytics::detectors::split_series_into_seasons(&adjclose, season_length as i64, 1);
            let _outliers = api::analytics::detectors::outliers(api::analytics::detectors::vecs_to_slices(&vv));
        }
        
        let changepoints = api::analytics::detectors::changepoints(&adjclose, false);
        for _changepoint in changepoints {
            // analyze changepoints
        }
        */
        let jumps = api::analytics::detectors::jumps_in_series(symbol, &timestamps, &adjclose, 4.0, 4.0);
        if jumps.len() > 0 {
            api::data::sql::events::insert_jump_events(sql_connection.clone(), &jumps);
        }
        // max one day of seasonality
        // for each season look for changepoints
        // and before that point analyze for increasing or decreasing slope
        let seasonality = api::analytics::detectors::seasonality(&adjclose, 10, 960, 0.2, false);
        for season_length in seasonality {
            //tracing::debug!("checking seasonality for symbol {} every {} minutes.", symbol, season_length);
            let s = api::analytics::detectors::split_series_into_seasons(&adjclose, season_length as i64, 1);
            let t = api::analytics::detectors::split_series_into_seasons(&timestamps, season_length as i64, 1);
            for i in 0..s.len() {
                let (local_min, local_max) = api::analytics::detectors::local_min_max(&s[i]);
                for j in 0..local_min.len() {
                    let (start, end) = if local_min[j] < local_max[j] {
                        (local_min[j], local_max[j])
                    } else {
                        (local_max[j], local_min[j])
                    };
                    let slice = s[i][start..end].iter().map(|x| x.to_owned()).collect();
                    //tracing::debug!("checking increasing slope for slice {} of {} from {} to {}.", i, s.len(), start, end);
                    let (slope, pos) = api::analytics::detectors::find_increasing_slope(&slice, 0.5, 0.3);
                    if slope > 0.0 {
                        let datetime = chrono::DateTime::from_timestamp_millis(t[i][pos]).unwrap().naive_utc();
                        tracing::debug!("At timestamp {} the series has increasing slope of {} ", datetime.to_string(), slope);
                    }
                }
            }
        }

    }
}
