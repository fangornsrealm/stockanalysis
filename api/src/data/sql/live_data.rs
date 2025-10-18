use chrono::Timelike;
use rusqlite::params;

/// return the number of time series data for the stock
pub fn live_data_count(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &super::MetaData,
) -> usize {
    let mut num = 0_usize;
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            tracing::error!("Failed to lock sql connection for use! {}", error);
            return 0;
        }
    };
    let query = "SELECT COUNT(timestamp) FROM live_data WHERE symbol = ?1";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![&metadata.symbol]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => match row.get(0) {
                                Ok(val) => num = val,
                                Err(error) => {
                                    tracing::error!("Failed to read datetime for file: {}", error);
                                    continue;
                                }
                            },
                            Ok(None) => {
                                //tracing::debug!("No data read from indices.");
                                break;
                            }
                            Err(error) => {
                                tracing::error!("Failed to read a row from indices: {}", error);
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(
                        "could not read line from videostore_indices database: {}",
                        err
                    );
                }
            }
        }
        Err(err) => {
            tracing::error!("could not prepare SQL statement: {}", err);
        }
    }
    num
}

pub fn live_data_all(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &super::MetaData,
) -> Vec<super::TimeSeriesData> {
    let mut t = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            tracing::error!("Failed to lock sql connection for use! {}", error);
            return t;
        }
    };
    let query = "SELECT timestamp, open, high, low, close, volume FROM live_data WHERE symbol = ?1 ORDER BY timestamp ASC";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![&metadata.symbol]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => {
                                let mut s = super::TimeSeriesData {
                                    ..Default::default()
                                };
                                match row.get(0) {
                                    Ok(val) => s.datetime = val,
                                    Err(error) => {
                                        tracing::error!(
                                            "Failed to read datetime for live_data: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(1) {
                                    Ok(val) => s.open = val,
                                    Err(error) => {
                                        tracing::error!("Failed to read open for live_data: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(2) {
                                    Ok(val) => s.high = val,
                                    Err(error) => {
                                        tracing::error!("Failed to read high for live_data: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(3) {
                                    Ok(val) => s.low = val,
                                    Err(error) => {
                                        tracing::error!("Failed to read low for live_data: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(4) {
                                    Ok(val) => s.close = val,
                                    Err(error) => {
                                        tracing::error!(
                                            "Failed to read close for live_data: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(5) {
                                    Ok(val) => s.volume = val,
                                    Err(error) => {
                                        tracing::error!("Failed to read volume for file: {}", error);
                                        continue;
                                    }
                                }
                                t.push(s);
                            }
                            Ok(None) => {
                                //tracing::debug!("No data read from indices.");
                                break;
                            }
                            Err(error) => {
                                tracing::error!("Failed to read a row from live_data: {}", error);
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::error!("could not read line from live_data database: {}", err);
                }
            }
        }
        Err(err) => {
            tracing::error!("could not prepare SQL statement: {}", err);
        }
    }

    t
}

fn timestamps_daily(start_date: &chrono::DateTime<chrono::Utc>, offset: i64) -> (i64, i64) {
    let day = match start_date.checked_add_days(chrono::Days::new(offset as u64)) {
        Some(d) => d,
        None => return (0,0),
    }.date_naive();
    let start_time = day.and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap());
    let end_time = day.and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(23*3600 + 59*60, 0).unwrap());
    (start_time.and_utc().timestamp(), end_time.and_utc().timestamp())
}

pub fn live_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &super::MetaData,
) -> Vec<Vec<super::TimeSeriesData>> {
    let mut v = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            tracing::error!("Failed to lock sql connection for use! {}", error);
            return v;
        }
    };
    let num_days = (metadata.end_date - metadata.start_date).num_days() + 1;
    for i in 0..num_days {
        let mut t = Vec::new();
        let (start, end) = timestamps_daily(&metadata.start_date, i);
        if start == 0 && end == 0 {
            continue;
        }
        let query = "SELECT timestamp, open, high, low, close, volume FROM live_data WHERE symbol = ?1 AND timestamp BETWEEN ?2 AND ?3 ORDER BY timestamp ASC";
        match connection.prepare(query) {
            Ok(mut statement) => {
                match statement.query(params![&metadata.symbol, &start, &end]) {
                    Ok(mut rows) => {
                        loop {
                            match rows.next() {
                                Ok(Some(row)) => {
                                    let mut s = super::TimeSeriesData {
                                        ..Default::default()
                                    };
                                    match row.get(0) {
                                        Ok(val) => s.datetime = val,
                                        Err(error) => {
                                            tracing::error!(
                                                "Failed to read datetime for live_data: {}",
                                                error
                                            );
                                            continue;
                                        }
                                    }
                                    match row.get(1) {
                                        Ok(val) => s.open = val,
                                        Err(error) => {
                                            tracing::error!("Failed to read open for live_data: {}", error);
                                            continue;
                                        }
                                    }
                                    match row.get(2) {
                                        Ok(val) => s.high = val,
                                        Err(error) => {
                                            tracing::error!("Failed to read high for live_data: {}", error);
                                            continue;
                                        }
                                    }
                                    match row.get(3) {
                                        Ok(val) => s.low = val,
                                        Err(error) => {
                                            tracing::error!("Failed to read low for live_data: {}", error);
                                            continue;
                                        }
                                    }
                                    match row.get(4) {
                                        Ok(val) => s.close = val,
                                        Err(error) => {
                                            tracing::error!(
                                                "Failed to read close for live_data: {}",
                                                error
                                            );
                                            continue;
                                        }
                                    }
                                    match row.get(5) {
                                        Ok(val) => s.volume = val,
                                        Err(error) => {
                                            tracing::error!("Failed to read volume for file: {}", error);
                                            continue;
                                        }
                                    }
                                    t.push(s);
                                }
                                Ok(None) => {
                                    //tracing::debug!("No data read from indices.");
                                    break;
                                }
                                Err(error) => {
                                    tracing::error!("Failed to read a row from live_data: {}", error);
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        tracing::error!("could not read line from live_data database: {}", err);
                    }
                }
            }
            Err(err) => {
                tracing::error!("could not prepare SQL statement: {}", err);
            }
        }
        v.push(t);
    }

    v
}

pub fn all_live_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &super::MetaData,
) -> Vec<super::TimeSeriesData> {
    let mut t = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            tracing::error!("Failed to lock sql connection for use! {}", error);
            return t;
        }
    };
    let start = metadata.start_date.timestamp();
    let end = metadata.end_date.timestamp();
    let query = "SELECT timestamp, open, high, low, close, volume FROM live_data WHERE symbol = ?1 AND timestamp BETWEEN ?2 AND ?3 ORDER BY timestamp ASC";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![&metadata.symbol, &start, &end]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => {
                                let mut s = super::TimeSeriesData {
                                    ..Default::default()
                                };
                                match row.get(0) {
                                    Ok(val) => s.datetime = val,
                                    Err(error) => {
                                        tracing::error!(
                                            "Failed to read datetime for live_data: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(1) {
                                    Ok(val) => s.open = val,
                                    Err(error) => {
                                        tracing::error!("Failed to read open for live_data: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(2) {
                                    Ok(val) => s.high = val,
                                    Err(error) => {
                                        tracing::error!("Failed to read high for live_data: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(3) {
                                    Ok(val) => s.low = val,
                                    Err(error) => {
                                        tracing::error!("Failed to read low for live_data: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(4) {
                                    Ok(val) => s.close = val,
                                    Err(error) => {
                                        tracing::error!(
                                            "Failed to read close for live_data: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(5) {
                                    Ok(val) => s.volume = val,
                                    Err(error) => {
                                        tracing::error!("Failed to read volume for file: {}", error);
                                        continue;
                                    }
                                }
                                t.push(s);
                            }
                            Ok(None) => {
                                //tracing::debug!("No data read from indices.");
                                break;
                            }
                            Err(error) => {
                                tracing::error!("Failed to read a row from live_data: {}", error);
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::error!("could not read line from live_data database: {}", err);
                }
            }
        }
        Err(err) => {
            tracing::error!("could not prepare SQL statement: {}", err);
        }
    }

    t
}

pub fn get_stock_metadata(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbol: &str,
) -> crate::data::sql::MetaData {
    let mut metadata = crate::data::sql::MetaData {..Default::default()};
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            tracing::error!("Failed to lock sql connection for use! {}", error);
            return metadata;
        }
    };
    // get the latest entry for this symbol
    let query = "SELECT currency, exchange FROM live_data WHERE symbol = ?1 ORDER BY timestamp_id DESC LIMIT 1";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![symbol]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => {
                                match row.get(0) {
                                    Ok(val) => metadata.currency = val,
                                    Err(error) => {
                                        tracing::error!(
                                            "Failed to read currency for live_data: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(1) {
                                    Ok(val) => metadata.exchange = val,
                                    Err(error) => {
                                        tracing::error!("Failed to read exchange for live_data: {}", error);
                                        continue;
                                    }
                                }
                            }
                            Ok(None) => {
                                //tracing::debug!("No data read from indices.");
                                return metadata;
                            }
                            Err(error) => {
                                tracing::error!("Failed to read a row from live_data: {}", error);
                                return metadata;
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::error!("could not read line from live_data database: {}", err);
                }
            }
        }
        Err(err) => {
            tracing::error!("could not prepare SQL statement: {}", err);
        }
    }
    // get the stock details for this symbol
    let query = "SELECT name, mic_code FROM stocks WHERE symbol = ?1 AND exchange = ?2 AND currency = ?3";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![symbol, &metadata.exchange, &metadata.currency]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => {
                                match row.get(0) {
                                    Ok(val) => metadata.name = val,
                                    Err(error) => {
                                        tracing::error!(
                                            "Failed to read name for stocks: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(1) {
                                    Ok(val) => metadata.exchange_code = val,
                                    Err(error) => {
                                        tracing::error!("Failed to read mic_code for stocks: {}", error);
                                        continue;
                                    }
                                }
                            }
                            Ok(None) => {
                                //tracing::debug!("No data read from indices.");
                                return metadata;
                            }
                            Err(error) => {
                                tracing::error!("Failed to read a row from stocks: {}", error);
                                return metadata;
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::error!("could not read line from stocks database: {}", err);
                }
            }
        }
        Err(err) => {
            tracing::error!("could not prepare SQL statement: {}", err);
        }
    }
    // get the exchange details for this symbol
    let query = "SELECT code, timezome FROM exchanges WHERE name = ?1";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![&metadata.exchange]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => {
                                match row.get(0) {
                                    Ok(val) => metadata.exchange_code = val,
                                    Err(error) => {
                                        tracing::error!(
                                            "Failed to read code for exchanges: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(1) {
                                    Ok(val) => metadata.exchange_timezone = val,
                                    Err(error) => {
                                        tracing::error!("Failed to read timezone for exchanges: {}", error);
                                        continue;
                                    }
                                }
                            }
                            Ok(None) => {
                                //tracing::debug!("No data read from indices.");
                                return metadata;
                            }
                            Err(error) => {
                                tracing::error!("Failed to read a row from exchanges: {}", error);
                                return metadata;
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::error!("could not read line from exchanges database: {}", err);
                }
            }
        }
        Err(err) => {
            tracing::error!("could not prepare SQL statement: {}", err);
        }
    }
    metadata
}

pub fn insert_live_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &super::MetaData,
    series: &market_data::EnhancedMarketSeries,
) -> Vec<super::TimeSeriesData> {
    let existing = all_live_data(sql_connection.clone(), metadata);
    let exists: std::collections::BTreeSet<i64> = existing.iter().map(|t| t.datetime).collect();
    let mut v = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            tracing::error!("Failed to lock sql connection for use! {}", error);
            return v;
        }
    };
    // Calculate the time stamp for the first entry in the list
    // List length depends on how many minutes were available
    let seconds_from_midnight = chrono::Utc::now().num_seconds_from_midnight();
    // round to the last full minute and subtract the number of entries per minute
    let minutes_from_midnight = seconds_from_midnight / 60;
    let seconds_from_midnight_normalized = (minutes_from_midnight - series.series.len() as u32) * 60;
    let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(seconds_from_midnight_normalized, 0).unwrap();
    let base_timestamp = chrono::Utc::now().date_naive().and_time(time).and_utc().timestamp();
    for i in 0..series.series.len() {
        let timestamp = base_timestamp + i as i64 * 60;
        if exists.contains(&timestamp) {
            continue;
        }
        let sma = 0.0_f32;
        let ema = 0.0_f32;
        let rsi = 0.0_f32;
        let stochastic = 0.0_f32;
        let macd_value = 0.0_f32;
        let signal_value = 0.0_f32;
        let hist_value = 0.0_f32;
        match connection.execute(
            "INSERT INTO live_data (timestamp, symbol, currency, exchange, open, high, low, close, volume, sma, ema, rsi, stochastic, macd_value, signal_value, hist_value ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![&timestamp, &metadata.symbol, &metadata.currency, &metadata.exchange, &series.series[i].open, &series.series[i].high, &series.series[i].low, &series.series[i].close, &series.series[i].volume, &sma, &ema, &rsi, &stochastic, &macd_value, &signal_value, &hist_value ],
        ) {
            Ok(_retval) => {} //tracing::debug!("Inserted {} video with ID {} and location {} into candidates.", video.id, video.index, candidate_id),
            Err(error) => {
                tracing::error!("Failed insert live_data for symbol {}! {}", metadata.symbol, error);
                return v;
            }
        }
        let t = super::TimeSeriesData {
            datetime: timestamp,
            open: series.series[i].open as f64,
            high: series.series[i].high as f64,
            low: series.series[i].low as f64,
            close: series.series[i].close as f64,
            volume: series.series[i].volume as f64,
        };
        v.push(t);
    }
    v
}

pub fn _delete_live_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &super::MetaData,
    _timeseries: &market_data::EnhancedMarketSeries,
) {
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            tracing::error!("Failed to lock sql connection for use! {}", error);
            return;
        }
    };
    let _ret = connection.execute(
        "DELETE FROM live_data WHERE symbol = ?1",
        params![&metadata.symbol],
    );
}

pub fn _update_live_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &super::MetaData,
    timeseries: &market_data::EnhancedMarketSeries,
) {
    _delete_live_data(sql_connection.clone(), metadata, timeseries);
    insert_live_data(sql_connection.clone(), metadata, timeseries);
}
