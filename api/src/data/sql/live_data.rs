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
            log::error!("Failed to lock sql connection for use! {}", error);
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
                                    log::error!("Failed to read datetime for file: {}", error);
                                    continue;
                                }
                            },
                            Ok(None) => {
                                //log::warn!("No data read from indices.");
                                break;
                            }
                            Err(error) => {
                                log::error!("Failed to read a row from indices: {}", error);
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    log::error!(
                        "could not read line from videostore_indices database: {}",
                        err
                    );
                }
            }
        }
        Err(err) => {
            log::error!("could not prepare SQL statement: {}", err);
        }
    }
    num
}

pub fn live_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &super::MetaData,
) -> Vec<super::TimeSeriesData> {
    let mut t = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
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
                                        log::error!(
                                            "Failed to read datetime for live_data: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(1) {
                                    Ok(val) => s.open = val,
                                    Err(error) => {
                                        log::error!("Failed to read open for live_data: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(2) {
                                    Ok(val) => s.high = val,
                                    Err(error) => {
                                        log::error!("Failed to read high for live_data: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(3) {
                                    Ok(val) => s.low = val,
                                    Err(error) => {
                                        log::error!("Failed to read low for live_data: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(4) {
                                    Ok(val) => s.close = val,
                                    Err(error) => {
                                        log::error!(
                                            "Failed to read close for live_data: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(5) {
                                    Ok(val) => s.volume = val,
                                    Err(error) => {
                                        log::error!("Failed to read volume for file: {}", error);
                                        continue;
                                    }
                                }
                                t.push(s);
                            }
                            Ok(None) => {
                                //log::warn!("No data read from indices.");
                                break;
                            }
                            Err(error) => {
                                log::error!("Failed to read a row from live_data: {}", error);
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    log::error!("could not read line from live_data database: {}", err);
                }
            }
        }
        Err(err) => {
            log::error!("could not prepare SQL statement: {}", err);
        }
    }

    t
}

pub fn insert_live_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &super::MetaData,
    series: &market_data::EnhancedMarketSeries,
) -> Vec<super::TimeSeriesData> {
    let existing = live_data(sql_connection.clone(), metadata);
    let exists: std::collections::BTreeSet<i64> = existing.iter().map(|t| t.datetime).collect();
    let mut v = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return v;
        }
    };
    let num_values = series.series.len();
    let base_timestamp = chrono::Utc::now().timestamp();
    for i in 0..num_values {
        let timestamp = base_timestamp - (num_values - i) as i64 * 60;
        if exists.contains(&timestamp) {
            continue;
        }
        let mut sma = 0.0_f32;
        let mut ema = 0.0_f32;
        let mut rsi = 0.0_f32;
        let mut stochastic = 0.0_f32;
        let mut macd_value = 0.0_f32;
        let mut signal_value = 0.0_f32;
        let mut hist_value = 0.0_f32;
        for (_indicator_name, indicator_values) in &series.indicators.sma {
            if let Some(value) = indicator_values.get(i) {
                sma = value.to_owned();
            }
        }

        for (_indicator_name, indicator_values) in &series.indicators.ema {
            if let Some(value) = indicator_values.get(i) {
                ema = value.to_owned();
            }
        }

        for (_indicator_name, indicator_values) in &series.indicators.rsi {
            if let Some(value) = indicator_values.get(i) {
                rsi = value.to_owned();
            }
        }

        for (_indicator_name, indicator_values) in &series.indicators.stochastic {
            if let Some(value) = indicator_values.get(i) {
                stochastic = value.to_owned();
            }
        }
        for (_indicator_name, (macd, signal, histogram)) in &series.indicators.macd {
            if let Some(val1) = macd.get(i) {
                if let Some(val2) = signal.get(i) {
                    if let Some(val3) = histogram.get(i) {
                        macd_value = val1.to_owned();
                        signal_value = val2.to_owned();
                        hist_value = val3.to_owned();
                    }
                }
            }
        }
        match connection.execute(
            "INSERT INTO live_data (timestamp, symbol, currency, exchange, open, high, low, close, volume, sma, ema, rsi, stochastic, macd_value, signal_value, hist_value ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![&timestamp, &metadata.symbol, &metadata.currency, &metadata.exchange, &series.series[i].open, &series.series[i].high, &series.series[i].low, &series.series[i].close, &series.series[i].volume, &sma, &ema, &rsi, &stochastic, &macd_value, &signal_value, &hist_value ],
        ) {
            Ok(_retval) => {} //log::warn!("Inserted {} video with ID {} and location {} into candidates.", video.id, video.index, candidate_id),
            Err(error) => {
                log::error!("Failed insert live_data! {}", error);
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
            log::error!("Failed to lock sql connection for use! {}", error);
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
