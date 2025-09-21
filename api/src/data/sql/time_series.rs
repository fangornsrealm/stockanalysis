use rusqlite::params;

pub fn timeseries_count(
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
    let query = "SELECT COUNT(timestamp) FROM time_series WHERE symbol = ?1";
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

pub fn timeseries(
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
    let start = metadata.start_date.timestamp();
    let end = metadata.end_date.timestamp();
    let query = "SELECT timestamp, open, high, low, close, volume FROM time_series WHERE symbol = ?1 AND timestamp BETWEEN ?2 AND ?3 ORDER BY timestamp ASC";
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
                                        log::error!("Failed to read datetime for file: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(1) {
                                    Ok(val) => s.open = val,
                                    Err(error) => {
                                        log::error!("Failed to read open for file: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(2) {
                                    Ok(val) => s.high = val,
                                    Err(error) => {
                                        log::error!("Failed to read high for file: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(3) {
                                    Ok(val) => s.low = val,
                                    Err(error) => {
                                        log::error!("Failed to read low for file: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(4) {
                                    Ok(val) => s.close = val,
                                    Err(error) => {
                                        log::error!("Failed to read close for file: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(4) {
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

    t
}

pub fn insert_timeseries(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &super::MetaData,
    series: &market_data::MarketSeries,
) -> u32 {
    let existing = timeseries(sql_connection.clone(), metadata);
    let exists: std::collections::BTreeSet<i64> = existing.iter().map(|t| t.datetime).collect();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return 0;
        }
    };

    for i in 0..series.data.len() {
        let time= chrono::NaiveTime::from_num_seconds_from_midnight_opt(22 * 3600, 0).expect("If this does not work the chrono code is crap!");
        let timestamp = series.data[i].date.and_time(time).and_utc().timestamp();
        if exists.contains(&timestamp) {
            continue;
        }
        match connection.execute(
            "INSERT INTO time_series (timestamp, symbol, currency, exchange, open, high, low, close, volume) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![&timestamp, &metadata.symbol, &metadata.currency, &metadata.exchange, &series.data[i].open, &series.data[i].high, &series.data[i].low, &series.data[i].close, &series.data[i].volume ],
        ) {
            Ok(_retval) => {} //log::warn!("Inserted {} video with ID {} and location {} into candidates.", video.id, video.index, candidate_id),
            Err(error) => {
                log::error!("Failed insert time_series! {}", error);
                return 0;
            }
        }
    }
    1
}

pub fn _delete_timeseries(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &super::MetaData,
    _timeseries: &market_data::MarketSeries,
) {
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return;
        }
    };
    let _ret = connection.execute(
        "DELETE FROM time_series WHERE symbol = ?1",
        params![&metadata.symbol],
    );
}

pub fn _update_timeseries(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    metadata: &super::MetaData,
    timeseries: &market_data::MarketSeries,
) {
    _delete_timeseries(sql_connection.clone(), metadata, timeseries);
    insert_timeseries(sql_connection.clone(), metadata, timeseries);
}
