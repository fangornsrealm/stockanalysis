use rusqlite::params;

pub fn jump_events_count(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbol: &str
) -> usize {
    let mut num = 0_usize;
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return 0;
        }
    };
    let query = "SELECT COUNT(timestamp) FROM jump_events WHERE symbol = ?1";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![symbol]) {
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

pub fn jump_events(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbol: &str
) -> Vec<super::JumpEventData> {
    let mut t = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return t;
        }
    };
    let query = "SELECT timestamp, symbol, percent FROM jump_events WHERE symbol = ?1 ORDER BY timestamp ASC";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![symbol]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => {
                                let mut s = super::JumpEventData {
                                    ..Default::default()
                                };
                                match row.get(0) {
                                    Ok(val) => s.datetime = val,
                                    Err(error) => {
                                        log::error!(
                                            "Failed to read datetime for jump_events: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(1) {
                                    Ok(val) => s.symbol = val,
                                    Err(error) => {
                                        log::error!("Failed to read open for jump_events: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(2) {
                                    Ok(val) => s.percent = val,
                                    Err(error) => {
                                        log::error!("Failed to read high for jump_events: {}", error);
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
                                log::error!("Failed to read a row from jump_events: {}", error);
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    log::error!("could not read line from jump_events database: {}", err);
                }
            }
        }
        Err(err) => {
            log::error!("could not prepare SQL statement: {}", err);
        }
    }

    t
}

pub fn insert_jump_events(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    series: &Vec<super::JumpEventData>,
) {
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return;
        }
    };
    let num_values = series.len();
    for i in 0..num_values {
        match connection.execute(
            "INSERT INTO jump_events (timestamp, symbol, percent ) VALUES (?1, ?2, ?3)",
            params![&series[i].datetime, &series[i].symbol, &series[i].percent],
        ) {
            Ok(_retval) => {} //log::warn!("Inserted {} video with ID {} and location {} into candidates.", video.id, video.index, candidate_id),
            Err(error) => {
                log::error!("Failed insert jump_events! {}", error);
                return;
            }
        }
    }
}

pub fn _delete_jump_events(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    series: &Vec<super::JumpEventData>,
) {
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return;
        }
    };
    let _ret = connection.execute(
        "DELETE FROM jump_events WHERE symbol = ?1",
        params![&series[0].symbol],
    );
}

pub fn _update_jump_events(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    series: &Vec<super::JumpEventData>,
) {
    _delete_jump_events(sql_connection.clone(), series);
    insert_jump_events(sql_connection.clone(), series);
}

pub fn drop_events_count(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbol: &str
) -> usize {
    let mut num = 0_usize;
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return 0;
        }
    };
    let query = "SELECT COUNT(timestamp) FROM drop_events WHERE symbol = ?1";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![symbol]) {
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

pub fn drop_events(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbol: &str
) -> Vec<super::JumpEventData> {
    let mut t = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return t;
        }
    };
    let query = "SELECT timestamp, symbol, percent FROM drop_events WHERE symbol = ?1 ORDER BY timestamp ASC";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![symbol]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => {
                                let mut s = super::JumpEventData {
                                    ..Default::default()
                                };
                                match row.get(0) {
                                    Ok(val) => s.datetime = val,
                                    Err(error) => {
                                        log::error!(
                                            "Failed to read datetime for drop_events: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(1) {
                                    Ok(val) => s.symbol = val,
                                    Err(error) => {
                                        log::error!("Failed to read open for drop_events: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(2) {
                                    Ok(val) => s.percent = val,
                                    Err(error) => {
                                        log::error!("Failed to read high for drop_events: {}", error);
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
                                log::error!("Failed to read a row from drop_events: {}", error);
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    log::error!("could not read line from drop_events database: {}", err);
                }
            }
        }
        Err(err) => {
            log::error!("could not prepare SQL statement: {}", err);
        }
    }

    t
}

pub fn insert_drop_events(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    series: &Vec<super::JumpEventData>,
) {
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return;
        }
    };
    let num_values = series.len();
    for i in 0..num_values {
        match connection.execute(
            "INSERT INTO drop_events (timestamp, symbol, percent ) VALUES (?1, ?2, ?3)",
            params![&series[i].datetime, &series[i].symbol, &series[i].percent],
        ) {
            Ok(_retval) => {} //log::warn!("Inserted {} video with ID {} and location {} into candidates.", video.id, video.index, candidate_id),
            Err(error) => {
                log::error!("Failed insert drop_events! {}", error);
                return;
            }
        }
    }
}

pub fn _delete_drop_events(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    series: &Vec<super::JumpEventData>,
) {
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return;
        }
    };
    let _ret = connection.execute(
        "DELETE FROM drop_events WHERE symbol = ?1",
        params![&series[0].symbol],
    );
}

pub fn _update_drop_events(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    series: &Vec<super::JumpEventData>,
) {
    _delete_drop_events(sql_connection.clone(), series);
    insert_drop_events(sql_connection.clone(), series);
}

pub fn recurring_events_count(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbol: &str
) -> usize {
    let mut num = 0_usize;
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return 0;
        }
    };
    let query = "SELECT COUNT(symbol) FROM recurring_events WHERE symbol = ?1";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![symbol]) {
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

pub fn recurring_events(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbol: &str
) -> Vec<super::RecurringEventData> {
    let mut t = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return t;
        }
    };
    let query = "SELECT symbol, minutes_period, percent FROM recurring_events WHERE symbol = ?1 ORDER BY timestamp ASC";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![symbol]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => {
                                let mut s = super::RecurringEventData {
                                    ..Default::default()
                                };
                                match row.get(0) {
                                    Ok(val) => s.symbol = val,
                                    Err(error) => {
                                        log::error!(
                                            "Failed to read datetime for recurring_events: {}",
                                            error
                                        );
                                        continue;
                                    }
                                }
                                match row.get(1) {
                                    Ok(val) => s.minutes_period = val,
                                    Err(error) => {
                                        log::error!("Failed to read open for recurring_events: {}", error);
                                        continue;
                                    }
                                }
                                match row.get(2) {
                                    Ok(val) => s.percent = val,
                                    Err(error) => {
                                        log::error!("Failed to read high for recurring_events: {}", error);
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
                                log::error!("Failed to read a row from recurring_events: {}", error);
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    log::error!("could not read line from recurring_events database: {}", err);
                }
            }
        }
        Err(err) => {
            log::error!("could not prepare SQL statement: {}", err);
        }
    }

    t
}

pub fn insert_recurring_events(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    series: &Vec<super::RecurringEventData>,
) {
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return;
        }
    };
    let num_values = series.len();
    for i in 0..num_values {
        match connection.execute(
            "INSERT INTO recurring_events (symbol, minutes_period percent ) VALUES (?1, ?2, ?3)",
            params![&series[i].symbol, &series[i].minutes_period, &series[i].percent],
        ) {
            Ok(_retval) => {} //log::warn!("Inserted {} video with ID {} and location {} into candidates.", video.id, video.index, candidate_id),
            Err(error) => {
                log::error!("Failed insert recurring_events! {}", error);
                return;
            }
        }
    }
}

pub fn _delete_recurring_events(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    series: &Vec<super::RecurringEventData>,
) {
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return;
        }
    };
    let _ret = connection.execute(
        "DELETE FROM recurring_events WHERE symbol = ?1",
        params![&series[0].symbol],
    );
}

pub fn _update_recurring_events(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    series: &Vec<super::RecurringEventData>,
) {
    _delete_recurring_events(sql_connection.clone(), series);
    insert_recurring_events(sql_connection.clone(), series);
}
