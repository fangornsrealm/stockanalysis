use rusqlite::params;

pub fn check_equity_exists(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbol: &str,
) -> bool {
    let mut num = 0_usize;
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return false;
        }
    };
    let query = "SELECT COUNT(symbol) FROM stocks WHERE symbol = ?1";
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
    num > 0
}

pub fn read_equity(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    stock_symbol: &str,
) -> Vec<super::StockEquity> {
    let mut v = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return v;
        }
    };

    let query = "SELECT symbol, name, currency, exchange, mic_code, country, type, figi_code, cfi_code, isin, cusip 
                                FROM stocks 
                                    WHERE (symbol = ?1)";
    match connection.prepare(query) {
        Ok(mut statement) => match statement.query(params![&stock_symbol]) {
            Ok(mut rows) => loop {
                match rows.next() {
                    Ok(Some(row)) => {
                        let mut s = super::StockEquity {
                            ..Default::default()
                        };
                        match row.get(0) {
                            Ok(val) => {
                                let st: String = val;
                                s.symbol = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read symbol for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(1) {
                            Ok(val) => {
                                let st: String = val;
                                s.name = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read name for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(2) {
                            Ok(val) => {
                                let st: String = val;
                                s.currency = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read currency for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(3) {
                            Ok(val) => {
                                let st: String = val;
                                s.exchange = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read exchange for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(4) {
                            Ok(val) => {
                                let st: String = val;
                                s.mic_code = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read mic_code for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(5) {
                            Ok(val) => {
                                let st: String = val;
                                s.r#type = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read type for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(6) {
                            Ok(val) => {
                                let st: String = val;
                                s.figi_code = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read figi_code for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(7) {
                            Ok(val) => {
                                let st: String = val;
                                s.cfi_code = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read cfi_code for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(8) {
                            Ok(val) => {
                                let st: String = val;
                                s.isin = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read isin for equities: {}", error);
                                continue;
                            }
                        }
                        match row.get(9) {
                            Ok(val) => {
                                let st: String = val;
                                s.cusip = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read cusip for equities: {}", error);
                                continue;
                            }
                        }
                        v.push(s);
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(error) => {
                        log::error!("Failed to read a row from indices: {}", error);
                        break;
                    }
                }
            },
            Err(err) => {
                log::error!(
                    "could not read line from videostore_indices database: {}",
                    err
                );
            }
        },
        Err(err) => {
            log::error!("could not prepare SQL statement: {}", err);
        }
    }

    v
}

pub fn match_yahoo_symbol_with_equity(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    stock_symbol: &str,
) -> String {
    let mut new_symbol = stock_symbol.to_string();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return new_symbol;
        }
    };
    // check if we have an active symbol that is just a longer name
    let query = "SELECT symbol FROM active_symbols WHERE symbol LIKE %?1%";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![stock_symbol]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => match row.get(0) {
                                Ok(val) => {
                                    new_symbol = val;
                                    return new_symbol;
                                },
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
    // see if we can find the description of a yahoo symbol
    let mut description = String::new();
    let query = "SELECT name FROM yahoosymbols WHERE symbol = ?1";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![stock_symbol]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => match row.get(0) {
                                Ok(val) => {
                                    description = val;
                                },
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
    if description.len() > 0 {
        // try to find a name in the twelvedata stock equities that matches this name
        let query = "SELECT symbol FROM stocks WHERE name = ?1";
        match connection.prepare(query) {
            Ok(mut statement) => {
                match statement.query(params![&description]) {
                    Ok(mut rows) => {
                        loop {
                            match rows.next() {
                                Ok(Some(row)) => match row.get(0) {
                                    Ok(val) => new_symbol = val,
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
    }
    new_symbol
}

/// return Stock Equity
pub fn equity(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    stock_symbol: &str,
) -> Vec<super::StockEquity> {
    if check_equity_exists(sql_connection.clone(), stock_symbol) {
        return read_equity(sql_connection.clone(), stock_symbol);
    } else {
        // Symbol not in TwelveData symbols list
        let newsymbol = match_yahoo_symbol_with_equity(sql_connection.clone(), stock_symbol);
        if &newsymbol != stock_symbol {
            return read_equity(sql_connection.clone(), stock_symbol);
        } else {
            return Vec::new();
        }
    }
}

/// Return Stock Exchange
pub fn exchange(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    exchange_code: &str,
) -> super::Exchange {
    let mut s = super::Exchange {
        ..Default::default()
    };
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return s;
        }
    };
    let query = "SELECT title, name, code, country, timezone
                                FROM exchanges s 
                                    WHERE (code = ?1 )";
    match connection.prepare(query) {
        Ok(mut statement) => match statement.query(params![&exchange_code]) {
            Ok(mut rows) => loop {
                match rows.next() {
                    Ok(Some(row)) => {
                        match row.get(0) {
                            Ok(val) => {
                                let st: String = val;
                                s.title = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read title for exchanges: {}", error);
                                continue;
                            }
                        }
                        match row.get(1) {
                            Ok(val) => {
                                let st: String = val;
                                s.name = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read name for exchanges: {}", error);
                                continue;
                            }
                        }
                        match row.get(2) {
                            Ok(val) => {
                                let st: String = val;
                                s.code = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read code for exchanges: {}", error);
                                continue;
                            }
                        }
                        match row.get(3) {
                            Ok(val) => {
                                let st: String = val;
                                s.country = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read country for exchanges: {}", error);
                                continue;
                            }
                        }
                        match row.get(4) {
                            Ok(val) => {
                                let st: String = val;
                                s.timezone = st.clone();
                            }
                            Err(error) => {
                                log::error!("Failed to read timezone for exchanges: {}", error);
                                continue;
                            }
                        }
                        return s;
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(error) => {
                        log::error!("Failed to read a row from indices: {}", error);
                        break;
                    }
                }
            },
            Err(err) => {
                log::error!(
                    "could not read line from videostore_indices database: {}",
                    err
                );
            }
        },
        Err(err) => {
            log::error!("could not prepare SQL statement: {}", err);
        }
    }

    s
}

pub fn check_symbol_exists(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbol: &str,
) -> bool {
    let mut num = 0_usize;
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return false;
        }
    };
    let query = "SELECT COUNT(symbol_id) FROM active_symbols WHERE symbol = ?1";
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
    num > 0
}

/// return the number of time series data for the stock
pub fn active_symbols_count(
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
    let query = "SELECT COUNT(symbol_id) FROM active_symbols WHERE symbol = ?1";
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

pub fn active_symbols(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
) -> Vec<String> {
    let mut t = Vec::new();
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return t;
        }
    };
    let query = "SELECT symbol FROM active_symbols ORDER BY symbol ASC";
    match connection.prepare(query) {
        Ok(mut statement) => {
            match statement.query(params![]) {
                Ok(mut rows) => {
                    loop {
                        match rows.next() {
                            Ok(Some(row)) => match row.get(0) {
                                Ok(val) => {
                                    let s = val;
                                    t.push(s);
                                }
                                Err(error) => {
                                    log::error!("Failed to read open for live_data: {}", error);
                                    continue;
                                }
                            },
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

pub fn insert_active_symbols(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbols: &Vec<String>,
) -> u32 {
    let existing = active_symbols(sql_connection.clone());
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return 1;
        }
    };
    for i in 0..symbols.len() {
        if existing.contains(&symbols[i]) {
            continue;
        }
        match connection.execute(
            "INSERT INTO active_symbols (symbol) VALUES (?1)",
            params![&symbols[i]],
        ) {
            Ok(_retval) => {} //log::warn!("Inserted {} video with ID {} and location {} into candidates.", video.id, video.index, candidate_id),
            Err(error) => {
                log::error!("Failed insert active_symbols! {}", error);
                return 1;
            }
        }
    }
    0
}

pub fn _delete_active_symbols(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbols: &Vec<String>,
) {
    let connection = match sql_connection.lock() {
        Ok(conn) => conn,
        Err(error) => {
            log::error!("Failed to lock sql connection for use! {}", error);
            return;
        }
    };
    for i in 0..symbols.len() {
        let _ret = connection.execute(
            "DELETE FROM active_symbols WHERE symbol = ?1",
            params![&symbols[i]],
        );
    }
}

pub fn _update_active_symbols(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbols: &Vec<String>,
) {
    _delete_active_symbols(sql_connection.clone(), symbols);
    insert_active_symbols(sql_connection.clone(), symbols);
}
