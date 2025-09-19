use std::path::PathBuf;
use rusqlite::{params, Connection};

pub fn init_database(sqlite_file: PathBuf) {
    use std::fs::File;
    use std::io::Read;

    if let Ok(connection) = Connection::open(sqlite_file) {
        println!("{}", connection.is_autocommit());
        // file_type: 0 other file, 1 image, 2, video, 3 audio
        match connection.execute(
            "CREATE TABLE IF NOT EXISTS active_symbols (
                symbols_id INTEGER,
                symbol TEXT,
                PRIMARY KEY(symbols_id AUTOINCREMENT)
            )",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create table active_symbols: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE INDEX index_symbol_active_symbols ON active_symbols (symbol)",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create index on active_symbols: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE TABLE exchanges (
                exchange_id INTEGER,
                title TEXT,
                name TEXT,
                code TEXT,
                country TEXT,
                timezone TEXT,
                PRIMARY KEY(exchange_id AUTOINCREMENT)
            )",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create table time_series: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE TABLE live_data (
                timestamp_id INTEGER,
                timestamp INTEGER,
                symbol TEXT,
                currency TEXT,
                exchange TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                sma DOUBLE,
                ema DOUBLE,
                rsi DOUBLE,
                stochastic DOUBLE,
                macd_value DOUBLE,
                signal_value DOUBLE,
                hist_value DOUBLE,
                PRIMARY KEY(timestamp_id AUTOINCREMENT)
            )",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create table live_data: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE INDEX index_timestamp_live_data ON live_data (timestamp)",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create index on live_data: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE INDEX index_symbol_live_data ON live_data (symbol)",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create index on time_series: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE TABLE stocks (
                            stock_id INTEGER,
                            symbol TEXT,
                            name TEXT,
                            currency TEXT,
                            exchange TEXT,
                            mic_code TEXT,
                            country TEXT,
                            type TEXT,
                            figi_code TEXT,
                            cfi_code TEXT,
                            isin TEXT,
                            cusip TEXT,
                            PRIMARY KEY(stock_id AUTOINCREMENT)
            )",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create table stocks: {}", error);
                return;
            }
        }
        match connection.execute("CREATE INDEX index_symbod_stocks ON stocks (symbol)", ()) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create index on stocks: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE TABLE IF NOT EXISTS time_series (
                timestamp_id INTEGER,
                timestamp INTEGER,
                symbol TEXT,
                currency TEXT,
                exchange TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                PRIMARY KEY(timestamp_id AUTOINCREMENT)
            )",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create table time_series: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE INDEX index_timestamp_time_series ON time_series (timestamp)",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create index on time_series: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE INDEX index_symbol_time_series ON time_series (symbol)",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create index on time_series: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE TABLE yahoosymbols(
                ysymbol_id INTEGER, 
                symbol TEXT, 
                name TEXT, 
                PRIMARY KEY(ysymbol_id AUTOINCREMENT) 
            )",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create table yahoosymbols: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE INDEX index_symbol_yahoosymbols ON yahoosymbols (symbol)",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create index on yahoosymbols: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE TABLE jump_events(jump_id INTEGER, timestamp INTEGER, symbol TEXT, percent DOUBLE, PRIMARY KEY(jump_id AUTOINCREMENT) )",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create table yahoosymbols: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE TABLE drop_events(drop_id INTEGER, timestamp INTEGER, symbol TEXT, percent DOUBLE, PRIMARY KEY(drop_id AUTOINCREMENT) )",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create table yahoosymbols: {}", error);
                return;
            }
        }
        match connection.execute(
            "CREATE TABLE recurring_events(recur_id INTEGER, symbol TEXT, minutes_period INTEGER, percent DOUBLE, PRIMARY KEY(recur_id AUTOINCREMENT) )",
            (),
        ) {
            Ok(_ret) => {}
            Err(error) => {
                log::error!("Failed to create table yahoosymbols: {}", error);
                return;
            }
        }

        // fill active_symbols with some data
        for symbol in [ "AAPL", "ADBE", "ADS", "AMD", "ARM", "ATOS", "BAB", "BAS", "BCS", "BE", "BIDU", "BNP", "BNP", "BYD", "CHEMM", "CSIQ", "CWR", "DBK", "DELL", "DEZ", "DHER", "DSY", "DTE", "ENR", "EOAN", "F3C", "GOOGL", "GTLB", "HPE", "IBM", "IFX", "INTC", "KTN", "META", "MPW", "MRNA", "MSFT", "MU", "NET", "NOW", "NVDA", "OKTA", "OVH", "PAH3", "RHM", "RWE", "SAP", "SIE", "SMCI", "SSTK", "TKA", "VOW3", "VRNS", "WAF", "WBD", "YSN" ] {
            match connection.execute(
                "INSERT INTO active_symbols (symbol) VALUES (?1)",
                params![&symbol],
            ) {
                Ok(_ret) => {}
                Err(error) => {
                    log::error!("Failed to create table active_symbols: {}", error);
                    return;
                }
            }
        }
        // fill the stocks and exchanges databases if the files exist
        let stocks_path = PathBuf::from("resources/stocks.json");
        let exchanges_path = PathBuf::from("resources/exchanges.json");
        let yahoosymbols_path = PathBuf::from("resources/yahoo_symbols.json");
        if stocks_path.is_file() {
            let mut file = File::open(stocks_path.clone()).unwrap();
            let mut data = String::new();
            file.read_to_string(&mut data).unwrap();
            match serde_json::from_str::<super::Equities>(&data) {
                Ok(stocks) => {
                    for i in 0..stocks.data.len() {
                        //stock_map.insert(stocks.data[i].symbol.clone(), stocks.data[i].clone());
                        match connection.execute(
                            "INSERT INTO stocks (symbol, name, currency, exchange, mic_code, country, type, figi_code, cfi_code, isin, cusip) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                            params![ &stocks.data[i].symbol, &stocks.data[i].name, &stocks.data[i].currency, &stocks.data[i].exchange, &stocks.data[i].mic_code, &stocks.data[i].country, &stocks.data[i].r#type, &stocks.data[i].figi_code, &stocks.data[i].cfi_code, &stocks.data[i].isin, &stocks.data[i].cusip ],
                        ) {
                            Ok(_retval) => {}
                            Err(error) => {
                                log::error!("Failed to insert stock into database: {}", error);
                                return;
                            }
                        }
                    }
                }
                Err(error) => {
                    log::error!("Failed to parse JSON from {:?}: {}", stocks_path, error);
                }
            }
        }
        if exchanges_path.is_file() {
            let mut file = File::open(exchanges_path.clone()).unwrap();
            let mut data = String::new();
            file.read_to_string(&mut data).unwrap();
            match serde_json::from_str::<super::Exchanges>(&data) {
                Ok(exchanges) => {
                    for i in 0..exchanges.data.len() {
                        //exchange_map.insert(exchanges.data[i].code.clone(), exchanges.data[i].clone());
                        match connection.execute(
                            "INSERT INTO exchanges (title, name, code, country, timezone) VALUES (?1, ?2, ?3, ?4, ?5)",
                            params![ &exchanges.data[i].title, &exchanges.data[i].name, &exchanges.data[i].code, &exchanges.data[i].country, &exchanges.data[i].timezone ],
                        ) {
                            Ok(_retval) => {}
                            Err(error) => {
                                log::error!("Failed to insert exchange into database: {}", error);
                                return;
                            }
                        }
                    }
                }
                Err(error) => {
                    log::error!("Failed to parse JSON from {:?}: {}", exchanges_path, error);
                }
            }
        }
        if yahoosymbols_path.is_file() {
            let mut file = File::open(exchanges_path.clone()).unwrap();
            let mut data = String::new();
            file.read_to_string(&mut data).unwrap();
            match serde_json::from_str::<super::YahooSymbols>(&data) {
                Ok(ysymbols) => {
                    for i in 0..ysymbols.data.len() {
                        match connection.execute(
                            "INSERT INTO yahoosymbols (symbol, name) VALUES (?1, ?2)",
                            params![ &ysymbols.data[i].ysymbol, &ysymbols.data[i].name],
                        ) {
                            Ok(_retval) => {}
                            Err(error) => {
                                log::error!("Failed to insert exchange into database: {}", error);
                                return;
                            }
                        }
                    }
                }
                Err(error) => {
                    log::error!("Failed to parse JSON from {:?}: {}", exchanges_path, error);
                }
            }
        }
    }
}

