use anyhow::Result;
use lazy_static::lazy_static;
use market_data::{AlphaVantage, EnhancedMarketSeries, MarketClient, OutputSize, Interval};
use std::env::var;

use super::super::sql::{insert_live_data, metadata, TimeSeriesData};

lazy_static! {
    static ref TOKEN: String =
        var("AlphaVantage_TOKEN").expect("AlphaVantage_TOKEN env variable is required");
}

pub fn live_data(
    sql_connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    symbol: &str,
    exchange_code: &str,
)  -> Result<Vec<TimeSeriesData>, Box<dyn std::error::Error>> {
    let mut site = AlphaVantage::new(TOKEN.to_string());
    // retrieve per minute data
    match site.intraday_series(symbol, OutputSize::Compact, Interval::Min1) {
        Ok(()) => {},
        Err(error) => return Err(Box::new(error))
    }
    // create the MarketClient
    let mut client = MarketClient::new(site);

    // check if we have data for this symbol
    let stock_symbol = symbol.to_string();
    let metadata = metadata(sql_connection.clone(), exchange_code, &stock_symbol);
    // creates the query URL & download the raw data
    client = client.create_endpoint()?.get_data()?;
    // transform into MarketSeries, that can be used for further processing
    let resvec = client.transform_data();

    //data.iter().for_each(|output| match output {
    //    Ok(data) =>  {
    //        log::debug!("{}\n\n", data);
    //        let _ret = crate::sql::insert_timeseries(sql_connection.clone(), &metadata, data);
    //    },
    //    Err(err) => log::error!("{}", err),
    //});
    // the data can be enhanced with the calculation of a number of  market indicators
    let enhanced_data: Vec<EnhancedMarketSeries> = resvec
        .into_iter()
        .filter_map(|series| series.ok())
        .map(|series| {
            series
                .enhance_data()
                .with_sma(10)
                .with_ema(20)
                .with_ema(6)
                .with_rsi(14)
                .calculate()
        })
        .collect();

    // store the data
    let mut retvec = Vec::new();
    for data in enhanced_data.iter() {
        let ret = insert_live_data(sql_connection.clone(), &metadata, data);
        retvec.extend(ret);
    }
    Ok(retvec)
}