use anyhow::Result;
use lazy_static::lazy_static;
use market_data::{EnhancedMarketSeries, Interval, MarketClient, Twelvedata};
use std::env::var;

use super::super::sql::TimeSeriesData;

lazy_static! {
    static ref TOKEN: String =
        var("Twelvedata_TOKEN").expect("Twelvedata_TOKEN env variable is required");
}

pub fn live_data(
    symbol: &str,
    start_time: chrono::NaiveDateTime,
    end_time: chrono::NaiveDateTime,
)  -> Result<Vec<TimeSeriesData>, Box<dyn std::error::Error>> {
    let site: Twelvedata = Twelvedata::new(TOKEN.to_string());
    // create the MarketClient
    let mut client: MarketClient<Twelvedata> = MarketClient::new(site);
    // calculate the number of seconds since the last timestamp
    let mut num_data = ((end_time - start_time).as_seconds_f32() / 60.0).round().abs() as u32;
    if num_data == 0 {
        return Err(format!("No new data available for this stock!").into())
    }
    if end_time.date() != start_time.date() {
        // last data from yesterday
        let start_time_today = end_time.date().and_hms_opt(7, 0, 0).unwrap();
        num_data = ((end_time - start_time_today).as_seconds_f32() / 60.0).round().abs() as u32;
    }
    // check if we have data for this symbol
    let stock_symbol = symbol.to_string();
    // retrieve per minute data for the last 10 minutes
    client.site.intraday_series(stock_symbol, num_data, Interval::Min1)?;
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
        retvec.extend(super::marketdata_to_timeseries(data));
    }
    Ok(retvec)
}