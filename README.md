# StockAnalysis

This project is based on [Finalytics](https://github.com/Nnamdi-sys/finalytics.git).

It is a personal project that will at some point get to do something useful.

Goal is to retrieve historic and live market data through the [market-data](https://github.com/danrusei/market-data) package and an active subscription to one of the data providers, store them in a database, analyze them with the rust library and visualize them into HTML files. At some point I want to get a notification when a monitored value does something interesting and either open the HTML report directly from the notification or open a link to a self-hosted webserver in my home network and load the HTML report from there.

Currently the rust library can work with data from the sqlite database. See the Portfolio part of the testexample code and the generated HTML files.

The database is updated by a small program running every night. Sooner or later this will have to move into the library and run in the background, updating the data every one to five minutes.

The web app is currently on hold. After three long days of trying to make it do something I can verify I decided that it is not worth the effort for my use-case.

## Installation

Add the following to your `Cargo.toml` file:

```toml
[dependencies]
stockanalysis = { git = "https://github.com/fangornsrealm/stockanalysis" }
```

## Example

```rust
use std::error::Error;
use stockanalysis::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    // Screen for Large-Cap NASDAQ Stocks
    let screener = Screener::builder()
        .quote_type(QuoteType::Equity)
        .add_filter(ScreenerFilter::EqStr(
            ScreenerMetric::Equity(EquityScreener::Exchange),
            Exchange::NASDAQ.as_ref()
        ))
        .sort_by(
            ScreenerMetric::Equity(EquityScreener::MarketCapIntraday),
            true
        )
        .size(10)
        .build()
        .await?;

    screener.overview().show()?;
    screener.metrics().await?.show()?;

    // Instantiate a Multiple Ticker Object
    let ticker_symbols = screener.symbols.iter()
        .map(|x| x.as_str()).collect::<Vec<&str>>();

    let tickers = TickersBuilder::new()
        .tickers(ticker_symbols.clone())
        .start_date("2023-01-01")
        .end_date("2024-12-31")
        .interval(Interval::OneDay)
        .benchmark_symbol("^GSPC")
        .confidence_level(0.95)
        .risk_free_rate(0.02)
        .build();

    // Generate a Single Ticker Report
    let symbol = ticker_symbols.first().unwrap();
    let ticker = tickers.clone().get_ticker(symbol).await?;
    ticker.report(Some(ReportType::Performance)).await?.show()?;
    ticker.report(Some(ReportType::Financials)).await?.show()?;
    ticker.report(Some(ReportType::Options)).await?.show()?;
    ticker.report(Some(ReportType::News)).await?.show()?;

    // Generate a Multiple Ticker Report
    tickers.report(Some(ReportType::Performance)).await?.show()?;

    // Perform a Portfolio Optimization
    let portfolio = tickers.optimize(Some(ObjectiveFunction::MaxSharpe), None).await?;

    // Generate a Portfolio Report
    portfolio.report(Some(ReportType::Performance)).await?.show()?;

    Ok(())
}
```
