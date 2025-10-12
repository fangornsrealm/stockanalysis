use std::error::Error;

use api::prelude::*;

fn build_screener(screener: ScreenerBuilder) -> Result<Screener, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        screener.build()
    )
}

fn metrics(screener: Screener) -> Result<DataTable, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        screener.metrics()
    )
}

fn get_ticker(tickers: Tickers, symbol: &str) -> Result<Ticker, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        tickers.get_ticker(symbol)
    )
}

fn optimize(tickers: Tickers, objective: Option<api::prelude::ObjectiveFunction>) -> Result<api::prelude::Portfolio, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        tickers.optimize(objective, None)
    )
}

fn report(ticker: Ticker, reporttype: Option<ReportType>) -> Result<api::reports::tabs::TabbedHtml, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.report(reporttype)
    )
}

fn report_portfolio(ticker: api::prelude::Portfolio, reporttype: Option<ReportType>) -> Result<api::reports::tabs::TabbedHtml, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        ticker.report(reporttype)
    )
}

pub fn run_screener_process(filepath: &std::path::PathBuf) -> Result<(), Box<dyn Error>> {
    let archivepath = super::archive_path(filepath);
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
        .size(10);
    let screener = build_screener(screener)?;
    let file_name = "screener_overview.html";
    let path = filepath.clone().join(file_name);
    let overview = screener.clone().overview().to_html();
    match overview {
        Ok(chart) => {
            super::move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&super::osstr_to_string(path.into_os_string()), &chart).expect("Should be able to write to file")
        },
        Err(e) => {
            tracing::error!("Failed to get overview for screener: {e}");
            return Ok(());
        }
    }

    let file_name = "screener_metrics.html";
    let path = filepath.clone().join(file_name);
    let metrics = metrics(screener.clone())?.to_html();
    match metrics {
        Ok(chart) => {
            super::move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&super::osstr_to_string(path.into_os_string()), &chart).expect("Should be able to write to file")
        },
        Err(e) => {
            tracing::error!("Failed to get metrics for screener: {e}");
            return Ok(());
        }
    }

    // Instantiate a Multiple Ticker Object
    let ticker_symbols = screener.symbols.iter()
        .map(|x| x.as_str()).collect::<Vec<&str>>();

    let tickers = api::models::tickers::TickersBuilder::new()
        .tickers(ticker_symbols.clone())
        .start_date("2025-03-01")
        .end_date("2025-09-15")
        .interval(Interval::OneDay)
        .benchmark_symbol("0H1C")
        .confidence_level(0.95)
        .risk_free_rate(0.02)
        .build();

    // Generate a Single Ticker Report
    let symbol = ticker_symbols.first().unwrap();
    let ticker = get_ticker(tickers.clone(), symbol)?;
    let performance = report(ticker.clone(), Some(ReportType::Performance))?.to_html();
    let file_name = "screener_top_performance.html";
    let path = filepath.clone().join(file_name);
    super::move_file_to_archive(filepath, &archivepath, &path);
    std::fs::write(&super::osstr_to_string(path.into_os_string()), &performance).expect("Should be able to write to file");
    let financials = report(ticker.clone(), Some(ReportType::Financials))?.to_html();
    let file_name = "screener_financials.html";
    let path = filepath.clone().join(file_name);
    super::move_file_to_archive(filepath, &archivepath, &path);
    std::fs::write(&super::osstr_to_string(path.into_os_string()), &financials).expect("Should be able to write to file");
    let options = report(ticker.clone(), Some(ReportType::Options))?.to_html();
    let file_name = "screener_options.html";
    let path = filepath.clone().join(file_name);
    super::move_file_to_archive(filepath, &archivepath, &path);
    std::fs::write(&super::osstr_to_string(path.into_os_string()), &options).expect("Should be able to write to file");
    let news = report(ticker.clone(), Some(ReportType::News))?.to_html();
    let file_name = "screescreener_newsner_overview.html";
    let path = filepath.clone().join(file_name);
    super::move_file_to_archive(filepath, &archivepath, &path);
    std::fs::write(&super::osstr_to_string(path.into_os_string()), &news).expect("Should be able to write to file");

    // Generate a Multiple Ticker Report
    let report = report(ticker.clone(), Some(ReportType::Performance))?.to_html();
    let file_name = "screener_report.html";
    let path = filepath.clone().join(file_name);
    super::move_file_to_archive(filepath, &archivepath, &path);
    std::fs::write(&super::osstr_to_string(path.into_os_string()), &report).expect("Should be able to write to file");

    // Perform a Portfolio Optimization
    let portfolio = optimize(tickers.clone(), Some(ObjectiveFunction::MaxSharpe))?;

    // Generate a Portfolio Report
    let portfolioreport = report_portfolio(portfolio.clone(), Some(ReportType::Performance))?.to_html();
    let file_name = "screener_portfolioreport.html";
    let path = filepath.clone().join(file_name);
    super::move_file_to_archive(filepath, &archivepath, &path);
    std::fs::write(&super::osstr_to_string(path.into_os_string()), &portfolioreport).expect("Should be able to write to file");

    // TODO write a HTML file with links to the written HTML files

    // TODO send it via notification and Apple Push notification

    Ok(())
}

