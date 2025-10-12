use std::error::Error;

use api::prelude::*;

fn build_portfolio(portfolio: api::models::portfolio::PortfolioBuilder) -> Result<Portfolio, Box<dyn Error>> {
    let handle = tokio::runtime::Handle::current();
    let _ = handle.enter();
    futures::executor::block_on(
        portfolio.build()
    )
}

pub fn run_portfolio_analysis(
    symbolsstrings: &Vec<String>,
    filepath: &std::path::PathBuf
) -> Result<(), Box<dyn Error>> {
    // 
    let symbols: Vec<&str> = symbolsstrings.iter().map(|s| &**s).collect();
    let three_months_ago = chrono::Local::now().date_naive().checked_sub_days(chrono::Days::new(90)).unwrap();
    let yesterday = super::yesterday();
    let archivepath = super::archive_path(filepath);

    let start_date = three_months_ago.and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap()).and_utc();
    let end_date = yesterday.and_time(chrono::NaiveTime::from_num_seconds_from_midnight_opt(23 * 3600 + 59 * 60, 0).unwrap()).and_utc();
    let portfolio = Portfolio::builder()
            .ticker_symbols(symbols.clone())
            .benchmark_symbol("0H1C")
            .start_date(&start_date.naive_utc().to_string())
            .end_date(&end_date.naive_utc().to_string())
            .interval(Interval::OneDay)
            .confidence_level(0.95)
            .risk_free_rate(0.02)
            .objective_function(ObjectiveFunction::MaxSharpe);
    let portfolio = build_portfolio(portfolio)?;
    let testportfolio = portfolio.clone();
    let opt_chart = testportfolio.optimization_chart(None, None)
            .map_err(|e| format!("Optimization Chart error: {e}"));
    match opt_chart {
        Ok(chart) => {
            let file_name = "opt_chart.jpg".to_string();
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            chart.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);

            let file_name = "opt_chart.html";
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&super::osstr_to_string(path.into_os_string()), &chart.to_html()).expect("Should be able to write to file")
        },
        Err(e) => {
            tracing::error!("Failed to get chart for portfolio: {e}");
            return Ok(());
        }
    }
    let testportfolio = portfolio.clone();
    let perf_chart = testportfolio.performance_chart(None, None)
            .map_err(|e| format!("Performance Chart error: {e}"));
    match perf_chart {
        Ok(chart) => {
            let file_name = "perf_chart.jpg".to_string();
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            chart.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);

            let file_name = "perf_chart.html";
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&super::osstr_to_string(path.into_os_string()), &chart.to_html()).expect("Should be able to write to file")
        },
        Err(e) => {
            tracing::error!("Failed to get chart for portfolio: {e}");
            return Ok(());
        }
    }
    let testportfolio = portfolio.clone();
    let perf_stats_chart = testportfolio.performance_stats_table()
            .map_err(|e| format!("Performance Stats Table error: {e}")).unwrap().to_html();
    match perf_stats_chart {
        Ok(chart) => {
            let file_name = "performance_stats_table.html";
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&super::osstr_to_string(path.into_os_string()), &chart).expect("Should be able to write to file")
        },
        Err(e) => {
            tracing::error!("Failed to get chart for portfolio: {e}");
            return Ok(());
        }
    }
    let testportfolio = portfolio.clone();
    let returns_table = testportfolio.returns_table().
            map_err(|e| format!("Returns Table error: {e}")).unwrap().to_html();
    let file_name = "returns_table.html";
    let path = filepath.clone().join(file_name);
    match returns_table {
        Ok(chart) => {
            super::move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&super::osstr_to_string(path.into_os_string()), &chart).expect("Should be able to write to file");
        },
        Err(e) => {
            tracing::error!("Failed to get chart for portfolio: {e}");
            return Ok(());
        }
    }
    let testportfolio = portfolio.clone();
    let returns_chart = testportfolio.returns_chart(None, None)
            .map_err(|e| format!("Returns Chart error: {e}"));
    match returns_chart {
        Ok(chart) => {
            let file_name = "returns_chart.jpg".to_string();
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            chart.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);

            let file_name = "returns_chart.html";
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&super::osstr_to_string(path.into_os_string()), &chart.to_html()).expect("Should be able to write to file")
        },
        Err(e) => {
            tracing::error!("Failed to get chart for portfolio: {e}");
            return Ok(());
        }
    }
    let testportfolio = portfolio.clone();
    let returns_matrix = testportfolio.returns_matrix(None, None)
            .map_err(|e| format!("Returns Matrix error: {e}"));
    match returns_matrix {
        Ok(chart) => {
            let file_name = "returns_matrix.jpg".to_string();
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            chart.to_jpeg(&super::osstr_to_string(path.into_os_string()), 1200, 800, 1.0);

            let file_name = "returns_matrix.html";
            let path = filepath.clone().join(file_name);
            super::move_file_to_archive(filepath, &archivepath, &path);
            std::fs::write(&super::osstr_to_string(path.into_os_string()), &chart.to_html()).expect("Should be able to write to file")
        },
        Err(e) => {
            tracing::error!("Failed to get chart for portfolio: {e}");
            return Ok(());
        }
    }
    Ok(())
}
