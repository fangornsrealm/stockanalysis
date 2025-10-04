//! detect interesting features in time series of stock data
//! 

use augurs::{
    changepoint::{Detector as ChangepointDetector, DefaultArgpcpDetector},
    clustering::DbscanClusterer,
    dtw::Dtw,
    ets::AutoETS,
    forecaster::{
        transforms::{LinearInterpolator, Log, MinMaxScaler},
        Forecaster, Transformer,
    },
    mstl::MSTLModel,
    outlier::{DbscanDetector, MADDetector, OutlierDetector},
    prophet::{PredictionData, Prophet, TrainingData, wasmstan::WasmstanOptimizer},
    seasons::{Detector, PeriodogramDetector},
};

/// calculates the average slope over a series of values
pub fn slope(series: &Vec<f64>, last_x: usize) -> f64 {
    if last_x == 0 || series.len() == 0 || last_x > series.len() {
        return 0.0;
    }
    let mut total_slope = 0.0;
    let end = series.len() - 1;
    let start = end - last_x;
    let points = &series[start..end];
    // Iterate through adjacent pairs of points
    for i in 0..(points.len() - 1) {
        let x1 = points[i];
        let x2 = points[i + 1];
        let slope = (x2 - x1) / last_x as f64;
        total_slope += slope;
    }

    total_slope
}

/// calculates the slopes for the latest five, ten and fifteen values 
/// if the slopes are increasing, and the percentage is larger than the threshold, 
/// returns the percentage change of the fifth last to the current
/// else returns 0.0
pub fn increasing_slope(series: &Vec<f64>, threshold_up: f64, threshold_down: f64) -> f64 {
    if series.len() < 15 {
        return 0.0;
    }
    let slope_5 = slope(series, 5);
    let slope_10 = slope(series, 15);
    let slope_15 = slope(series, 15);
    let end = series.len() - 1;
    let start = end - 5;
    let change = series[end] - series[start];
    let percentage = change / series[start] * 100.0;
    if slope_5 < 0.0 && slope_10 < 0.0 && slope_15 < 0.0 {
        if slope_5 < slope_10 && slope_10 < slope_15 {
            if percentage.abs() > threshold_down.abs() {
                return percentage;
            }
        }
    } else if slope_5 > 0.0 && slope_10 > 0.0 && slope_15 > 0.0 {
        if slope_5 > slope_10 && slope_10 > slope_15 {
            if percentage > threshold_up {
                return percentage;
            }
        }
    }
    0.0
}

/// detect jumps in the series and return the percentage if it is larger than limit up or limit down
/// and the position in the list
pub fn jumps_in_series(
    symbol: &str, 
    timestamps: &Vec<i64>, 
    series: &Vec<f64>, 
    threshold_up: f64, 
    threshold_down: f64
) -> Vec<crate::data::sql::JumpEventData> 
{
    let mut v = Vec::new();
    if timestamps.len() != series.len() {
        return v;
    }
    for i in 1..series.len() {
        let change = series[i] - series[i - 1];
        if change < 0.0 {
            let percentage = change.abs() / series[i - 1] * 100.0;
            if threshold_down.abs() < percentage {
                let s = crate::data::sql::JumpEventData {
                    datetime: timestamps[i],
                    symbol: symbol.to_string(),
                    percent: -percentage,
                    ..Default::default()
                };
                v.push(s);
            }
        } else {
            let percentage = change / series[i - 1] * 100.0;
            if threshold_up < percentage {
                let s = crate::data::sql::JumpEventData {
                    datetime: timestamps[i],
                    symbol: symbol.to_string(),
                    percent: percentage,
                    ..Default::default()
                };
                v.push(s);
            }
        }
    }
    v
}

pub fn recurring_events_in_series(
    symbol: &str, 
    timestamps: &Vec<i64>, 
    series: &Vec<f64>, 
    threshold: f64, 
) -> Vec<crate::data::sql::RecurringEventData> 
{
    let mut v = Vec::new();
    if timestamps.len() < 2 {
        return v;
    }
    let time_diff = (timestamps[1] - timestamps[0]) / 60 / 1000; // in minutes
    let periods = seasonality(series, 3, 300, threshold, false);
    for p in periods {
        let minutes_period= time_diff * p as i64;
        let s = crate::data::sql::RecurringEventData {
            symbol: symbol.to_string(),
            minutes_period,
            time_scale: time_diff as f64,
            ..Default::default()
        };
        v.push(s);
    }

    v
}

/// detect seasonality
pub fn _seasonality_default(series: &Vec<f64>, smooth: bool) -> Vec<usize> {
    let data;
    if smooth {
        data = smooth_series(series, 5);
    } else {
        data = series.clone();
    }
    // Use the detector with default parameters.
    let periods_u32 = PeriodogramDetector::default().detect(&data);
    let mut periods = Vec::new();
    for u in periods_u32 {
        periods.push(u as usize);
    }
    periods
}

/// detect seasonality with optional parameters
pub fn seasonality(series: &Vec<f64>, min_period: u32, max_period: u32, threshold: f64, smooth: bool) -> Vec<usize> {
    let data;
    if smooth {
        data = smooth_series(series, 5);
    } else {
        data = series.clone();
    }
    // Customise the detector using the builder.
    let periods_u32 = PeriodogramDetector::builder()
        .min_period(min_period)
        .max_period(max_period)
        .threshold(threshold)
        .build()
        .detect(&data);
    let mut periods = Vec::new();
    for u in periods_u32 {
        periods.push(u as usize);
    }
    periods
}

// generate a new series where every five entries will be averaged
pub fn smooth_series(series: &Vec<f64>, num_elements_to_average: u32) -> Vec<f64> {
    let mut data;
    let mut average= 0.0;
    let mut num_elements = 0;
    data = Vec::new();
    for i in 0..data.len() {
        average += series[i];
        num_elements += 1;
        if num_elements % num_elements_to_average == 0 {
            data.push(average / num_elements as f64);
            average = 0.0;
            num_elements = 0;
        }
    }
    if num_elements > 0 {
        data.push(average / num_elements as f64);
    }
    data
}

/// detect changepoints
/// if smooth is true, it will generate a new series where every five entries will be averaged
pub fn changepoints(series: &Vec<f64>, smooth: bool) -> Vec<usize> {
    // Use the detector with default parameters.
    //let periods_u32 = PeriodogramDetector::default().detect(y);
    let data;
    if smooth {
        data = smooth_series(series, 5);
    } else {
        data = series.clone();
    }
    DefaultArgpcpDetector::default().detect_changepoints(&data)
}

/// Detect outliers
/// Outliers could be special events
pub fn outliers(series: Vec<&[f64]>) -> Vec<usize> {
    let mut debug = Vec::new();
    // Create and configure detector
    let mut detector = DbscanDetector::with_sensitivity(0.5)
        .expect("sensitivity is between 0.0 and 1.0");

    // Enable parallel processing (requires 'parallel' feature)
    detector = detector.parallelize(true);

    // Detect outliers
    let processed = detector.preprocess(&series).expect("input data is valid");
    let outliers = detector.detect(&processed).expect("detection succeeds");

    debug.push(format!("Outlying series indices: {:?}", outliers.outlying_series));
    debug.push(format!("Series scores: {:?}", outliers.series_results));
    // Get indices of outlying series
    for &idx in &outliers.outlying_series {
        debug.push(format!("Series {} is an outlier", idx));
    }

    // Examine detailed results for each series
    for (idx, result) in outliers.series_results.iter().enumerate() {
        debug.push(format!("Series {}: outlier = {}", idx, result.is_outlier));
        debug.push(format!("Scores: {:?}", result.scores));
    }

    std::fs::write("outliers_debugging.log", &debug.join("\n")).expect("Should be able to write to file");
    outliers.outlying_series.iter().map(|x| x.to_owned()).collect()
}

/// Detect if a new data series is an outlier 
/// needs a series of historical data and a series to compare to it
pub fn is_outlier(historical_data: Vec<&[f64]>, new_data: &[f64]) -> bool {
    // Create detector from historical data
    let detector = MADDetector::with_sensitivity(0.5)
        .expect("sensitivity is between 0.0 and 1.0");

    // Combine historical and new data
    let mut all_series: Vec<&[f64]> = historical_data;
    all_series.push(new_data);

    // Check for outliers
    let processed = detector.preprocess(&all_series)
        .expect("input data is valid");
    let outliers = detector.detect(&processed)
        .expect("detection succeeds");

    // Check if new series (last index) is an outlier
    outliers.outlying_series.contains(&(all_series.len() - 1))
}

/// Sort seasonally organized data series into clusters by similarity\
pub fn cluster_seasonal_data(series: Vec<&[f64]>) -> Vec<i32> {
    let mut v = Vec::new();
    let mut debug = Vec::new();
    // Euclidean DTW
    let euclidean_matrix = Dtw::euclidean()
        .distance_matrix(&series);
    let euclidean_clusters = DbscanClusterer::new(0.5, 2)
        .fit(&euclidean_matrix);

    // Manhattan DTW
    let manhattan_matrix = Dtw::manhattan()
        .distance_matrix(&series);
    let manhattan_clusters = DbscanClusterer::new(0.5, 2)
        .fit(&manhattan_matrix);

    // Compare results
    debug.push(format!("Euclidean clusters: {:?}", euclidean_clusters));
    debug.push(format!("Manhattan clusters: {:?}", manhattan_clusters));
    
    // Compute distance matrix using DTW
    let distance_matrix = Dtw::euclidean()
        .with_window(2)
        .with_lower_bound(4.0)
        .with_upper_bound(10.0)
        .with_max_distance(10.0)
        .distance_matrix(&series);

    // Set DBSCAN parameters
    let epsilon = 0.5;
    let min_cluster_size = 2;

    // Perform clustering
    let clusters = DbscanClusterer::new(epsilon, min_cluster_size)
        .fit(&distance_matrix);

    debug.push(format!("Cluster assignments: {:?}", clusters));
    std::fs::write("cluster_debugging.log", &debug.join("\n")).expect("Should be able to write to file");
    // Clusters are labeled: -1 for noise, 0+ for cluster membership
    for c in clusters {
        v.push(c.as_i32());
    }
    v
}

pub fn forecast(series: &Vec<f64>) {
    let mut debug = Vec::new();
    // Set up model and transformers
    let ets = AutoETS::non_seasonal().into_trend_model();
    let mstl = MSTLModel::new(vec![2], ets);

    let transformers = vec![
        LinearInterpolator::new().boxed(),
        MinMaxScaler::new().boxed(),
        Log::new().boxed(),
    ];

    // Create and fit forecaster
    let mut forecaster = Forecaster::new(mstl).with_transformers(transformers);
    forecaster.fit(series).expect("model should fit");

    // Generate forecasts
    let forecast = forecaster
        .predict(5, 0.95)
        .expect("forecasting should work");

    debug.push(format!("Forecast values: {:?}", forecast.point));
    debug.push(format!("Lower bounds: {:?}", forecast.intervals.as_ref().unwrap().lower));
    debug.push(format!("Upper bounds: {:?}", forecast.intervals.as_ref().unwrap().upper));
    std::fs::write("forecast_debugging.log", &debug.join("\n")).expect("Should be able to write to file");
}

/// Use Metas Prophet forecast routine to fit a model and make a forecast
pub fn forecast_prophet(series: Vec<f64>, timestamps_millis: &Vec<i64>) -> Result<(Vec<f64>, Vec<f64>, Vec<f64>), Box<dyn std::error::Error>> {
    let mut debug = Vec::new();
    let timestamps: Vec<i64> = timestamps_millis.iter().map(|x| *x / 1000).collect();
    // Create training data
    let data = TrainingData::new(timestamps.clone(), series.clone())?;

    // Initialize Prophet with WASMSTAN optimizer
    let optimizer = WasmstanOptimizer::new();
    let mut prophet = Prophet::new(Default::default(), optimizer);

    // Fit the model
    prophet.fit(data, Default::default())?;

    // Make in-sample predictions
    let predictions = prophet.predict(None)?;

    let point = predictions.yhat.point;
    let lower = predictions.yhat.lower.unwrap();
    let upper = predictions.yhat.upper.unwrap();
    debug.push(format!("Predictions inner: {:?}", point));
    debug.push(format!("Lower bounds inner: {:?}", lower));
    debug.push(format!("Upper bounds inner: {:?}", upper));

    // forecast for the next ten minutes
    let mut future_timestamps = Vec::new();
    let starttime = timestamps[timestamps.len() - 1] + 60;
    for i in 0..10 {
        future_timestamps.push(starttime + 60 * i);
    }
    let prediction_data = PredictionData::new(future_timestamps);
    let predictions = prophet.predict(Some(prediction_data))?;

    // Access the forecasted values, and their bounds.
    let point = predictions.yhat.point;
    let lower = predictions.yhat.lower.unwrap();
    let upper = predictions.yhat.upper.unwrap();
    debug.push(format!("Predictions: {:?}", point));
    debug.push(format!("Lower bounds: {:?}", lower));
    debug.push(format!("Upper bounds: {:?}", upper));
    std::fs::write("forecast_prophet_debugging.log", &debug.join("\n")).expect("Should be able to write to file");

    Ok((point, lower, upper))
}

/// split series into chunks
pub fn split_series_into_seasons(series: &Vec<f64>, minutes_per_period: i64, minutes_per_step: i64) -> Vec<Vec<f64>> {
    let mut v = Vec::new();
    let pivot = minutes_per_period / minutes_per_step as i64;
    let mut count = 0;
    let mut w = Vec::new();
    for f in series {
        w.push(f.to_owned());
        count += 1;
        if count % pivot == 0 {
            v.push(w.clone());
            w.clear();
            count = 0;
        }
    }
    v
}

// Utilities

/// convert a Vec<Vec<>> to Vec<&[]>
pub fn vecs_to_slices<T>(vecs: &[Vec<T>]) -> Vec<&[T]> {
    vecs.iter().map(Vec::as_slice).collect()
}

