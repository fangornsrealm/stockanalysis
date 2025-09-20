//! detect interesting features in time series of stock data
//! 

use augurs::{
    changepoint::{Detector as ChangepointDetector, DefaultArgpcpDetector},
    clustering::{DbscanCluster, DbscanClusterer},
    dtw::Dtw,
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

/// detect seasonality
pub fn seasonality(y: &Vec<f64>, min_period: u32, max_period: u32, threshold: f64) -> Vec<usize> {
    // Use the detector with default parameters.
    //let periods_u32 = PeriodogramDetector::default().detect(y);

    // Customise the detector using the builder.
    let periods_u32 = PeriodogramDetector::builder()
        .min_period(min_period)
        .max_period(max_period)
        .threshold(threshold)
        .build()
        .detect(y);
    let mut periods = Vec::new();
    for u in periods_u32 {
        periods.push(u as usize);
    }
    periods
}

// generate a new series where every five entries will be averaged
pub fn smooth_series(series: &Vec<f64>) -> Vec<f64> {
    let mut data;
    let mut average= 0.0;
    let mut num_elements = 0;
    data = Vec::new();
    for i in 0..data.len() {
        average += series[i];
        num_elements += 1;
        if num_elements % 5 == 0 {
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
        data = smooth_series(series);
    } else {
        data = series.clone();
    }
    DefaultArgpcpDetector::default().detect_changepoints(&data)
}


// split series into chunks

