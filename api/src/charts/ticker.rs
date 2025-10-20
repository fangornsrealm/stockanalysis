use std::error::Error;
use polars::prelude::*;
use chrono::DateTime;
use plotly::common::{AxisSide, Fill, Line, LineShape, Mode, Title};
use plotly::{Bar, Candlestick, Histogram, Layout, Plot, Scatter, Surface};
use plotly::layout::{Axis, GridPattern, LayoutGrid, LayoutScene, RangeSelector, RangeSlider, RowOrder, SelectorButton, SelectorStep, StepMode};

use crate::models::ticker::Ticker;
use crate::data::ticker::TickerData;
use crate::prelude::{DataTable, DataTableDisplay, DataTableFormat, StatementFrequency, StatementType};
use crate::prelude::TechnicalIndicators;
use crate::analytics::performance::TickerPerformance;
use crate::analytics::stochastics::VolatilitySurface;
use crate::analytics::statistics::{cumulative_returns_list, maximum_drawdown};
use crate::charts::set_layout;
use crate::data::dataframes::i64_column_to_datetime_vec;

pub struct FinancialsTables {
    pub income_statement: DataTable,
    pub balance_sheet: DataTable,
    pub cashflow_statement: DataTable,
    pub financial_ratios: DataTable,
}

pub struct OptionsCharts {
    pub volatility_surface: Plot,
    pub volatility_smile: Plot,
    pub volatility_term_structure: Plot,
}

pub struct OptionsTables {
    pub options_chain: DataTable,
    pub volatility_surface: DataTable,
}

pub trait TickerCharts {
    fn ohlcv_table(&self) -> impl std::future::Future<Output = Result<DataTable, Box<dyn Error>>>;
    fn ohlcv_table_live(&self) -> impl std::future::Future<Output = Result<DataTable, Box<dyn Error>>>;
    fn candlestick_chart(&self, height: Option<usize>, width: Option<usize>) -> impl std::future::Future<Output = Result<Plot, Box<dyn Error>>>;
    fn candlestick_chart_live(&self, height: Option<usize>, width: Option<usize>) -> impl std::future::Future<Output = Result<Plot, Box<dyn Error>>>;
    fn candlestick_chart_live_df(&self, ohlcv: DataFrame, metadata: &crate::data::sql::MetaData, height: Option<usize>, width: Option<usize>) -> impl std::future::Future<Output = Result<Plot, Box<dyn Error>>>;
    fn performance_chart(&self, height: Option<usize>, width: Option<usize>) -> impl std::future::Future<Output = Result<Plot, Box<dyn Error>>>;
    fn summary_stats_table(&self) -> impl std::future::Future<Output = Result<DataTable, Box<dyn Error>>>;
    fn performance_stats_table(&self) -> impl std::future::Future<Output = Result<DataTable, Box<dyn Error>>>;
    fn financials_tables(&self, frequency: StatementFrequency) -> impl std::future::Future<Output = Result<FinancialsTables, Box<dyn Error>>>;
    fn options_charts(&self, height: Option<usize>, width: Option<usize>) -> impl std::future::Future<Output = Result<OptionsCharts, Box<dyn Error>>>;
    fn options_tables(&self) -> impl std::future::Future<Output = Result<OptionsTables, Box<dyn Error>>>;
    fn news_sentiment_chart(&self, height: Option<usize>, width: Option<usize>) -> impl std::future::Future<Output = Result<Plot, Box<dyn Error>>>;
    fn news_sentiment_table(&self) -> impl std::future::Future<Output = Result<DataTable, Box<dyn Error>>>;
    fn macd_chart_recent(&self, ohlcv: DataFrame, metadata: &crate::data::sql::MetaData,height: Option<usize>, width: Option<usize>) -> impl std::future::Future<Output = Result<Plot, Box<dyn Error>>>;
    fn ppo_chart_recent(&self, ohlcv: DataFrame, metadata: &crate::data::sql::MetaData,height: Option<usize>, width: Option<usize>) -> impl std::future::Future<Output = Result<Plot, Box<dyn Error>>>;
    fn mfi_chart_recent(&self, ohlcv: DataFrame, metadata: &crate::data::sql::MetaData,height: Option<usize>, width: Option<usize>) -> impl std::future::Future<Output = Result<Plot, Box<dyn Error>>>;
    fn stochastic_chart_recent(&self, ohlcv: DataFrame, metadata: &crate::data::sql::MetaData, height: Option<usize>, width: Option<usize>) -> impl std::future::Future<Output = Result<Plot, Box<dyn Error>>>;
}

impl TickerCharts for Ticker {
    /// Displays the OHLCV Table for the ticker
    ///
    /// # Returns
    ///
    /// * `DataTable` - Interactive Table Chart struct
    async fn ohlcv_table(&self) -> Result<DataTable, Box<dyn Error>> {
        let ohlcv = self.get_chart_daily().await?;
        let datetimes = match i64_column_to_datetime_vec(&ohlcv) {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn timestamps into dates! {:?}", error);
                return Err(error);
            }
        };
        let datetimes = datetimes.iter().map(|x| x.date().to_string()).collect::<Vec<String>>();
        let open = ohlcv.column("open")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let high = ohlcv.column("high")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let low = ohlcv.column("low")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let close = ohlcv.column("close")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let volume = ohlcv.column("volume")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let adjclose = ohlcv.column("adjclose")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let df = df!(
            "timestamp" => datetimes,
            "open" => open,
            "high" => high,
            "low" => low,
            "close" => close,
            "volume" => volume,
            "adjclose" => adjclose,
        )?;

        let table = df.to_datatable("df", true, DataTableFormat::Number);
        Ok(table)
    }

    async fn ohlcv_table_live(&self) -> Result<DataTable, Box<dyn Error>> {
        let ohlcv = self.get_chart().await?;
        let datetimes = match i64_column_to_datetime_vec(&ohlcv) {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn timestamps into dates! {:?}", error);
                return Err(error);
            }
        };
        let datetimes = datetimes.iter().map(|x| x.to_string()).collect::<Vec<String>>();
        let open = ohlcv.column("open")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let high = ohlcv.column("high")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let low = ohlcv.column("low")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let close = ohlcv.column("close")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let volume = ohlcv.column("volume")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let adjclose = ohlcv.column("adjclose")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let df = df!(
            "timestamp" => datetimes,
            "open" => open,
            "high" => high,
            "low" => low,
            "close" => close,
            "volume" => volume,
            "adjclose" => adjclose,
        )?;

        let table = df.to_datatable("df", true, DataTableFormat::Number);
        Ok(table)
    }

    /// Generates an OHLCV candlestick chart for the ticker with technical indicators
    ///
    /// # Arguments
    ///
    /// * `height` - `usize` - Height of the chart
    /// * `width` - `usize` - Width of the chart
    ///
    /// # Returns
    ///
    /// * `Plot` Plotly Chart struct
    async fn candlestick_chart(&self, height: Option<usize>, width: Option<usize>) -> Result<Plot, Box<dyn Error>> {
        let ohlcv = self.get_chart_daily().await?;
        let datetimes = match i64_column_to_datetime_vec(&ohlcv) {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn timestamps into dates! {:?}", error);
                return Err(error);
            }
        };
        let x = datetimes.iter().map(|x| x.date().to_string()).collect::<Vec<String>>();
        let open = ohlcv.column("open")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let high = ohlcv.column("high")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let low = ohlcv.column("low")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let close = ohlcv.column("close")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let volume = ohlcv.column("volume")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let rsi_df = self.rsi_df(ohlcv.clone(), 14, None).await?;
        let rsi_values = rsi_df.column("rsi-14")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let ma_50_df = self.sma_df(ohlcv.clone(), 50, None).await?;
        let ma_50_values = ma_50_df.column("sma-50")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let ma_200_df = self.sma_df(ohlcv.clone(), 200, None).await?;
        let ma_200_values = ma_200_df.column("sma-200")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let candlestick_trace = Candlestick::new(x.clone(), open, high, low, close)
            .name("Prices");
        let volume_trace = Bar::new(x.clone(), volume)
            .name("Volume")
            //.marker(Marker::new().color(NamedColor::Blue))
            .x_axis("x")
            .y_axis("y2");
        let rsi_trace = Scatter::new(x.clone(), rsi_values)
            .name("RSI 14")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let ma50_trace = Scatter::new(x.clone(), ma_50_values)
            .name("MA 50")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline));
        let ma200_trace = Scatter::new(x.clone(), ma_200_values)
            .name("MA 200")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline));

        let sql_connection = crate::data::sql::connect();
        let metadata = crate::data::sql::live_data::get_stock_metadata(sql_connection.clone(), &self.ticker);
        let title = format!("{} ({}) in {} via {}", metadata.name, metadata.symbol, metadata.currency, metadata.exchange);
        let layout = Layout::new()
            .title(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{} Candlestick Chart</span>", title))
            .grid(
                LayoutGrid::new()
                    .rows(3)
                    .columns(1)
                    .pattern(GridPattern::Coupled)
                    .row_order(RowOrder::TopToBottom)
            )
            .x_axis(
                Axis::new()
                    .range_slider(RangeSlider::new().visible(true))
                    .range_selector(RangeSelector::new().buttons(vec![
                        SelectorButton::new()
                            .count(1)
                            .label("1H")
                            .step(SelectorStep::Hour)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1D")
                            .step(SelectorStep::Day)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(6)
                            .label("6M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("YTD")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::ToDate),
                        SelectorButton::new()
                            .count(1)
                            .label("1Y")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .label("MAX")
                            .step(SelectorStep::All),
                    ])),
            )
            .y_axis(
                Axis::new()
                    .domain(&[0.4, 1.0])
            )
            .y_axis2(
                Axis::new()
                    .domain(&[0.2, 0.4])
            )
            .y_axis3(
                Axis::new()
                    .domain(&[0.0, 0.2])
            );

        let mut plot = Plot::new();
        plot.add_trace(Box::new(candlestick_trace));
        plot.add_trace(volume_trace);
        plot.add_trace(ma50_trace);
        plot.add_trace(ma200_trace);
        plot.add_trace(rsi_trace);
        
        let plot = set_layout(plot, layout, height, width);

        Ok(plot)

    }

    async fn candlestick_chart_live(&self, height: Option<usize>, width: Option<usize>) -> Result<Plot, Box<dyn Error>> {
        let mut ohlcv = self.get_chart().await?;
        if ohlcv.height() < 6 {
            return Err(format!("Not enough data found for symbol {}", self.ticker).into());
        }
        if ohlcv.height() > 250 {
            // drop enough values to limit the graphs to < 100 values
            let num_to_average = ((ohlcv.height() as f64/ 250.0) + 0.5).round();
            ohlcv = match crate::data::sql::to_dataframe::smooth_ohlcv(ohlcv.clone(), num_to_average as u32) {
                Ok(df) => df,
                Err(e) => {
                    tracing::error!("Unable to reduce dataframe to usable size! {:?}", e);
                    return Err(e);
                }
            }
        }
        let datetimes = match i64_column_to_datetime_vec(&ohlcv) {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn timestamps into dates! {:?}", error);
                return Err(error);
            }
        };
        let x = datetimes.iter().map(|x| x.to_string()).collect::<Vec<String>>();
        let open = ohlcv.column("open")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let high = ohlcv.column("high")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let low = ohlcv.column("low")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let close = ohlcv.column("close")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let volume = ohlcv.column("volume")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let rsi_df = self.rsi_df(ohlcv.clone(), 14, None).await?;
        let rsi_values = rsi_df.column("rsi-14")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let ma_50_df = self.sma_df(ohlcv.clone(), 50, None).await?;
        let ma_50_values = ma_50_df.column("sma-50")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let ma_200_df = self.sma_df(ohlcv.clone(), 200, None).await?;
        let ma_200_values = ma_200_df.column("sma-200")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let candlestick_trace = Candlestick::new(x.clone(), open, high, low, close)
            .name("Prices");
        let volume_trace = Bar::new(x.clone(), volume)
            .name("Volume")
            //.marker(Marker::new().color(NamedColor::Blue))
            .x_axis("x")
            .y_axis("y2");
        let rsi_trace = Scatter::new(x.clone(), rsi_values)
            .name("RSI 14")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let ma50_trace = Scatter::new(x.clone(), ma_50_values)
            .name("MA 50")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline));
        let ma200_trace = Scatter::new(x.clone(), ma_200_values)
            .name("MA 200")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline));
        let sql_connection = crate::data::sql::connect();
        let metadata = crate::data::sql::live_data::get_stock_metadata(sql_connection.clone(), &self.ticker);
        let title = format!("{} ({}) in {} via {}", metadata.name, self.ticker, metadata.currency, metadata.exchange);
        let layout = Layout::new()
            .title(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{} Intra-day Chart</span>", title))
            .grid(
                LayoutGrid::new()
                    .rows(3)
                    .columns(1)
                    .pattern(GridPattern::Coupled)
                    .row_order(RowOrder::TopToBottom)
            )
            .x_axis(
                Axis::new()
                    .range_slider(RangeSlider::new().visible(true))
                    .range_selector(RangeSelector::new().buttons(vec![
                        SelectorButton::new()
                            .count(1)
                            .label("1H")
                            .step(SelectorStep::Hour)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1D")
                            .step(SelectorStep::Day)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(6)
                            .label("6M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("YTD")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::ToDate),
                        SelectorButton::new()
                            .count(1)
                            .label("1Y")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .label("MAX")
                            .step(SelectorStep::All),
                    ])),
            )
            .y_axis(
                Axis::new()
                    .domain(&[0.4, 1.0])
            )
            .y_axis2(
                Axis::new()
                    .domain(&[0.2, 0.4])
            )
            .y_axis3(
                Axis::new()
                    .domain(&[0.0, 0.2])
            );

        let mut plot = Plot::new();
        plot.add_trace(Box::new(candlestick_trace));
        plot.add_trace(volume_trace);
        plot.add_trace(ma50_trace);
        plot.add_trace(ma200_trace);
        plot.add_trace(rsi_trace);
        
        let plot = set_layout(plot, layout, height, width);

        Ok(plot)

    }

    async fn candlestick_chart_live_df(
        &self, 
        ohlcv: DataFrame, 
        metadata: &crate::data::sql::MetaData, 
        height: Option<usize>, 
        width: Option<usize>
    ) -> Result<Plot, Box<dyn Error>> {
        let mut ohlcv = ohlcv;
        if ohlcv.height() < 6 {
            return Err(format!("Not enough data found for symbol {}", self.ticker).into());
        }
        if ohlcv.height() > 250 {
            // drop enough values to limit the graphs to < 100 values
            let num_to_average = ((ohlcv.height() as f64/ 250.0) + 0.5).round();
            ohlcv = match crate::data::sql::to_dataframe::smooth_ohlcv(ohlcv.clone(), num_to_average as u32) {
                Ok(df) => df,
                Err(e) => {
                    tracing::error!("Unable to reduce dataframe to usable size! {:?}", e);
                    return Err(e);
                }
            }
        }
        let datetimes = match i64_column_to_datetime_vec(&ohlcv) {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn timestamps into dates! {:?}", error);
                return Err(error);
            }
        };
        let x = datetimes.iter().map(|x| x.to_string()).collect::<Vec<String>>();
        let open = ohlcv.column("open")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let high = ohlcv.column("high")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let low = ohlcv.column("low")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let close = ohlcv.column("close")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let volume = ohlcv.column("volume")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let rsi_df = self.rsi_df(ohlcv.clone(), 14, None).await?;
        let rsi_values = rsi_df.column("rsi-14")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let ma_50_df = self.sma_df(ohlcv.clone(), 50, None).await?;
        let ma_50_values = ma_50_df.column("sma-50")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let ma_200_df = self.sma_df(ohlcv.clone(), 200, None).await?;
        let ma_200_values = ma_200_df.column("sma-200")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let candlestick_trace = Candlestick::new(x.clone(), open, high, low, close)
            .name("Prices");
        let volume_trace = Bar::new(x.clone(), volume)
            .name("Volume")
            //.marker(Marker::new().color(NamedColor::Blue))
            .x_axis("x")
            .y_axis("y2");
        let rsi_trace = Scatter::new(x.clone(), rsi_values)
            .name("RSI 14")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let ma50_trace = Scatter::new(x.clone(), ma_50_values)
            .name("MA 50")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline));
        let ma200_trace = Scatter::new(x.clone(), ma_200_values)
            .name("MA 200")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline));
        let title = format!("{} ({}) in {} via {}", metadata.name, metadata.symbol, metadata.currency, metadata.exchange);
        let layout = Layout::new()
            .title(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{} Intra-day Chart</span>", title))
            .grid(
                LayoutGrid::new()
                    .rows(3)
                    .columns(1)
                    .pattern(GridPattern::Coupled)
                    .row_order(RowOrder::TopToBottom)
            )
            .x_axis(
                Axis::new()
                    .range_slider(RangeSlider::new().visible(true))
                    .range_selector(RangeSelector::new().buttons(vec![
                        SelectorButton::new()
                            .count(1)
                            .label("1H")
                            .step(SelectorStep::Hour)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1D")
                            .step(SelectorStep::Day)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(6)
                            .label("6M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("YTD")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::ToDate),
                        SelectorButton::new()
                            .count(1)
                            .label("1Y")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .label("MAX")
                            .step(SelectorStep::All),
                    ])),
            )
            .y_axis(
                Axis::new()
                    .domain(&[0.4, 1.0])
            )
            .y_axis2(
                Axis::new()
                    .domain(&[0.2, 0.4])
            )
            .y_axis3(
                Axis::new()
                    .domain(&[0.0, 0.2])
            );

        let mut plot = Plot::new();
        plot.add_trace(Box::new(candlestick_trace));
        plot.add_trace(volume_trace);
        plot.add_trace(ma50_trace);
        plot.add_trace(ma200_trace);
        plot.add_trace(rsi_trace);
        
        let plot = set_layout(plot, layout, height, width);

        Ok(plot)

    }

    /// Generates a performance chart for the ticker
    ///
    /// # Arguments
    ///
    /// * `height` - `usize` - Height of the chart
    /// * `width` - `usize` - Width of the chart
    ///
    /// # Returns
    ///
    /// * `Plot` Plotly Chart struct
    async fn performance_chart(&self, height: Option<usize>, width: Option<usize>) -> Result<Plot, Box<dyn Error>> {
        let performance_stats = self.performance_stats().await?;
        let dates = performance_stats.dates_array;
        let returns = performance_stats.security_returns.clone().f64().unwrap().to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();

        let benchmark_returns = performance_stats.benchmark_returns.f64().unwrap().to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();

        let cum_returns= cumulative_returns_list(returns.clone());

        let benchmark_cum_returns= cumulative_returns_list(benchmark_returns.clone());

        let (drawdowns, _) = maximum_drawdown(&performance_stats.security_returns);
        let drawdowns = drawdowns.iter().map(|x| x/100.0).collect::<Vec<f64>>();

        let returns_trace = Scatter::new(dates.clone(), returns.clone().iter().map(|x| x/100.0).collect::<Vec<f64>>())
            .name(format!("{} Returns", self.ticker))
            .mode(Mode::Markers)
            .fill(Fill::ToZeroY);

        let returns_dist_trace = Histogram::new(returns.clone().iter().map(|x| x/100.0).collect::<Vec<f64>>())
            .name(format!("{} Returns Distribution", self.ticker))
            .x_axis("x2")
            .y_axis("y2");

        let cum_returns_trace = Scatter::new(dates.clone(), cum_returns.clone())
            .name(format!("{} Cumulative Returns", self.ticker))
            .mode(Mode::Lines)
            .fill(Fill::ToZeroY)
            .x_axis("x3")
            .y_axis("y3");

        let benchmark_cum_returns_trace = Scatter::new(dates.clone(), benchmark_cum_returns.clone())
            .name(format!("{} Cumulative Returns", performance_stats.benchmark_symbol))
            .mode(Mode::Lines)
            .fill(Fill::ToZeroY)
            .x_axis("x3")
            .y_axis("y3");

        let drawdown_trace = Scatter::new(dates.clone(), drawdowns.clone())
            .name(format!("{} Drawdown", self.ticker))
            .mode(Mode::Lines)
            .fill(Fill::ToZeroY)
            .x_axis("x4")
            .y_axis("y4");

        let mut plot = Plot::new();
        plot.add_trace(returns_trace);
        plot.add_trace(returns_dist_trace);
        plot.add_trace(cum_returns_trace);
        plot.add_trace(benchmark_cum_returns_trace);
        plot.add_trace(drawdown_trace);

        // Set layout for the plot
        let layout = Layout::new()
            .title(Title::from(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{} Performance Chart</span>",
                                         self.ticker)))
            .grid(
                LayoutGrid::new()
                    .rows(4)
                    .columns(1)
                    .pattern(GridPattern::Independent)
                    .row_order(RowOrder::TopToBottom)
            )
            .y_axis(
                Axis::new()
                    .title(Title::from("Returns"))
                    .tick_format(".0%")
            )
            .y_axis2(
                Axis::new()
                    .title(Title::from("Returns Distribution"))
            )
            .x_axis2(
                Axis::new()
                    .tick_format(".0%")
            )
            .y_axis3(
                Axis::new()
                    .title(Title::from("Cumulative Returns"))
                    .tick_format(".0%")
            )
            .y_axis4(
                Axis::new()
                    .title(Title::from("Drawdown"))
                    .tick_format(".0%")
            );

        let plot = set_layout(plot, layout, height, width);

        Ok(plot)
    }

    /// Displays the Summary Statistics table for the ticker
    ///
    /// # Returns
    ///
    /// * `DataTable` - Table Chart struct
    async fn summary_stats_table(&self) -> Result<DataTable, Box<dyn Error>> {
        let stats = self.get_ticker_stats().await?;
        let df = stats.to_dataframe()?;
        let table = df.to_datatable("summary_stats", false, DataTableFormat::Number);
        Ok(table)
    }

    /// Displays the Performance Statistics table for the ticker
    ///
    /// # Returns
    ///
    /// * `DataTable` - Table Chart struct
    async fn performance_stats_table(&self) -> Result<DataTable, Box<dyn Error>> {
        let stats = self.performance_stats().await?;

        let fields = vec![
            "Daily Return".to_string(),
            "Daily Volatility".to_string(),
            "Cumulative Return".to_string(),
            "Annualized Return".to_string(),
            "Annualized Volatility".to_string(),
            "Alpha".to_string(),
            "Beta".to_string(),
            "Sharpe Ratio".to_string(),
            "Sortino Ratio".to_string(),
            "Active Return".to_string(),
            "Active Risk".to_string(),
            "Information Ratio".to_string(),
            "Calmar Ratio".to_string(),
            "Maximum Drawdown".to_string(),
            "Value At Risk".to_string(),
            "Expected Shortfall".to_string(),
        ];

        let values = vec![
            format!("{:.2}%",stats.performance_stats.daily_return),
            format!("{:.2}%",stats.performance_stats.daily_volatility),
            format!("{:.2}%",stats.performance_stats.cumulative_return),
            format!("{:.2}%",stats.performance_stats.annualized_return),
            format!("{:.2}%",stats.performance_stats.annualized_volatility),
            format!("{:.2}",stats.performance_stats.alpha),
            format!("{:.2}",stats.performance_stats.beta),
            format!("{:.2}",stats.performance_stats.sharpe_ratio),
            format!("{:.2}",stats.performance_stats.sortino_ratio),
            format!("{:.2}%",stats.performance_stats.active_return),
            format!("{:.2}%",stats.performance_stats.active_risk),
            format!("{:.2}",stats.performance_stats.information_ratio),
            format!("{:.2}",stats.performance_stats.calmar_ratio),
            format!("{:.2}%",stats.performance_stats.maximum_drawdown),
            format!("{:.2}%",stats.performance_stats.value_at_risk),
            format!("{:.2}%",stats.performance_stats.expected_shortfall),
        ];

        let df = DataFrame::new(vec![
            Column::new("Metric".into(), fields),
            Column::new("Value".into(), values),
        ])?;

        let table = df.to_datatable("performance_stats", false, DataTableFormat::Number);

        Ok(table)
    }

    /// Generates Table Plots for the Ticker's Financial Statements
    ///
    /// # Arguments
    /// * `frequency` - `StatementFrequency` - Frequency of the Financial Statements
    ///
    /// # Returns
    ///
    /// * `FinancialsTables` - Financials Tables struct
    async fn financials_tables(&self, frequency: StatementFrequency) -> Result<FinancialsTables, Box<dyn Error>> {
        let data = self.get_financials(StatementType::IncomeStatement, frequency).await?;
        let income_statement = data.to_datatable(
            &format!("{frequency}IncomeStatement"),
            false, 
            DataTableFormat::Currency
        );

        let data = self.get_financials(StatementType::BalanceSheet, frequency).await?;
        let balance_sheet = data.to_datatable(
            &format!("{frequency}BalanceSheet"),
            false, 
            DataTableFormat::Currency
        );

        let data = self.get_financials(StatementType::CashFlowStatement, frequency).await?;
        let cashflow_statement = data.to_datatable(
            &format!("{frequency}CashFlowStatement"),
            false, 
            DataTableFormat::Currency
        );

        let data = self.get_financials(StatementType::FinancialRatios, frequency).await?;
        let financial_ratios = data.to_datatable(
            &format!("{frequency}FinancialRatios"),
            false, 
            DataTableFormat::Number
        );

        Ok(FinancialsTables {
            income_statement,
            balance_sheet,
            cashflow_statement,
            financial_ratios,
        })
    }

    /// Generates Charts of the Ticker's Option Volatility Surface, Smile, and Term Structure
    ///
    /// # Arguments
    ///
    /// * `height` - `usize` - Height of the chart
    /// * `width` - `usize` - Width of the chart
    ///
    /// # Returns
    ///
    /// * `OptionsCharts` - Options Charts struct
    async fn options_charts(&self, height: Option<usize>, width: Option<usize>) -> Result<OptionsCharts, Box<dyn Error>> {
        let vol_surface = self.volatility_surface().await?;
        let symbol = vol_surface.symbol;
        let ivols = vol_surface.ivols;
        let strikes = vol_surface.strikes;
        let ttms = vol_surface.ttms;

        // Volatility Surface
        let trace = Surface::new(ivols.clone()).x(strikes.clone()).y(ttms.clone());
        let mut surface_plot = Plot::new();
        surface_plot.add_trace(trace);

        let layout = Layout::new()
            .title(Title::from(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{symbol} Volatility Surface</span>")))
            .scene(
                LayoutScene::new()
                    .x_axis(
                        Axis::new()
                            .title(Title::from("Strike"))
                    )
                    .y_axis(
                        Axis::new()
                            .title(Title::from("Time to Maturity"))
                    )
                    .z_axis(Axis::new()
                        .title(Title::from("Implied Volatility")))

            );
        let surface_plot = set_layout(surface_plot, layout, height, width);

        // Volatility Smile
        let mut traces = Vec::new();

        for (index, ttm) in ttms.iter().enumerate() {
            let ivols = ivols[index].clone();
            let trace = Scatter::new(strikes.clone(), ivols)
                .mode(Mode::LinesMarkers)
                .line(Line::new().shape(LineShape::Spline))
                .name(&*format!("Volatility Smile - {ttm:.1} Months Expiration"));

            traces.push(trace);
        }

        let layout = Layout::new()
            .title(Title::from(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{symbol} Volatility Smile</span>")))
            .x_axis(Axis::new().title(Title::from("Strike")))
            .y_axis(Axis::new().title(Title::from("Implied Volatility")));

        let mut smile_plot = Plot::new();
        for trace in traces {
            smile_plot.add_trace(trace);
        }
        let smile_plot = set_layout(smile_plot, layout, height, width);


        // Volatility Term Structure
        let rows = ivols[0].len();
        let cols = ivols.len();
        let mut strike_vols: Vec<Vec<f64>>= vec![vec![Default::default(); cols]; rows];

        for (j, col) in ivols.iter().enumerate() {
            for (i, &val) in col.iter().enumerate() {
                strike_vols[i][j] = val;
            }
        }
        let mut traces = Vec::new();


        for (index, strike) in strikes.iter().enumerate() {
            let ivols = strike_vols[index].clone();
            let trace = Scatter::new(ttms.clone(), ivols)
                .mode(Mode::LinesMarkers)
                .line(Line::new().shape(LineShape::Spline))
                .name(&*format!("Volatility Cone - {strike} Strike"));

            traces.push(trace);
        }

        let layout = Layout::new()
            .title(Title::from(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{symbol} Volatility Term Structure</span>")))
            .x_axis(Axis::new().title(Title::from("Time to Maturity (Months)")))
            .y_axis(Axis::new().title(Title::from("Implied Volatility")));

        let mut term_plot = Plot::new();
        for trace in traces {
            term_plot.add_trace(trace);
        }
        let term_plot = set_layout(term_plot, layout, height, width);


        Ok(OptionsCharts {
            volatility_surface: surface_plot,
            volatility_smile: smile_plot,
            volatility_term_structure: term_plot,
        })
    }

    /// Generates Tables of the Ticker's Options Chain and Volatility Surface Data
    ///
    /// # Returns
    ///
    /// * `OptionsTables` - Options Tables struct
    async fn options_tables(&self) -> Result<OptionsTables, Box<dyn Error>> {
        // Options Chain
        let data = self.get_options().await?.chain;
        let options_chain = data.to_datatable("options_chain", true, DataTableFormat::Number);

        // Volatility Surface
        let data = self.volatility_surface().await?.ivols_df;
        let volatility_surface = data.to_datatable("volatility_surface", true, DataTableFormat::Number);

        Ok(OptionsTables {
            options_chain,
            volatility_surface,
        })
    }

    /// Generates a News Sentiment Chart for the Ticker
    ///
    /// # Arguments
    ///
    /// * `height` - `Option<usize>` - Height of the chart
    /// * `width` - `Option<usize>` - Width of the chart
    ///
    /// # Returns
    ///
    /// * `Plot` - Plotly Chart struct
    async fn news_sentiment_chart(&self, height: Option<usize>, width: Option<usize>) -> Result<Plot, Box<dyn Error>> {
        let data = self.get_news().await?;
        let data = data.lazy()
            .with_column(col("Published Date").dt().date().alias("Published Date"));
        let grouped = data.clone().lazy().group_by_stable([col("Published Date")])
            .agg([
                col("Sentiment Score").mean().alias("Average Sentiment Score"),
                col("Sentiment Score").count().alias("Number of Articles"),
            ]).collect()?;
        let grouped = grouped.sort(["Published Date"], SortMultipleOptions::new().with_order_descending(false))?
            .lazy()
            .with_column(col("Published Date").cast(DataType::Datetime(TimeUnit::Milliseconds, None)).alias("Published Date"))
            .collect()?;


        // Convert to Vec for plotting
        let dates = grouped.column("Published Date")?.datetime()?
            .into_no_null_iter().map(|x| DateTime::from_timestamp_millis(x).unwrap()
            .naive_local().date().to_string()).collect::<Vec<_>>();
        let scores = grouped.column("Average Sentiment Score")?.f64()?.into_no_null_iter().collect::<Vec<_>>();
        let counts = grouped.column("Number of Articles")?.u32()?.into_no_null_iter().collect::<Vec<_>>();

        // Create Plotly traces
        let bar_trace = Bar::new(dates.clone(), counts)
            .name("Articles Count")
            .opacity(0.7);

        let line_trace = Scatter::new(dates, scores)
            .mode(Mode::LinesMarkers)
            .name("Sentiment Score")
            .y_axis("y2");

        // Create the Plotly plot
        let mut plot = Plot::new();
        plot.add_trace(bar_trace);
        plot.add_trace(line_trace);

        // Set the layout
        let layout = Layout::new()
            .title(Title::from(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{} News Sentiment Chart</span>", &self.ticker)))
            //.bar_mode(BarMode::Group)
            .x_axis(Axis::new()
                .title("Published Date")
                .color("purple")
                .show_grid(false))
            .y_axis(Axis::new()
                .title("Number of Articles")
                .color("purple")
                .show_grid(false))
            .y_axis2(Axis::new()
                .title("Average Sentiment Score")
                .color("purple")
                .show_grid(false)
                .overlaying("y")
                .side(AxisSide::Right)
            );

        let plot = set_layout(plot, layout, height, width);

        Ok(plot)
    }
    
    /// Generates a News Sentiment Table for the Ticker
    /// 
    /// # Returns
    /// * `DataTable` - Table Chart struct
    async fn news_sentiment_table(&self) -> Result<DataTable, Box<dyn Error>> {
        let mut news = self.get_news().await?;
        let _ = news.drop_in_place("Title")?;
        news.rename("Link", "Title".into())?;
        let news_table = news.to_datatable("News", true, DataTableFormat::Number);
        Ok(news_table)
    }

    async fn macd_chart_recent(
        &self, 
        ohlcv: DataFrame, 
        metadata: &crate::data::sql::MetaData,
        height: Option<usize>, 
        width: Option<usize>
    ) -> Result<Plot, Box<dyn Error>> {
        let mut ohlcv = ohlcv;
        if ohlcv.height() < 6 {
            return Err(format!("Not enough data found for symbol {}", self.ticker).into());
        }
        if ohlcv.height() > 250 {
            // drop enough values to limit the graphs to < 100 values
            let num_to_average = ((ohlcv.height() as f64/ 250.0) + 0.5).round();
            ohlcv = match crate::data::sql::to_dataframe::smooth_ohlcv(ohlcv.clone(), num_to_average as u32) {
                Ok(df) => df,
                Err(e) => {
                    tracing::error!("Unable to reduce dataframe to usable size! {:?}", e);
                    return Err(e);
                }
            }
        }
        let datetimes = match i64_column_to_datetime_vec(&ohlcv) {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn timestamps into dates! {:?}", error);
                return Err(error);
            }
        };
        let x = datetimes.iter().map(|x| x.to_string()).collect::<Vec<String>>();
        let macd_df = self.macd_df(ohlcv.clone(), 12, 26, 9, None).await?;
        let macd_values = macd_df.column("macd-(12,26,9)")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let macd_signal_values = macd_df.column("macd_signal-(12,26,9)")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let macd_divergence_values = macd_df.column("macd_divergence-(12,26,9)")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let open = ohlcv.column("open")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let high = ohlcv.column("high")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let low = ohlcv.column("low")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let close = ohlcv.column("close")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let volume = ohlcv.column("volume")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let candlestick_trace = Candlestick::new(x.clone(), open, high, low, close)
            .name("Prices");
        let volume_trace = Bar::new(x.clone(), volume)
            .name("Volume")
            //.marker(Marker::new().color(NamedColor::Blue))
            .x_axis("x")
            .y_axis("y2");
        let macd_trace = Scatter::new(x.clone(), macd_values)
            .name("MACD")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let macd_signal_trace = Scatter::new(x.clone(), macd_signal_values)
            .name("MACD Signal")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let macd_divergence_trace = Scatter::new(x.clone(), macd_divergence_values)
            .name("MACD Divergence")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let title = format!("{} ({}) in {} via {}", metadata.name, metadata.symbol, metadata.currency, metadata.exchange);
        let layout = Layout::new()
            .title(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{} MACD Chart</span>", title))
            .grid(
                LayoutGrid::new()
                    .rows(3)
                    .columns(1)
                    .pattern(GridPattern::Coupled)
                    .row_order(RowOrder::TopToBottom)
            )
            .x_axis(
                Axis::new()
                    .range_slider(RangeSlider::new().visible(true))
                    .range_selector(RangeSelector::new().buttons(vec![
                        SelectorButton::new()
                            .count(1)
                            .label("1H")
                            .step(SelectorStep::Hour)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1D")
                            .step(SelectorStep::Day)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(6)
                            .label("6M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("YTD")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::ToDate),
                        SelectorButton::new()
                            .count(1)
                            .label("1Y")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .label("MAX")
                            .step(SelectorStep::All),
                    ])),
            )
            .y_axis(
                Axis::new()
                    .domain(&[0.4, 1.0])
            )
            .y_axis2(
                Axis::new()
                    .domain(&[0.2, 0.4])
            )
            .y_axis3(
                Axis::new()
                    .domain(&[0.0, 0.2])
            );

        let mut plot = Plot::new();
        plot.add_trace(Box::new(candlestick_trace));
        plot.add_trace(volume_trace);
        plot.add_trace(macd_trace);
        plot.add_trace(macd_signal_trace);
        plot.add_trace(macd_divergence_trace);
        
        let plot = set_layout(plot, layout, height, width);

        Ok(plot)

    }

    async fn ppo_chart_recent(
        &self, 
        ohlcv: DataFrame, 
        metadata: &crate::data::sql::MetaData,
        height: Option<usize>, 
        width: Option<usize>
    ) -> Result<Plot, Box<dyn Error>> {
        let mut ohlcv = ohlcv;
        if ohlcv.height() < 6 {
            return Err(format!("Not enough data found for symbol {}", self.ticker).into());
        }
        if ohlcv.height() > 250 {
            // drop enough values to limit the graphs to < 100 values
            let num_to_average = ((ohlcv.height() as f64/ 250.0) + 0.5).round();
            ohlcv = match crate::data::sql::to_dataframe::smooth_ohlcv(ohlcv.clone(), num_to_average as u32) {
                Ok(df) => df,
                Err(e) => {
                    tracing::error!("Unable to reduce dataframe to usable size! {:?}", e);
                    return Err(e);
                }
            }
        }
        let datetimes = match i64_column_to_datetime_vec(&ohlcv) {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn timestamps into dates! {:?}", error);
                return Err(error);
            }
        };
        let x = datetimes.iter().map(|x| x.to_string()).collect::<Vec<String>>();
        let ppo_df = self.ppo_df(ohlcv.clone(), 12, 26, 9, None).await?;
        let ppo_values = ppo_df.column("ppo-(12,26,9)")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let ppo_signal_values = ppo_df.column("ppo_signal-(12,26,9)")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let ppo_divergence_values = ppo_df.column("ppo_divergence-(12,26,9)")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let open = ohlcv.column("open")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let high = ohlcv.column("high")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let low = ohlcv.column("low")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let close = ohlcv.column("close")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let volume = ohlcv.column("volume")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let candlestick_trace = Candlestick::new(x.clone(), open, high, low, close)
            .name("Prices");
        let volume_trace = Bar::new(x.clone(), volume)
            .name("Volume")
            //.marker(Marker::new().color(NamedColor::Blue))
            .x_axis("x")
            .y_axis("y2");
        let ppo_trace = Scatter::new(x.clone(), ppo_values)
            .name("PPO")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let ppo_signal_trace = Scatter::new(x.clone(), ppo_signal_values)
            .name("PPO Signal")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let ppo_divergence_trace = Scatter::new(x.clone(), ppo_divergence_values)
            .name("PPO Divergence")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let title = format!("{} ({}) in {} via {}", metadata.name, metadata.symbol, metadata.currency, metadata.exchange);
        let layout = Layout::new()
            .title(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{} PPO Chart</span>", title))
            .grid(
                LayoutGrid::new()
                    .rows(3)
                    .columns(1)
                    .pattern(GridPattern::Coupled)
                    .row_order(RowOrder::TopToBottom)
            )
            .x_axis(
                Axis::new()
                    .range_slider(RangeSlider::new().visible(true))
                    .range_selector(RangeSelector::new().buttons(vec![
                        SelectorButton::new()
                            .count(1)
                            .label("1H")
                            .step(SelectorStep::Hour)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1D")
                            .step(SelectorStep::Day)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(6)
                            .label("6M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("YTD")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::ToDate),
                        SelectorButton::new()
                            .count(1)
                            .label("1Y")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .label("MAX")
                            .step(SelectorStep::All),
                    ])),
            )
            .y_axis(
                Axis::new()
                    .domain(&[0.4, 1.0])
            )
            .y_axis2(
                Axis::new()
                    .domain(&[0.2, 0.4])
            )
            .y_axis3(
                Axis::new()
                    .domain(&[0.0, 0.2])
            );

        let mut plot = Plot::new();
        plot.add_trace(Box::new(candlestick_trace));
        plot.add_trace(volume_trace);
        plot.add_trace(ppo_trace);
        plot.add_trace(ppo_signal_trace);
        plot.add_trace(ppo_divergence_trace);
        
        let plot = set_layout(plot, layout, height, width);

        Ok(plot)

    }
    
    async fn mfi_chart_recent(
        &self, 
        ohlcv: DataFrame, 
        metadata: &crate::data::sql::MetaData,
        height: Option<usize>, 
        width: Option<usize>
    ) -> Result<Plot, Box<dyn Error>> {
        let mut ohlcv = ohlcv;
        if ohlcv.height() < 6 {
            return Err(format!("Not enough data found for symbol {}", self.ticker).into());
        }
        if ohlcv.height() > 250 {
            // drop enough values to limit the graphs to < 100 values
            let num_to_average = ((ohlcv.height() as f64/ 250.0) + 0.5).round();
            ohlcv = match crate::data::sql::to_dataframe::smooth_ohlcv(ohlcv.clone(), num_to_average as u32) {
                Ok(df) => df,
                Err(e) => {
                    tracing::error!("Unable to reduce dataframe to usable size! {:?}", e);
                    return Err(e);
                }
            }
        }
        let datetimes = match i64_column_to_datetime_vec(&ohlcv) {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn timestamps into dates! {:?}", error);
                return Err(error);
            }
        };
        let x = datetimes.iter().map(|x| x.to_string()).collect::<Vec<String>>();
        let mfi_df = self.mfi_df(ohlcv.clone(), 14).await?;
        let mfi_values = mfi_df.column("mfi-14")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let open = ohlcv.column("open")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let high = ohlcv.column("high")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let low = ohlcv.column("low")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let close = ohlcv.column("close")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let volume = ohlcv.column("volume")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let candlestick_trace = Candlestick::new(x.clone(), open, high, low, close)
            .name("Prices");
        let volume_trace = Bar::new(x.clone(), volume)
            .name("Volume")
            //.marker(Marker::new().color(NamedColor::Blue))
            .x_axis("x")
            .y_axis("y2");
        let mfi_trace = Scatter::new(x.clone(), mfi_values)
            .name("MFI")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let title = format!("{} ({}) in {} via {}", metadata.name, metadata.symbol, metadata.currency, metadata.exchange);
        let layout = Layout::new()
            .title(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{} MFI Chart</span>", title))
            .grid(
                LayoutGrid::new()
                    .rows(3)
                    .columns(1)
                    .pattern(GridPattern::Coupled)
                    .row_order(RowOrder::TopToBottom)
            )
            .x_axis(
                Axis::new()
                    .range_slider(RangeSlider::new().visible(true))
                    .range_selector(RangeSelector::new().buttons(vec![
                        SelectorButton::new()
                            .count(1)
                            .label("1H")
                            .step(SelectorStep::Hour)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1D")
                            .step(SelectorStep::Day)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(6)
                            .label("6M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("YTD")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::ToDate),
                        SelectorButton::new()
                            .count(1)
                            .label("1Y")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .label("MAX")
                            .step(SelectorStep::All),
                    ])),
            )
            .y_axis(
                Axis::new()
                    .domain(&[1.0, 1.0])
            );

        let mut plot = Plot::new();
        plot.add_trace(Box::new(candlestick_trace));
        plot.add_trace(volume_trace);
        plot.add_trace(mfi_trace);
        
        let plot = set_layout(plot, layout, height, width);

        Ok(plot)

    }
        
    async fn stochastic_chart_recent(
        &self, 
        ohlcv: DataFrame, 
        metadata: &crate::data::sql::MetaData,
        height: Option<usize>, 
        width: Option<usize>
    ) -> Result<Plot, Box<dyn Error>> {

        let mut ohlcv = ohlcv;
        if ohlcv.height() < 6 {
            return Err(format!("Not enough data found for symbol {}", self.ticker).into());
        }
        if ohlcv.height() > 250 {
            // drop enough values to limit the graphs to < 100 values
            let num_to_average = ((ohlcv.height() as f64/ 250.0) + 0.5).round();
            ohlcv = match crate::data::sql::to_dataframe::smooth_ohlcv(ohlcv.clone(), num_to_average as u32) {
                Ok(df) => df,
                Err(e) => {
                    tracing::error!("Unable to reduce dataframe to usable size! {:?}", e);
                    return Err(e);
                }
            }
        }
        let datetimes = match i64_column_to_datetime_vec(&ohlcv) {
            Ok(df) => df,
            Err(error) => {
                tracing::error!("Unable to turn timestamps into dates! {:?}", error);
                return Err(error);
            }
        };
        let x = datetimes.iter().map(|x| x.to_string()).collect::<Vec<String>>();
        let bb_df = self.bb_df(ohlcv.clone(), 20, 2.0, None).await?;
        let bb_values = bb_df.column("bb-(20,2)")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let bb_upper_values = bb_df.column("bb_upper-(20,2)")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let bb_lower_values = bb_df.column("bb_lower-(20,2)")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let fs_df = self.fs_df(ohlcv.clone(), 14, None).await?;
        let fs_values = fs_df.column("fs-14")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let ss_df = self.ss_df(ohlcv.clone(), 7, 3, None).await?;
        let ss_values = ss_df.column("ss-(7,3)")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let sd_df = self.sd_df(ohlcv.clone(), 20, None).await?;
        let sd_values = sd_df.column("sd-20")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let open = ohlcv.column("open")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let high = ohlcv.column("high")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let low = ohlcv.column("low")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let close = ohlcv.column("close")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let volume = ohlcv.column("volume")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let candlestick_trace = Candlestick::new(x.clone(), open, high, low, close)
            .name("Prices");
        let volume_trace = Bar::new(x.clone(), volume)
            .name("Volume")
            //.marker(Marker::new().color(NamedColor::Blue))
            .x_axis("x")
            .y_axis("y2");
        let bb_trace = Scatter::new(x.clone(), bb_values)
            .name("BB")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline));
        let bb_upper_trace = Scatter::new(x.clone(), bb_upper_values)
            .name("BB Upper")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline));
        let bb_lower_trace = Scatter::new(x.clone(), bb_lower_values)
            .name("BB Lower")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline));
        let fs_trace = Scatter::new(x.clone(), fs_values)
            .name("Fast")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let ss_trace = Scatter::new(x.clone(), ss_values)
            .name("Slow")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let sd_trace = Scatter::new(x.clone(), sd_values)
            .name("Deviation")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let mad_df = self.mad_df(ohlcv.clone(), 20, None).await?;
        let mad_values = mad_df.column("mad-20")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let mad_trace = Scatter::new(x.clone(), mad_values)
            .name("MAD")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let atr_df = self.atr_df(ohlcv.clone(), 14).await?;
        let atr_values = atr_df.column("atr-14")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let atr_trace = Scatter::new(x.clone(), atr_values)
            .name("ATR")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let roc_df = self.roc_df(ohlcv.clone(), 1, None).await?;
        let roc_values = roc_df.column("roc-1")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let roc_trace = Scatter::new(x.clone(), roc_values)
            .name("ROC")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y3");
        let obv_df = self.obv_df(ohlcv.clone()).await?;
        let obv_values = obv_df.column("obv")?.f64()?.to_vec()
            .iter().map(|x| x.unwrap()).collect::<Vec<f64>>();
        let obv_trace = Scatter::new(x.clone(), obv_values)
            .name("OBV")
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline))
            .x_axis("x")
            .y_axis("y2");
        let title = format!("{} ({}) in {} via {}", metadata.name, metadata.symbol, metadata.currency, metadata.exchange);
        let layout = Layout::new()
            .title(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{} Stochastic Chart</span>", title))
            .grid(
                LayoutGrid::new()
                    .rows(3)
                    .columns(1)
                    .pattern(GridPattern::Coupled)
                    .row_order(RowOrder::TopToBottom)
            )
            .x_axis(
                Axis::new()
                    .range_slider(RangeSlider::new().visible(true))
                    .range_selector(RangeSelector::new().buttons(vec![
                        SelectorButton::new()
                            .count(1)
                            .label("1H")
                            .step(SelectorStep::Hour)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1D")
                            .step(SelectorStep::Day)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("1M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(6)
                            .label("6M")
                            .step(SelectorStep::Month)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .count(1)
                            .label("YTD")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::ToDate),
                        SelectorButton::new()
                            .count(1)
                            .label("1Y")
                            .step(SelectorStep::Year)
                            .step_mode(StepMode::Backward),
                        SelectorButton::new()
                            .label("MAX")
                            .step(SelectorStep::All),
                    ])),
            )
            .y_axis(
                Axis::new()
                    .domain(&[0.4, 1.0])
            )
            .y_axis2(
                Axis::new()
                    .domain(&[0.2, 0.4])
            )
            .y_axis3(
                Axis::new()
                    .domain(&[0.0, 0.2])
            );

        let mut plot = Plot::new();
        plot.add_trace(Box::new(candlestick_trace));
        plot.add_trace(volume_trace);
        plot.add_trace(bb_trace);
        plot.add_trace(bb_upper_trace);
        plot.add_trace(bb_lower_trace);
        plot.add_trace(fs_trace);
        plot.add_trace(ss_trace);
        plot.add_trace(sd_trace);
        plot.add_trace(mad_trace);
        plot.add_trace(atr_trace);
        plot.add_trace(roc_trace);
        plot.add_trace(obv_trace);
       
        let plot = set_layout(plot, layout, height, width);

        Ok(plot)

    }

}
