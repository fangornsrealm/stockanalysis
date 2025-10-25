pub mod portfolio;
pub mod ticker;
pub mod tickers;
use std::error::Error;

use plotly::common::{Line, LineShape, Mode};
use plotly::{Configuration, Layout, Plot, Scatter};
use plotly::layout::Axis;

pub fn set_layout(mut plot: Plot, mut layout: Layout, height: Option<usize>, width: Option<usize>) -> Plot {
    if let (Some(h), Some(w)) = (height, width) {
        layout = layout.height(h).width(w);
        plot.set_layout(layout);
        plot
    } else {
        layout = layout.auto_size(true);
        plot.set_layout(layout);
        plot.set_configuration(Configuration::default()
            .responsive(true)
            .fill_frame(true)
        );
        plot
    }
}

pub fn plotxy_one_curve(
    x: Vec<String>, val: Vec<f64>, val_label: &str, 
    xlabel: &str, ylabel: &str, title: &str, 
    height: Option<usize>, width: Option<usize>
) -> Result<Plot, Box<dyn Error>> {
    let y_trace = Scatter::new(x.clone(), val.clone())
            .name(val_label)
            .mode(Mode::Lines)
            .line(Line::new());

    let layout = Layout::new()
        .title(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{}</span>", title))
        .x_axis(
            Axis::new().title(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{}</span>", xlabel))
        )
        .y_axis(
            Axis::new().title(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{}</span>", ylabel))
        );

    let mut plot = Plot::new();
    plot.add_trace(y_trace);
    
    let plot = set_layout(plot, layout, height, width);

    Ok(plot)

}

pub fn plotxy_curves(
    x: Vec<String>, vals: &Vec<Vec<f64>>, val_labels: Vec<String>, 
    xlabel: &str, ylabel: &str, title: &str, 
    height: Option<usize>, width: Option<usize>
) -> Result<Plot, Box<dyn Error>> {
    if vals.len() != val_labels.len() || vals.len() == 0 {
        return Err(format!("Values and Labels are not in match!").into());
    }
    let mut traces = Vec::new();
    for i in 0..vals.len() {
        let trace = Scatter::new(x.clone(), vals[i].clone())
            .name(&val_labels[i])
            .mode(Mode::Lines)
            .line(Line::new().shape(LineShape::Spline));
        traces.push(trace);
    }
    let layout = Layout::new()
        .title(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{}</span>", title))
        .x_axis(
            Axis::new().title(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{}</span>", xlabel))
        )
        .y_axis(
            Axis::new().title(&*format!("<span style=\"font-weight:bold; color:darkgreen;\">{}</span>", ylabel))
        );

    let mut plot = Plot::new();
    for i in 0..traces.len() {
        plot.add_trace(traces[i].clone());
    }
    let plot = set_layout(plot, layout, height, width);

    Ok(plot)

}
