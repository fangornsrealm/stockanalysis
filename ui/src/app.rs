use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Page {
    Home,
    Performance,
    Financials,
    Options,
    Portfolio,
    Screener,
    News,
    NotFound,
}
