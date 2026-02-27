use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookUpdate {
    pub ts: DateTime<Utc>,
    pub condition_id: String,
    pub token_id: String,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub last_trade_price: Option<f64>,
    pub last_trade_size: Option<f64>,
    pub book_timestamp: Option<i64>,
    pub source: String,
}

impl BookUpdate {
    pub fn new(condition_id: String, token_id: String, bids: Vec<BookLevel>, asks: Vec<BookLevel>) -> Self {
        Self {
            ts: Utc::now(),
            condition_id,
            token_id,
            bids,
            asks,
            last_trade_price: None,
            last_trade_size: None,
            book_timestamp: None,
            source: "live_rust".to_string(),
        }
    }
}
