//! Data models for OracleProxy

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a single price point received from an exchange feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangePrice {
    pub exchange: String,
    pub price: f64,
    pub timestamp: DateTime<Utc>,
    pub volume: Option<f64>,
}

/// Last known committed Chainlink on-chain price — the "stale baseline".
/// Updated lazily (only on ≥0.1% deviation or heartbeat), which is the
/// staleness we're exploiting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainlinkBaseline {
    pub price: f64,
    pub updated_at: DateTime<Utc>,
}

impl ChainlinkBaseline {
    pub fn age_secs(&self) -> u64 {
        Utc::now()
            .signed_duration_since(self.updated_at)
            .num_seconds()
            .max(0) as u64
    }
}

/// Fully enriched price snapshot — emitted every aggregation tick.
/// Includes market price, Chainlink baseline, deviation, and indicators.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceUpdate {
    pub timestamp: DateTime<Utc>,
    pub symbol: String,

    /// Median-aggregated market price from exchange feeds.
    pub market_price: f64,

    /// Last committed Chainlink on-chain price (None until first RTDS update).
    pub chainlink_price: Option<f64>,

    /// Seconds since Chainlink last updated (staleness = opportunity window).
    pub chainlink_age_secs: Option<u64>,

    /// `(market_price - chainlink_price) / chainlink_price × 100`
    pub deviation_pct: Option<f64>,

    /// `true` when `abs(deviation_pct) >= 0.10` — OCR2 round likely triggering.
    pub round_imminent: bool,

    /// Raw per-exchange prices included for bot inspection.
    pub exchange_prices: HashMap<String, f64>,

    pub indicators: IndicatorValues,
}

/// Technical indicators calculated on the rolling price history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorValues {
    pub ema_12: Option<f64>,
    pub ema_26: Option<f64>,
    pub ema_50: Option<f64>,
    pub rsi_14: Option<f64>,
    pub momentum_10: Option<f64>,
    pub momentum_20: Option<f64>,
    pub volatility: Option<f64>,
    pub bb_upper: Option<f64>,
    pub bb_middle: Option<f64>,
    pub bb_lower: Option<f64>,
    pub macd: Option<f64>,
    pub macd_signal: Option<f64>,
    pub macd_histogram: Option<f64>,
}

impl Default for IndicatorValues {
    fn default() -> Self {
        Self {
            ema_12: None,
            ema_26: None,
            ema_50: None,
            rsi_14: None,
            momentum_10: None,
            momentum_20: None,
            volatility: None,
            bb_upper: None,
            bb_middle: None,
            bb_lower: None,
            macd: None,
            macd_signal: None,
            macd_histogram: None,
        }
    }
}

/// Shared application state — held behind `Arc<RwLock<AppState>>`.
pub struct AppState {
    /// Latest fully enriched price update (None until first aggregation tick).
    pub current_price: Option<PriceUpdate>,

    /// Rolling price history for indicator calculations.
    pub price_history: Vec<f64>,

    /// Per-exchange connection status, updated on every received message.
    pub exchange_status: DashMap<String, ExchangeStatus>,

    /// Last committed Chainlink on-chain price from Polymarket RTDS.
    pub chainlink_baseline: Option<ChainlinkBaseline>,

    pub last_update: Option<DateTime<Utc>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            current_price: None,
            price_history: Vec::with_capacity(1000),
            exchange_status: DashMap::new(),
            chainlink_baseline: None,
            last_update: None,
        }
    }
}

/// Connection health for a single exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeStatus {
    pub exchange: String,
    pub connected: bool,
    pub last_price: Option<f64>,
    pub last_update: Option<DateTime<Utc>>,
    pub error_count: u32,
    pub last_error: Option<String>,
}

/// Health check response for `/health`.
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub uptime_seconds: u64,
    pub exchanges: HashMap<String, String>,
    pub chainlink_baseline_age_secs: Option<u64>,
    pub price_freshness_ms: u64,
    pub websocket_clients: usize,
    pub last_price: Option<f64>,
}

/// WebSocket message types (server ↔ client).
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WsMessage {
    #[serde(rename = "subscribe")]
    Subscribe { channels: Vec<String> },

    #[serde(rename = "unsubscribe")]
    Unsubscribe { channels: Vec<String> },

    #[serde(rename = "price_update")]
    PriceUpdate { data: PriceUpdate },

    #[serde(rename = "heartbeat")]
    Heartbeat { timestamp: DateTime<Utc> },

    #[serde(rename = "error")]
    Error { message: String, code: u16 },

    #[serde(rename = "subscribed")]
    Subscribed { channel: String },
}
