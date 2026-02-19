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

    /// Last committed Chainlink on-chain price (polled from Polygon RPC).
    pub chainlink_baseline: Option<ChainlinkBaseline>,

    pub last_update: Option<DateTime<Utc>>,

    /// Timestamp when `round_imminent` first became true for the current event.
    /// Reset to None after a `RoundSettled` event fires.
    pub round_imminent_since: Option<DateTime<Utc>>,

    // ── Edge-detection state (previous-tick values) ──────────────────────────

    pub prev_round_imminent: bool,
    pub prev_deviation_approaching: bool,
    pub prev_bb_breakout_dir: Option<RoundDirection>,
    pub prev_bb_width_pct: Option<f64>,
    pub prev_pre_trigger: bool,
    /// Used to detect `round_settled` (baseline price changed).
    pub prev_chainlink_price: Option<f64>,

}

impl AppState {
    pub fn new() -> Self {
        Self {
            current_price: None,
            price_history: Vec::with_capacity(1000),
            exchange_status: DashMap::new(),
            chainlink_baseline: None,
            last_update: None,
            round_imminent_since: None,
            prev_round_imminent: false,
            prev_deviation_approaching: false,
            prev_bb_breakout_dir: None,
            prev_bb_width_pct: None,
            prev_pre_trigger: false,
            prev_chainlink_price: None,
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

/// Direction of a Chainlink round trigger.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum RoundDirection {
    Up,
    Down,
}

/// Fired once when deviation first crosses ±0.10% (rising edge only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoundTriggeredEvent {
    pub direction: RoundDirection,
    pub market_price: f64,
    pub chainlink_price: f64,
    pub chainlink_age_secs: u64,
    pub deviation_pct: f64,
    pub exchange_prices: HashMap<String, f64>,
}

/// Fired when the Chainlink poller detects a new committed on-chain price.
/// Marks the end of the opportunity window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoundSettledEvent {
    pub prev_price: f64,
    pub new_price: f64,
    pub price_delta: f64,
    pub delta_pct: f64,
    /// Seconds from when `round_imminent` first fired to when baseline updated.
    pub round_duration_secs: Option<u64>,
}

/// Fired (rising edge) when deviation first crosses the 0.07% approach zone.
/// Early warning before the 0.10% round trigger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviationApproachEvent {
    pub direction: RoundDirection,
    pub deviation_pct: f64,
    pub market_price: f64,
    pub chainlink_price: f64,
    pub chainlink_age_secs: u64,
}

/// Fired when price breaks outside its Bollinger Band while the bands are
/// expanding — indicates directional momentum building beyond recent range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BbBreakoutEvent {
    pub direction: RoundDirection,
    pub market_price: f64,
    pub bb_upper: f64,
    pub bb_lower: f64,
    /// `(bb_upper - bb_lower) / bb_middle × 100` — relative band width as % of price.
    pub bb_width_pct: f64,
    /// True when band width is wider than the previous tick (volatility expanding).
    pub bb_expanding: bool,
    pub deviation_pct: Option<f64>,
}

/// High-confidence pre-trigger convergence signal.
/// Fires (rising edge) when ≥2 independent signals agree AND deviation ≥ 0.05%.
/// Combination: BB breakout + deviation approach + aligned momentum (any two).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreTriggerAlertEvent {
    pub direction: RoundDirection,
    /// Which signals contributed: "bb_breakout", "deviation_approach",
    /// "momentum_surge", "rsi_extreme"
    pub signals: Vec<String>,
    pub deviation_pct: f64,
    pub market_price: f64,
    pub chainlink_price: f64,
    pub chainlink_age_secs: u64,
    pub bb_width_pct: Option<f64>,
    pub rsi_14: Option<f64>,
    pub momentum_10: Option<f64>,
}

/// All server → client WebSocket events share this envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsEvent {
    /// Regular tick (~500ms). Primary streaming update.
    Tick {
        ts: DateTime<Utc>,
        data: PriceUpdate,
    },
    /// Rising-edge signal: deviation just crossed ±0.10%.
    RoundTriggered {
        ts: DateTime<Utc>,
        data: RoundTriggeredEvent,
    },
    /// Chainlink baseline updated on-chain — opportunity window closed.
    RoundSettled {
        ts: DateTime<Utc>,
        data: RoundSettledEvent,
    },
    /// Deviation entered the 0.07% approach zone (rising edge).
    DeviationApproach {
        ts: DateTime<Utc>,
        data: DeviationApproachEvent,
    },
    /// Price broke outside Bollinger Band with bands expanding (rising edge).
    BbBreakout {
        ts: DateTime<Utc>,
        data: BbBreakoutEvent,
    },
    /// Multiple signals converging — high-confidence pre-trigger alert (rising edge).
    PreTriggerAlert {
        ts: DateTime<Utc>,
        data: PreTriggerAlertEvent,
    },
    /// Exchange feed connect/disconnect.
    ExchangeStatus {
        ts: DateTime<Utc>,
        exchange: String,
        status: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

/// Inbound client → server messages.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsClientMessage {
    Subscribe { channels: Vec<String> },
    Unsubscribe { channels: Vec<String> },
}

/// Legacy alias kept for compatibility with existing handler stubs.
pub type WsMessage = WsEvent;
