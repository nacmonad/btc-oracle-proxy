//! HTTP route definitions

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct PriceResponse {
    pub symbol: String,
    pub price: f64,
    pub timestamp: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndicatorResponse {
    pub ema_12: Option<f64>,
    pub ema_26: Option<f64>,
    pub rsi_14: Option<f64>,
    pub momentum: Option<f64>,
}

// TODO: Implement route handlers
// GET /api/v1/price/{symbol}
// GET /api/v1/indicators
// GET /api/v1/health
// GET /api/v1/metrics
