//! HTTP request handlers

use crate::models::HealthResponse;
use std::collections::HashMap;

pub async fn health_check() -> HealthResponse {
    HealthResponse {
        status: "healthy".to_string(),
        uptime_seconds: 0,
        exchanges: HashMap::new(),
        chainlink_baseline_age_secs: None,
        price_freshness_ms: 0,
        websocket_clients: 0,
        last_price: None,
    }
}

// TODO: Implement other handlers
// - get_price()
// - get_indicators()
// - get_metrics()
