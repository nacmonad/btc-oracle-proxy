//! Health check implementation

use crate::models::HealthResponse;
use std::collections::HashMap;

pub struct HealthChecker {
    // TODO: Add health check state
}

impl HealthChecker {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn check_health(&self) -> HealthResponse {
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
}
