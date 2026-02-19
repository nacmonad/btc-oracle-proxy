//! HTTP REST API for querying prices and indicators

use crate::models::AppState;
use crate::error::OracleResult;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

pub mod routes;
pub mod handlers;

/// Starts HTTP API server
pub async fn run_server(
    _state: Arc<RwLock<AppState>>,
    listen_addr: &str,
) -> OracleResult<()> {
    info!("Starting HTTP API server on {}", listen_addr);

    // TODO: Implement HTTP server
    // 1. Create router
    // 2. Define routes:
    //    - GET /price/{symbol}
    //    - GET /indicators
    //    - GET /health
    //    - GET /metrics
    // 3. Start listener

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
