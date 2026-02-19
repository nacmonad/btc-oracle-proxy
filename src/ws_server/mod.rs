//! WebSocket server for real-time price feeds to Python bot

use crate::models::AppState;
use crate::error::OracleResult;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

pub mod handler;
pub mod broadcast;

/// Starts WebSocket server
pub async fn run_server(
    _state: Arc<RwLock<AppState>>,
    listen_addr: &str,
) -> OracleResult<()> {
    info!("Starting WebSocket server on {}", listen_addr);

    // TODO: Implement WebSocket server
    // 1. Create listener
    // 2. Accept connections
    // 3. Handle subscriptions
    // 4. Broadcast price updates
    // 5. Handle heartbeats

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
