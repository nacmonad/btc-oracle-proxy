//! OracleProxy - Production-Ready BTC Price Oracle Service
//! 
//! Aggregates BTC prices from major exchanges, mimics Chainlink oracle calculation,
//! adds momentum indicators, and exposes WebSocket feed for Python trading bot integration.

mod config;
mod error;
mod models;
mod aggregator;
mod indicators;
mod store;
mod ws_server;
mod http_api;
mod monitoring;

use std::sync::Arc;
use tracing::{info, warn, error as log_error};
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging and tracing
    tracing_subscriber::fmt()
        .with_env_filter("oracle_proxy=info,tokio=warn")
        .with_target(true)
        .with_thread_ids(true)
        .init();

    info!("OracleProxy initializing...");

    // Load configuration
    let config = config::Config::load()?;
    info!("Configuration loaded: {:?}", config);

    // Initialize shared state
    let state = Arc::new(RwLock::new(models::AppState::new()));

    // Start background tasks
    let state_clone = state.clone();
    let config_clone = config.clone();
    let aggregator_task = tokio::spawn(async move {
        if let Err(e) = aggregator::run_aggregator(state_clone, config_clone).await {
            log_error!("Aggregator error: {}", e);
        }
    });

    // Start WebSocket server
    let ws_addr = config.ws_listen_addr.clone();
    let http_addr = config.http_listen_addr.clone();
    let state_clone = state.clone();
    let ws_task = tokio::spawn(async move {
        if let Err(e) = ws_server::run_server(state_clone, &ws_addr).await {
            log_error!("WebSocket server error: {}", e);
        }
    });

    // Start HTTP API server
    let state_clone = state.clone();
    let http_task = tokio::spawn(async move {
        if let Err(e) = http_api::run_server(state_clone, &http_addr).await {
            log_error!("HTTP server error: {}", e);
        }
    });

    info!("OracleProxy started successfully");
    info!("WebSocket server: ws://{}", config.ws_listen_addr);
    info!("HTTP API: http://{}", config.http_listen_addr);

    // Wait for any task to fail (they should run indefinitely)
    tokio::select! {
        _ = aggregator_task => warn!("Aggregator task ended"),
        _ = ws_task => warn!("WebSocket task ended"),
        _ = http_task => warn!("HTTP API task ended"),
    }

    Ok(())
}
