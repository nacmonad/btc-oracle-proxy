//! OracleProxy — BTC Chainlink Round Trigger Detector

mod config;
mod error;
mod models;
mod aggregator;
mod indicators;
mod store;
mod ws_server;
mod http_api;
mod monitoring;
mod tui;

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn, error as log_error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    // Build the log capture buffer before tracing is initialised so the layer
    // can hold a reference to it from the start.
    let log_buffer = tui::new_log_buffer();

    // Layered subscriber: TUI log-capture only.
    // The fmt layer is intentionally omitted — the TUI owns the terminal and
    // writes to stdout directly via ratatui/crossterm.
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("oracle_proxy=info,tokio=warn"))
        .with(tui::log_layer::TuiLogLayer::new(log_buffer.clone(), 200))
        .init();

    info!("OracleProxy initializing...");

    let config = config::Config::load()?;
    let state = Arc::new(RwLock::new(models::AppState::new()));

    // ── Background tasks ────────────────────────────────────────────────────

    let state_agg = state.clone();
    let cfg_agg = config.clone();
    let aggregator_task = tokio::spawn(async move {
        if let Err(e) = aggregator::run_aggregator(state_agg, cfg_agg).await {
            log_error!("Aggregator error: {}", e);
        }
    });

    let ws_addr = config.ws_listen_addr.clone();
    let state_ws = state.clone();
    let ws_task = tokio::spawn(async move {
        if let Err(e) = ws_server::run_server(state_ws, &ws_addr).await {
            log_error!("WebSocket server error: {}", e);
        }
    });

    let http_addr = config.http_listen_addr.clone();
    let state_http = state.clone();
    let http_task = tokio::spawn(async move {
        if let Err(e) = http_api::run_server(state_http, &http_addr).await {
            log_error!("HTTP server error: {}", e);
        }
    });

    // ── TUI ─────────────────────────────────────────────────────────────────

    let state_tui = state.clone();
    let tui_task = tokio::spawn(async move {
        if let Err(e) = tui::run_tui(state_tui, log_buffer).await {
            log_error!("TUI error: {e}");
        }
    });

    info!(
        ws = %config.ws_listen_addr,
        http = %config.http_listen_addr,
        "OracleProxy started"
    );

    tokio::select! {
        _ = aggregator_task => warn!("Aggregator task ended"),
        _ = ws_task         => warn!("WebSocket task ended"),
        _ = http_task       => warn!("HTTP API task ended"),
        _ = tui_task        => info!("TUI closed — shutting down"),
    }

    Ok(())
}
