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
mod clob;
mod oracle_db;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use tokio::sync::RwLock;
use tracing::{info, warn, error as log_error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let log_buffer = tui::new_log_buffer();

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("oracle_proxy=info,tokio=warn"))
        .with(tui::log_layer::TuiLogLayer::new(log_buffer.clone(), 200))
        .init();

    info!("OracleProxy initializing...");

    let config = config::Config::load()?;
    let state = Arc::new(RwLock::new(models::AppState::new()));

    // ── Broadcast channel — aggregator publishes, WS clients subscribe ───────
    let (event_tx, _) = ws_server::broadcast::new_channel();

    // ── Shared status counters for the TUI ───────────────────────────────────
    let ws_online = Arc::new(AtomicBool::new(false));
    let ws_clients = Arc::new(AtomicUsize::new(0));

    // ── Oracle DB writer (oracle_ticks + signal_events) ──────────────────────
    let (oracle_writer, oracle_rx) = oracle_db::OracleDbWriter::new(20_000);
    tokio::spawn(async move {
        oracle_db::run_writer_loop(oracle_rx, 500).await;
    });

    // ── Optional CLOB writer + WS-first ingest ───────────────────────────────
    let clob_ui_state = Arc::new(RwLock::new(clob::ClobUiState::default()));
    let mut clob_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    if config.clob_enabled {
        let (writer, rx) = clob::ClobWriter::new(20_000);
        let flush_ms = config.clob_writer_flush_ms;
        let clob_ui_state_writer = clob_ui_state.clone();
        clob_tasks.push(tokio::spawn(async move {
            clob::writer::run_writer_loop(rx, flush_ms, clob_ui_state_writer).await;
        }));

        let cfg_clob = config.clone();
        let clob_ui_state_ws = clob_ui_state.clone();
        clob_tasks.push(tokio::spawn(async move {
            if let Err(e) = clob::ws::run_ws_first(writer, cfg_clob, clob_ui_state_ws).await {
                log_error!("CLOB WS-first loop error: {}", e);
            }
        }));

        clob_tasks.push(tokio::spawn(async move {
            clob::outcome_local::run_local_outcome_loop(30_000).await;
        }));

        info!("CLOB WS-first ingestion enabled");
    }

    // ── Aggregator ────────────────────────────────────────────────────────────
    let state_agg = state.clone();
    let cfg_agg = config.clone();
    let tx_agg = event_tx.clone();
    let oracle_writer_agg = oracle_writer.clone();
    let aggregator_task = tokio::spawn(async move {
        if let Err(e) = aggregator::run_aggregator(state_agg, cfg_agg, tx_agg, oracle_writer_agg).await {
            log_error!("Aggregator error: {}", e);
        }
    });

    // ── WebSocket server ──────────────────────────────────────────────────────
    let ws_addr = config.ws_listen_addr.clone();
    let state_ws = state.clone();
    let ws_online_srv = ws_online.clone();
    let ws_clients_srv = ws_clients.clone();
    let ws_task = tokio::spawn(async move {
        if let Err(e) = ws_server::run_server(
            state_ws,
            &ws_addr,
            event_tx,
            ws_online_srv,
            ws_clients_srv,
        )
        .await
        {
            log_error!("WebSocket server error: {}", e);
        }
    });

    // ── TUI ───────────────────────────────────────────────────────────────────
    let state_tui = state.clone();
    let tui_ws_addr = config.ws_listen_addr.clone();
    let clob_ui_state_tui = clob_ui_state.clone();
    let tui_task = tokio::spawn(async move {
        if let Err(e) = tui::run_tui(
            state_tui,
            clob_ui_state_tui,
            log_buffer,
            tui_ws_addr,
            ws_online,
            ws_clients,
        )
        .await
        {
            log_error!("TUI error: {e}");
        }
    });

    info!(ws = %config.ws_listen_addr, "OracleProxy started");

    tokio::select! {
        _ = aggregator_task => warn!("Aggregator task ended"),
        _ = ws_task         => warn!("WebSocket task ended"),
        _ = tui_task        => info!("TUI closed — shutting down"),
    }

    Ok(())
}
