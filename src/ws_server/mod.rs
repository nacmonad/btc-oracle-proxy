//! WebSocket server — streams `WsEvent` JSON to connected Python bots.
//!
//! Each client gets a snapshot of the current price on connect, then receives
//! every subsequent event (tick + signal events) via the broadcast channel.
//!
//! Inbound messages (subscribe/unsubscribe) are accepted and logged; channel
//! filtering is a future enhancement — for now every client gets everything.

pub mod broadcast;
pub mod handler;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::RwLock;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::error::{OracleError, OracleResult};
use crate::models::{AppState, WsClientMessage, WsEvent};

use self::broadcast::EventSender;

pub async fn run_server(
    state: Arc<RwLock<AppState>>,
    listen_addr: &str,
    event_tx: EventSender,
    ws_online: Arc<AtomicBool>,
    ws_clients: Arc<AtomicUsize>,
) -> OracleResult<()> {
    let listener = TcpListener::bind(listen_addr)
        .await
        .map_err(|e| OracleError::HttpError(format!("WS bind failed: {e}")))?;

    ws_online.store(true, Ordering::Relaxed);
    info!("WebSocket server listening on ws://{}", listen_addr);

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let rx = event_tx.subscribe();
                let state = state.clone();
                let clients = ws_clients.clone();
                tokio::spawn(async move {
                    handle_connection(stream, peer, rx, state, clients).await;
                });
            }
            Err(e) => warn!("WS accept error: {e}"),
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    mut rx: broadcast::EventReceiver,
    state: Arc<RwLock<AppState>>,
    clients: Arc<AtomicUsize>,
) {
    let ws = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            warn!("WS handshake failed from {peer}: {e}");
            return;
        }
    };

    let count = clients.fetch_add(1, Ordering::Relaxed) + 1;
    info!("WS client connected: {peer}  (total: {count})");

    let (mut sink, mut source) = ws.split();

    // Send current snapshot immediately so the bot doesn't wait for next tick
    {
        let s = state.read().await;
        if let Some(ref price) = s.current_price {
            let snap = WsEvent::Tick {
                ts: price.timestamp,
                data: price.clone(),
            };
            if let Ok(json) = serde_json::to_string(&snap) {
                let _ = sink.send(Message::Text(json)).await;
            }
        }
    }

    loop {
        tokio::select! {
            // ── Outbound: broadcast events to client ──────────────────────
            result = rx.recv() => {
                match result {
                    Ok(event) => {
                        match serde_json::to_string(&event) {
                            Ok(json) => {
                                if sink.send(Message::Text(json)).await.is_err() {
                                    break; // client disconnected
                                }
                            }
                            Err(e) => warn!("Serialisation error: {e}"),
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        warn!("Client {peer} lagged by {n} events — some events dropped");
                    }
                    Err(RecvError::Closed) => break,
                }
            }

            // ── Inbound: handle subscribe / unsubscribe ────────────────────
            msg = source.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<WsClientMessage>(&text) {
                            Ok(client_msg) => {
                                let _ = handler::handle_message(client_msg).await;
                            }
                            Err(e) => debug!("Unrecognised message from {peer}: {e}"),
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = sink.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }

    let remaining = clients.fetch_sub(1, Ordering::Relaxed) - 1;
    info!("WS client disconnected: {peer}  (remaining: {remaining})");
}
