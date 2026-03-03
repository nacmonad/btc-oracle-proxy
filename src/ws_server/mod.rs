//! WebSocket server — streams canonical market_frame JSON.

pub mod broadcast;
pub mod handler;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::RwLock;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::clob::state::ClobUiState;
use crate::error::{OracleError, OracleResult};
use crate::models::{AppState, PriceUpdate, WsClientMessage, WsEvent};

use self::broadcast::EventSender;

#[derive(Clone, Debug)]
struct ClientSub {
    assets: HashSet<String>,
    timeframes: HashSet<String>,
}

impl Default for ClientSub {
    fn default() -> Self {
        Self {
            assets: ["BTC".to_string()].into_iter().collect(),
            timeframes: ["5m".to_string(), "15m".to_string()].into_iter().collect(),
        }
    }
}

pub async fn run_server(
    state: Arc<RwLock<AppState>>,
    clob_state: Arc<RwLock<ClobUiState>>,
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
                let clob_state = clob_state.clone();
                let clients = ws_clients.clone();
                tokio::spawn(async move {
                    handle_connection(stream, peer, rx, state, clob_state, clients).await;
                });
            }
            Err(e) => warn!("WS accept error: {e}"),
        }
    }
}

fn direction_to_str(d: &crate::models::RoundDirection) -> &'static str {
    match d {
        crate::models::RoundDirection::Up => "UP",
        crate::models::RoundDirection::Down => "DOWN",
    }
}

fn event_to_signal(evt: &WsEvent) -> Option<Value> {
    match evt {
        WsEvent::RoundTriggered { ts, data } => Some(json!({
            "event_type": "round_triggered",
            "ts_event": ts,
            "direction": direction_to_str(&data.direction),
            "meta": {
                "deviation_pct": data.deviation_pct,
                "chainlink_age_secs": data.chainlink_age_secs,
                "market_price": data.market_price,
                "chainlink_price": data.chainlink_price,
            }
        })),
        WsEvent::RoundSettled { ts, data } => Some(json!({
            "event_type": "round_settled",
            "ts_event": ts,
            "direction": Value::Null,
            "meta": {
                "prev_price": data.prev_price,
                "new_price": data.new_price,
                "delta_pct": data.delta_pct,
                "round_duration_secs": data.round_duration_secs,
            }
        })),
        WsEvent::DeviationApproach { ts, data } => Some(json!({
            "event_type": "deviation_approach",
            "ts_event": ts,
            "direction": direction_to_str(&data.direction),
            "meta": {
                "deviation_pct": data.deviation_pct,
                "chainlink_age_secs": data.chainlink_age_secs,
                "market_price": data.market_price,
                "chainlink_price": data.chainlink_price,
            }
        })),
        WsEvent::BbBreakout { ts, data } => Some(json!({
            "event_type": "bb_breakout",
            "ts_event": ts,
            "direction": direction_to_str(&data.direction),
            "meta": {
                "deviation_pct": data.deviation_pct,
                "bb_upper": data.bb_upper,
                "bb_lower": data.bb_lower,
                "bb_width_pct": data.bb_width_pct,
                "bb_expanding": data.bb_expanding,
            }
        })),
        WsEvent::PreTriggerAlert { ts, data } => Some(json!({
            "event_type": "pre_trigger_alert",
            "ts_event": ts,
            "direction": direction_to_str(&data.direction),
            "meta": {
                "deviation_pct": data.deviation_pct,
                "chainlink_age_secs": data.chainlink_age_secs,
                "signals": data.signals,
                "bb_width_pct": data.bb_width_pct,
                "rsi_14": data.rsi_14,
                "momentum_10": data.momentum_10,
            }
        })),
        _ => None,
    }
}

fn build_conditions(clob: &ClobUiState, sub: &ClientSub) -> Vec<Value> {
    use std::collections::HashMap;
    #[derive(Default)]
    struct CondAcc {
        timeframe: String,
        close_time: String,
        token_yes_id: String,
        token_no_id: String,
    }

    let mut by_condition: HashMap<String, CondAcc> = HashMap::new();
    for m in clob.markets.values() {
        if !sub.assets.contains(&m.asset.to_uppercase()) || !sub.timeframes.contains(&m.timeframe.to_lowercase()) {
            continue;
        }
        let e = by_condition.entry(m.condition_id.clone()).or_default();
        e.timeframe = m.timeframe.clone();
        e.close_time = m.close_time.clone();
        if m.side.eq_ignore_ascii_case("UP") {
            e.token_yes_id = m.token_id.clone();
        } else if m.side.eq_ignore_ascii_case("DOWN") {
            e.token_no_id = m.token_id.clone();
        }
    }

    let mut out = Vec::new();
    for (cid, acc) in by_condition {
        if acc.token_yes_id.is_empty() || acc.token_no_id.is_empty() {
            continue;
        }
        let yes = match clob.tokens.get(&acc.token_yes_id) {
            Some(v) => v,
            None => continue,
        };
        let no = match clob.tokens.get(&acc.token_no_id) {
            Some(v) => v,
            None => continue,
        };

        out.push(json!({
            "condition_id": cid,
            "timeframe": acc.timeframe,
            "close_time": acc.close_time,
            "token_yes_id": acc.token_yes_id,
            "token_no_id": acc.token_no_id,
            "clob_l1": {
                "yes": {"best_bid": yes.best_bid, "best_ask": yes.best_ask, "spread": yes.spread},
                "no":  {"best_bid": no.best_bid,  "best_ask": no.best_ask,  "spread": no.spread}
            },
            "clob_l2_top5": {
                "yes": {"bids": yes.bid_ladder, "asks": yes.ask_ladder},
                "no":  {"bids": no.bid_ladder,  "asks": no.ask_ladder}
            }
        }));
    }
    out
}

fn build_market_frame(seq: u64, tick: &PriceUpdate, signals: &[Value], sub: &ClientSub, clob: &ClobUiState) -> Value {
    let conditions = build_conditions(clob, sub);
    json!({
        "type": "market_frame",
        "schema_version": 1,
        "seq": seq,
        "ts_event": tick.timestamp,
        "ts_emit": chrono::Utc::now(),
        "asset": tick.symbol.split('/').next().unwrap_or("BTC").to_uppercase(),
        "subscribed_timeframes": sub.timeframes.iter().cloned().collect::<Vec<_>>(),
        "oracle": {
            "market_price": tick.market_price,
            "chainlink_price": tick.chainlink_price,
            "chainlink_age_secs": tick.chainlink_age_secs,
            "deviation_pct": tick.deviation_pct,
        },
        "signals": signals,
        "conditions": conditions,
    })
}

async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    mut rx: broadcast::EventReceiver,
    state: Arc<RwLock<AppState>>,
    clob_state: Arc<RwLock<ClobUiState>>,
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
    let mut sub = ClientSub::default();
    let mut seq: u64 = 0;

    // Send initial frame from current state.
    {
        let s = state.read().await;
        if let Some(ref price) = s.current_price {
            let cl = clob_state.read().await;
            let frame = build_market_frame(seq, price, &[], &sub, &cl);
            if let Ok(json) = serde_json::to_string(&frame) {
                let _ = sink.send(Message::Text(json.into())).await;
            }
            seq += 1;
        }
    }

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(event) => {
                        let mut signals: Vec<Value> = Vec::new();
                        if let Some(sig) = event_to_signal(&event) {
                            signals.push(sig);
                        }

                        // Emit a frame for EVERY incoming event (tick or signal),
                        // using latest current_price when this event isn't a Tick.
                        let maybe_tick = match &event {
                            WsEvent::Tick { data, .. } => Some(data.clone()),
                            _ => {
                                let s = state.read().await;
                                s.current_price.clone()
                            }
                        };

                        if let Some(tick) = maybe_tick {
                            let cl = clob_state.read().await;
                            let frame = build_market_frame(seq, &tick, &signals, &sub, &cl);
                            seq += 1;
                            match serde_json::to_string(&frame) {
                                Ok(json) => {
                                    if sink.send(Message::Text(json.into())).await.is_err() {
                                        break;
                                    }
                                }
                                Err(e) => warn!("Serialisation error: {e}"),
                            }
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        warn!("Client {peer} lagged by {n} events — some events dropped");
                    }
                    Err(RecvError::Closed) => break,
                }
            }
            msg = source.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<WsClientMessage>(&text) {
                            Ok(client_msg) => {
                                match &client_msg {
                                    WsClientMessage::Subscribe { assets, timeframes, .. } => {
                                        if !assets.is_empty() {
                                            sub.assets = assets.iter().map(|s| s.to_uppercase()).collect();
                                        }
                                        if !timeframes.is_empty() {
                                            sub.timeframes = timeframes.iter().map(|s| s.to_lowercase()).collect();
                                        }
                                        info!("WS client {peer} updated subscription assets={:?} timeframes={:?}", sub.assets, sub.timeframes);
                                    }
                                    WsClientMessage::Unsubscribe { .. } => {}
                                }
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
