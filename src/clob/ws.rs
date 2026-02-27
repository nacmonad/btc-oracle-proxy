use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use duckdb::Connection;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use crate::clob::metrics::derive_l2_metrics;
use crate::clob::writer::{ClobWriter, DepthLevel, SnapshotRow};
use crate::config::Config;

fn load_active_tokens(cfg: &Config) -> anyhow::Result<Vec<(String, String)>> {
    let db_path = std::env::var("RESEARCHER_DB_PATH")
        .or_else(|_| std::env::var("DB_PATH"))
        .unwrap_or_else(|_| "../data/researcher.db".to_string());

    let conn = Connection::open(&db_path)?;
    let mut stmt = conn.prepare(
        "SELECT condition_id, token_yes_id, token_no_id, asset, timeframe
         FROM pm_markets
         WHERE resolved_at IS NULL
           AND token_yes_id IS NOT NULL"
    )?;

    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        let cid: String = r.get(0)?;
        let yes: String = r.get(1)?;
        let no: Option<String> = r.get(2)?;
        let asset: String = r.get::<usize, String>(3).unwrap_or_default().to_uppercase();
        let tf: String = r.get::<usize, String>(4).unwrap_or_default().to_lowercase();

        if !cfg.clob_assets.is_empty() && !cfg.clob_assets.iter().any(|a| a == &asset) {
            continue;
        }
        if !cfg.clob_timeframes.is_empty() && !cfg.clob_timeframes.iter().any(|t| t == &tf) {
            continue;
        }

        out.push((cid.clone(), yes));
        if let Some(n) = no { out.push((cid.clone(), n)); }
    }
    Ok(out)
}

pub async fn run_ws_first(writer: ClobWriter, cfg: Config) -> anyhow::Result<()> {
    let mut backoff = cfg.clob_initial_backoff_ms.max(100);

    loop {
        let tokens = load_active_tokens(&cfg).context("load_active_tokens failed")?;
        if tokens.is_empty() {
            warn!("no active CLOB tokens found; retrying");
            tokio::time::sleep(Duration::from_millis(cfg.clob_poll_interval_ms.max(500))).await;
            continue;
        }

        info!(count=tokens.len(), "connecting CLOB WS");
        match connect_async(&cfg.clob_ws_url).await {
            Ok((ws, _)) => {
                info!("CLOB WS connected");
                backoff = cfg.clob_initial_backoff_ms.max(100);

                let (mut write, mut read) = ws.split();
                let assets: Vec<String> = tokens.iter().map(|(_, t)| t.clone()).collect();
                let sub = serde_json::json!({"type":"market","assets_ids": assets});
                write.send(Message::Text(sub.to_string())).await?;

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(txt)) => {
                            if let Err(e) = handle_message(&txt, &tokens, &writer).await {
                                warn!(error=%e, "clob ws handle message failed");
                            }
                        }
                        Ok(Message::Binary(bin)) => {
                            if let Ok(txt) = String::from_utf8(bin.to_vec()) {
                                if let Err(e) = handle_message(&txt, &tokens, &writer).await {
                                    warn!(error=%e, "clob ws handle binary message failed");
                                }
                            }
                        }
                        Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                        Ok(Message::Close(_)) => {
                            warn!("CLOB WS closed by remote");
                            break;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!(error=%e, "CLOB WS read error");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error=%e, "CLOB WS connect failed");
            }
        }

        let sleep_ms = backoff.min(cfg.clob_max_backoff_ms.max(backoff));
        warn!(sleep_ms, "CLOB WS reconnect backoff");
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        backoff = (backoff * 2).min(cfg.clob_max_backoff_ms.max(backoff));
    }
}

async fn handle_message(
    txt: &str,
    token_map: &[(String, String)],
    writer: &ClobWriter,
) -> anyhow::Result<()> {
    let v: serde_json::Value = serde_json::from_str(txt)?;

    // messages can be either object or array
    let items: Vec<serde_json::Value> = if v.is_array() {
        v.as_array().cloned().unwrap_or_default()
    } else {
        vec![v]
    };

    for item in items {
        let bids = item.get("bids").and_then(|x| x.as_array());
        let asks = item.get("asks").and_then(|x| x.as_array());
        if bids.is_none() && asks.is_none() { continue; }

        let asset_id = item
            .get("asset_id")
            .or_else(|| item.get("market"))
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        if asset_id.is_empty() { continue; }

        let condition_id = token_map
            .iter()
            .find(|(_, t)| t == &asset_id)
            .map(|(c, _)| c.clone())
            .unwrap_or_default();
        if condition_id.is_empty() { continue; }

        let parse_levels = |arr: Option<&Vec<serde_json::Value>>| -> Vec<crate::clob::BookLevel> {
            arr.unwrap_or(&Vec::new())
                .iter()
                .filter_map(|x| {
                    let p = x.get("price")?.as_str()?.parse::<f64>().ok()?;
                    let s = x.get("size")?.as_str()?.parse::<f64>().ok()?;
                    Some(crate::clob::BookLevel { price: p, size: s })
                })
                .collect()
        };

        let bid_levels = parse_levels(bids);
        let ask_levels = parse_levels(asks);
        let m = derive_l2_metrics(&bid_levels, &ask_levels);

        let row = SnapshotRow {
            ts: Utc::now(),
            condition_id,
            token_id: asset_id,
            best_bid: m.best_bid,
            best_ask: m.best_ask,
            mid_price: m.mid_price,
            spread: m.spread,
            bid_depth_1: m.bid_depth_1,
            ask_depth_1: m.ask_depth_1,
            bid_depth_5: m.bid_depth_5,
            ask_depth_5: m.ask_depth_5,
            bid_depth_10: m.bid_depth_10,
            ask_depth_10: m.ask_depth_10,
            depth_imbalance_5: m.depth_imbalance_5,
            depth_imbalance_10: m.depth_imbalance_10,
            slippage_100: m.slippage_100,
            slippage_1000: m.slippage_1000,
            total_bid_levels: m.total_bid_levels,
            total_ask_levels: m.total_ask_levels,
            book_timestamp: item.get("timestamp").and_then(|x| x.as_i64()),
            source: "live_rust".to_string(),
            last_trade_price: item.get("last_trade_price").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()),
            last_trade_size: None,
            bid_levels: bid_levels.iter().map(|l| DepthLevel { price: l.price, size: l.size }).collect(),
            ask_levels: ask_levels.iter().map(|l| DepthLevel { price: l.price, size: l.size }).collect(),
        };

        if !writer.try_enqueue(row) {
            error!("clob writer queue full; dropping snapshot row");
        }
    }

    Ok(())
}
