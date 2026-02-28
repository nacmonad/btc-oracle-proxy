use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use crate::clob::metrics::derive_l2_metrics;
use crate::clob::state::ClobUiState;
use crate::clob::writer::{ClobWriter, DepthLevel, MarketUpsertRow, SnapshotRow};
use crate::config::Config;

type TokenRec = (String, String, String, String, String, String); // (condition_id, token_id, asset, timeframe, close_time, side)

#[derive(Debug, Clone)]
struct MarketRec {
    condition_id: String,
    question: String,
    asset: String,
    timeframe: String,
    close_time: String,
    outcome: Option<String>,
    winning_price: Option<f64>,
    resolved_at: Option<String>,
    token_yes_id: String,
    token_no_id: String,
}

#[derive(Debug, Default)]
struct DiscoveryBatch {
    tokens: Vec<TokenRec>,
    markets: Vec<MarketRec>,
}

#[derive(Debug, Default, Clone)]
struct TokenSampleSlot {
    latest: Option<SnapshotRow>,
    last_emitted: Option<SnapshotRow>,
}

#[derive(Debug)]
struct L2Sampler {
    by_token: HashMap<String, TokenSampleSlot>,
    sample_interval_ms: i64,
    force_emit_ms: i64,
    ingested_rows: u64,
    emitted_rows: u64,
}

impl L2Sampler {
    fn new(cfg: &Config) -> Self {
        Self {
            by_token: HashMap::new(),
            sample_interval_ms: (cfg.clob_sample_interval_ms.max(1)) as i64,
            force_emit_ms: (cfg.clob_sample_force_emit_ms.max(1)) as i64,
            ingested_rows: 0,
            emitted_rows: 0,
        }
    }

    fn ingest(&mut self, row: SnapshotRow) {
        self.ingested_rows += 1;
        let slot = self.by_token.entry(row.token_id.clone()).or_default();
        slot.latest = Some(row);
    }

    fn take_ready(&mut self, now: DateTime<Utc>) -> Vec<SnapshotRow> {
        let mut out = Vec::new();

        for slot in self.by_token.values_mut() {
            let Some(latest) = slot.latest.clone() else { continue };

            let should_emit = match &slot.last_emitted {
                None => true,
                Some(prev) => {
                    let elapsed_ms = now.signed_duration_since(prev.ts).num_milliseconds();
                    let changed = row_changed_significantly(&latest, prev);
                    elapsed_ms >= self.sample_interval_ms && (changed || elapsed_ms >= self.force_emit_ms)
                }
            };

            if should_emit {
                let mut emitted = latest.clone();
                // IMPORTANT: each persisted snapshot must get a fresh ingest timestamp.
                // Re-emitting an unchanged cached row with the original ts can violate
                // pm_snapshots primary key (ts, token_id).
                emitted.ts = now;
                slot.last_emitted = Some(emitted.clone());
                out.push(emitted);
                self.emitted_rows += 1;
            }
        }

        out
    }
}

fn opt_changed(a: Option<f64>, b: Option<f64>, eps: f64) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => (x - y).abs() > eps,
        (None, None) => false,
        _ => true,
    }
}

fn row_changed_significantly(a: &SnapshotRow, b: &SnapshotRow) -> bool {
    opt_changed(a.best_bid, b.best_bid, 1e-9)
        || opt_changed(a.best_ask, b.best_ask, 1e-9)
        || opt_changed(a.spread, b.spread, 1e-9)
        || opt_changed(a.mid_price, b.mid_price, 1e-9)
        || opt_changed(a.bid_depth_5, b.bid_depth_5, 1e-6)
        || opt_changed(a.ask_depth_5, b.ask_depth_5, 1e-6)
        || opt_changed(a.depth_imbalance_5, b.depth_imbalance_5, 1e-6)
        || opt_changed(a.slippage_100, b.slippage_100, 1e-6)
        || a.total_bid_levels != b.total_bid_levels
        || a.total_ask_levels != b.total_ask_levels
}

fn sane_price(p: f64) -> bool {
    p.is_finite() && (0.0..=1.0).contains(&p)
}

fn sane_size(s: f64) -> bool {
    s.is_finite() && s >= 0.0
}

fn sane_opt(v: Option<f64>) -> Option<f64> {
    v.filter(|x| x.is_finite())
}

fn timeframe_secs(tf: &str) -> Option<i64> {
    match tf {
        "5m" => Some(300),
        "15m" => Some(900),
        _ => None,
    }
}

fn parse_close_time_to_utc(s: &str) -> Option<chrono::DateTime<Utc>> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%z") {
        return Some(dt.with_timezone(&Utc));
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(chrono::DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc));
    }
    None
}

fn should_persist_market_now(timeframe: &str, close_time: &str, now: chrono::DateTime<Utc>) -> bool {
    let Some(tf_secs) = timeframe_secs(timeframe) else { return false };
    let Some(close_ts) = parse_close_time_to_utc(close_time) else { return false };
    let secs_to_close = close_ts.signed_duration_since(now).num_seconds();
    secs_to_close >= 0 && secs_to_close <= tf_secs
}

fn slug_prefix(asset: &str) -> String {
    format!("{}-updown", asset.to_lowercase())
}

fn compute_window_slugs(asset: &str, timeframe: &str, lookahead: i64, lookbehind: i64) -> Vec<String> {
    let tf_secs = match timeframe_secs(timeframe) {
        Some(s) => s,
        None => return Vec::new(),
    };
    let now = Utc::now().timestamp();
    let window_start = now - (now % tf_secs);
    let prefix = slug_prefix(asset);

    let mut out = Vec::new();
    for i in -lookbehind..=lookahead {
        out.push(format!("{}-{}-{}", prefix, timeframe, window_start + i * tf_secs));
    }
    out
}

fn extract_tokens(m: &serde_json::Value) -> Option<(String, String)> {
    let raw = m.get("clobTokenIds")?;
    if let Some(s) = raw.as_str() {
        let arr: serde_json::Value = serde_json::from_str(s).ok()?;
        let a = arr.as_array()?;
        if a.len() >= 2 {
            let t0 = a[0].as_str()?.to_string();
            let t1 = a[1].as_str()?.to_string();
            return Some((t0, t1));
        }
    }
    if let Some(a) = raw.as_array() {
        if a.len() >= 2 {
            let t0 = a[0].as_str()?.to_string();
            let t1 = a[1].as_str()?.to_string();
            return Some((t0, t1));
        }
    }
    None
}

fn parse_market_outcome(m: &serde_json::Value) -> (Option<String>, Option<f64>, Option<String>) {
    let closed = m.get("closed").and_then(|x| x.as_bool()).unwrap_or(false);
    let mut outcome: Option<String> = None;

    if closed {
        let outcomes_raw = m.get("outcomes").cloned().unwrap_or(serde_json::Value::Null);
        let prices_raw = m.get("outcomePrices").cloned().unwrap_or(serde_json::Value::Null);

        let outcomes: Vec<String> = if let Some(s) = outcomes_raw.as_str() {
            serde_json::from_str::<Vec<String>>(s).unwrap_or_default()
        } else {
            outcomes_raw.as_array().map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect()).unwrap_or_default()
        };
        let prices: Vec<f64> = if let Some(s) = prices_raw.as_str() {
            serde_json::from_str::<Vec<String>>(s)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|x| x.parse::<f64>().ok())
                .collect()
        } else {
            prices_raw.as_array().map(|a| a.iter().filter_map(|v| v.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| v.as_f64())).collect()).unwrap_or_default()
        };

        for (name, p) in outcomes.into_iter().zip(prices.into_iter()) {
            if (p - 1.0).abs() < 1e-9 {
                let u = name.to_ascii_uppercase();
                outcome = Some(if u.contains("UP") || u.contains("YES") { "YES".to_string() } else { "NO".to_string() });
                break;
            }
        }
    }

    if outcome.is_none() {
        if let Some(raw) = m.get("outcome").and_then(|x| x.as_str()) {
            let u = raw.to_ascii_uppercase();
            if u == "UP" || u == "YES" {
                outcome = Some("YES".to_string());
            } else if u == "DOWN" || u == "NO" {
                outcome = Some("NO".to_string());
            }
        }
    }

    let winning_price = m
        .get("winningPrice")
        .or_else(|| m.get("winning_price"))
        .and_then(|x| x.as_f64().or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok())));

    let resolved_at = if outcome.is_some() {
        m.get("resolvedAt")
            .or_else(|| m.get("resolved_at"))
            .or_else(|| m.get("updatedAt"))
            .and_then(|x| x.as_str())
            .map(|s| s.to_string())
    } else {
        None
    };

    (outcome, winning_price, resolved_at)
}

fn to_market_upserts(markets: &[MarketRec]) -> Vec<MarketUpsertRow> {
    markets
        .iter()
        .map(|m| MarketUpsertRow {
            condition_id: m.condition_id.clone(),
            question: m.question.clone(),
            asset: m.asset.clone(),
            timeframe: m.timeframe.clone(),
            close_time: m.close_time.clone(),
            resolved_at: m.resolved_at.clone(),
            outcome: m.outcome.clone(),
            winning_price: m.winning_price,
            token_yes_id: m.token_yes_id.clone(),
            token_no_id: m.token_no_id.clone(),
        })
        .collect()
}

async fn load_active_tokens(cfg: &Config) -> anyhow::Result<DiscoveryBatch> {
    let client = Client::builder().timeout(Duration::from_secs(8)).build()?;
    let mut out: Vec<TokenRec> = Vec::new();
    let mut markets: Vec<MarketRec> = Vec::new();

    for asset in &cfg.clob_assets {
        for tf in &cfg.clob_timeframes {
            for slug in compute_window_slugs(asset, tf, 3, 1) {
                let url = format!("{}/markets", cfg.clob_gamma_api_url.trim_end_matches('/'));
                let res = client
                    .get(&url)
                    .query(&[("slug", slug.as_str()), ("limit", "1")])
                    .send()
                    .await;

                let Ok(resp) = res else { continue };
                if !resp.status().is_success() {
                    continue;
                }
                let v: serde_json::Value = match resp.json().await {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                let items: Vec<serde_json::Value> = if let Some(a) = v.as_array() {
                    a.clone()
                } else if let Some(a) = v.get("markets").and_then(|x| x.as_array()) {
                    a.clone()
                } else {
                    Vec::new()
                };

                for m in items {
                    let cid = m
                        .get("conditionId")
                        .or_else(|| m.get("condition_id"))
                        .and_then(|x| x.as_str())
                        .unwrap_or_default()
                        .to_string();
                    if cid.is_empty() {
                        continue;
                    }

                    let close_time = m
                        .get("endDate")
                        .or_else(|| m.get("end_date"))
                        .and_then(|x| x.as_str())
                        .unwrap_or("?")
                        .to_string();

                    let Some((yes, no)) = extract_tokens(&m) else { continue };
                    let question = m
                        .get("question")
                        .or_else(|| m.get("title"))
                        .and_then(|x| x.as_str())
                        .unwrap_or("?")
                        .to_string();
                    let (outcome, winning_price, resolved_at) = parse_market_outcome(&m);

                    markets.push(MarketRec {
                        condition_id: cid.clone(),
                        question,
                        asset: asset.clone(),
                        timeframe: tf.clone(),
                        close_time: close_time.clone(),
                        outcome,
                        winning_price,
                        resolved_at,
                        token_yes_id: yes.clone(),
                        token_no_id: no.clone(),
                    });

                    out.push((cid.clone(), yes, asset.clone(), tf.clone(), close_time.clone(), "UP".to_string()));
                    out.push((cid, no, asset.clone(), tf.clone(), close_time, "DOWN".to_string()));
                }
            }
        }
    }

    // de-dupe token ids
    let mut seen = HashSet::new();
    out.retain(|(_, token, _, _, _, _)| seen.insert(token.clone()));

    // de-dupe markets by condition_id (keep latest seen)
    let mut by_condition: HashMap<String, MarketRec> = HashMap::new();
    for m in markets {
        by_condition.insert(m.condition_id.clone(), m);
    }

    Ok(DiscoveryBatch {
        tokens: out,
        markets: by_condition.into_values().collect(),
    })
}

pub async fn run_ws_first(writer: ClobWriter, cfg: Config, ui_state: Arc<RwLock<ClobUiState>>) -> anyhow::Result<()> {
    let mut backoff = cfg.clob_initial_backoff_ms.max(100);

    loop {
        let discovery = match load_active_tokens(&cfg).await {
            Ok(d) => {
                info!(loaded_tokens=d.tokens.len(), loaded_markets=d.markets.len(), "load_active_tokens ok (slug-based)");
                d
            }
            Err(e) => {
                warn!("load_active_tokens failed: {:#}", e);
                DiscoveryBatch::default()
            }
        };

        let market_rows = to_market_upserts(&discovery.markets);
        if !market_rows.is_empty() && !writer.try_enqueue_markets(market_rows) {
            warn!("db writer queue full; dropping market upsert batch");
        }

        let tokens = discovery.tokens;

        if tokens.is_empty() {
            warn!("no active CLOB tokens found via slug discovery; retrying");
            tokio::time::sleep(Duration::from_millis(cfg.clob_poll_interval_ms.max(1000))).await;
            continue;
        }

        info!(count=tokens.len(), "connecting CLOB WS");
        match connect_async(&cfg.clob_ws_url).await {
            Ok((ws, _)) => {
                info!(sample_interval_ms=cfg.clob_sample_interval_ms, force_emit_ms=cfg.clob_sample_force_emit_ms, "CLOB WS connected");
                backoff = cfg.clob_initial_backoff_ms.max(100);
                let mut sampler = L2Sampler::new(&cfg);

                let (mut write, mut read) = ws.split();
                {
                    let mut s = ui_state.write().await;
                    s.markets.clear();
                    for (cid, token, asset, tf, ct, side) in &tokens {
                        s.set_market_meta(token.clone(), cid.clone(), asset.clone(), tf.clone(), ct.clone(), side.clone());
                    }
                }

                let assets: Vec<String> = tokens.iter().map(|(_, t, _, _, _, _)| t.clone()).collect();
                let sub = serde_json::json!({"type":"market","assets_ids": assets});
                write.send(Message::Text(sub.to_string())).await?;

                let mut refresh = tokio::time::interval(Duration::from_millis(cfg.clob_market_refresh_ms.max(30_000)));
                refresh.tick().await; // first immediate tick consumed

                let mut sample_tick = tokio::time::interval(Duration::from_millis(cfg.clob_sample_interval_ms.max(1)));
                sample_tick.tick().await; // align on interval
                let mut stats_tick = tokio::time::interval(Duration::from_secs(10));
                stats_tick.tick().await;

                loop {
                    tokio::select! {
                        _ = refresh.tick() => {
                            info!("CLOB WS market refresh tick: reconnecting to refresh subscriptions");
                            break;
                        }
                        _ = sample_tick.tick() => {
                            let sampled_rows = sampler.take_ready(Utc::now());
                            for row in sampled_rows {
                                if !writer.try_enqueue_snapshot(row) {
                                    {
                                        let mut s = ui_state.write().await;
                                        s.on_drop();
                                    }
                                    error!("clob writer queue full; dropping sampled snapshot row");
                                } else {
                                    let mut s = ui_state.write().await;
                                    s.on_enqueue();
                                }
                            }
                            let mut s = ui_state.write().await;
                            s.set_sampler_stats(sampler.ingested_rows, sampler.emitted_rows);
                        }
                        _ = stats_tick.tick() => {
                            let s = ui_state.read().await;
                            info!(
                                ws_tokens=s.tokens.len(),
                                ws_markets=s.markets.len(),
                                ws_enqueued=s.enqueued_rows,
                                ws_dropped=s.dropped_rows,
                                ws_reconnects=s.reconnects,
                                sampler_slots=sampler.by_token.len(),
                                sampler_ingested=sampler.ingested_rows,
                                sampler_emitted=sampler.emitted_rows,
                                "clob ws stats"
                            );
                        }
                        maybe = read.next() => {
                            let Some(msg) = maybe else { break; };
                            match msg {
                                Ok(Message::Text(txt)) => {
                                    if let Err(e) = handle_message(&txt, &tokens, &ui_state, &mut sampler).await {
                                        warn!(error=%e, "clob ws handle message failed");
                                    }
                                }
                                Ok(Message::Binary(bin)) => {
                                    if let Ok(txt) = String::from_utf8(bin.to_vec()) {
                                        if let Err(e) = handle_message(&txt, &tokens, &ui_state, &mut sampler).await {
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
                }
            }
            Err(e) => {
                warn!(error=%e, "CLOB WS connect failed");
            }
        }

        let sleep_ms = backoff.min(cfg.clob_max_backoff_ms.max(backoff));
        warn!(sleep_ms, "CLOB WS reconnect backoff");
        {
            let mut s = ui_state.write().await;
            s.on_reconnect(sleep_ms);
        }
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
        backoff = (backoff * 2).min(cfg.clob_max_backoff_ms.max(backoff));
    }
}

async fn handle_message(
    txt: &str,
    token_map: &[TokenRec],
    ui_state: &Arc<RwLock<ClobUiState>>,
    sampler: &mut L2Sampler,
) -> anyhow::Result<()> {
    let v: serde_json::Value = serde_json::from_str(txt)?;

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

        let found = token_map.iter().find(|(_, t, _, _, _, _)| t == &asset_id);
        let (condition_id, timeframe, close_time) = match found {
            Some((c, _, _, tf, ct, _)) => (c.clone(), tf.clone(), ct.clone()),
            None => continue,
        };

        let parse_levels = |arr: Option<&Vec<serde_json::Value>>| -> Vec<crate::clob::BookLevel> {
            arr.unwrap_or(&Vec::new())
                .iter()
                .filter_map(|x| {
                    let p = x.get("price")?.as_str()?.parse::<f64>().ok()?;
                    let s = x.get("size")?.as_str()?.parse::<f64>().ok()?;
                    if !sane_price(p) || !sane_size(s) {
                        return None;
                    }
                    Some(crate::clob::BookLevel { price: p, size: s })
                })
                .collect()
        };

        let bid_levels = parse_levels(bids);
        let ask_levels = parse_levels(asks);
        if bid_levels.is_empty() && ask_levels.is_empty() {
            let mut s = ui_state.write().await;
            s.on_malformed();
            continue;
        }

        let m = derive_l2_metrics(&bid_levels, &ask_levels);
        let best_bid = sane_opt(m.best_bid).filter(|v| sane_price(*v));
        let best_ask = sane_opt(m.best_ask).filter(|v| sane_price(*v));

        if best_bid.is_none() && best_ask.is_none() {
            let mut s = ui_state.write().await;
            s.on_malformed();
            continue;
        }

        let row = SnapshotRow {
            ts: Utc::now(),
            condition_id,
            token_id: asset_id,
            best_bid,
            best_ask,
            mid_price: sane_opt(m.mid_price),
            spread: sane_opt(m.spread),
            bid_depth_1: sane_opt(m.bid_depth_1),
            ask_depth_1: sane_opt(m.ask_depth_1),
            bid_depth_5: sane_opt(m.bid_depth_5),
            ask_depth_5: sane_opt(m.ask_depth_5),
            bid_depth_10: sane_opt(m.bid_depth_10),
            ask_depth_10: sane_opt(m.ask_depth_10),
            depth_imbalance_5: sane_opt(m.depth_imbalance_5),
            depth_imbalance_10: sane_opt(m.depth_imbalance_10),
            slippage_100: sane_opt(m.slippage_100),
            slippage_1000: sane_opt(m.slippage_1000),
            total_bid_levels: m.total_bid_levels,
            total_ask_levels: m.total_ask_levels,
            book_timestamp: item.get("timestamp").and_then(|x| x.as_i64()),
            source: "live_rust".to_string(),
            last_trade_price: item
                .get("last_trade_price")
                .and_then(|x| x.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .filter(|p| sane_price(*p)),
            last_trade_size: None,
            bid_levels: bid_levels.iter().map(|l| DepthLevel { price: l.price, size: l.size }).collect(),
            ask_levels: ask_levels.iter().map(|l| DepthLevel { price: l.price, size: l.size }).collect(),
        };

        {
            let mut s = ui_state.write().await;
            s.update_from_row(&row);
            if let Some(ts) = s.tokens.get_mut(&row.token_id) {
                ts.timeframe = timeframe.clone();
            }
        }

        if should_persist_market_now(&timeframe, &close_time, Utc::now()) {
            sampler.ingest(row);
        } else {
            let mut s = ui_state.write().await;
            s.on_skipped_non_current();
        }
    }

    Ok(())
}
