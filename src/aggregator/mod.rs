//! Price aggregation from multiple exchange sources + Chainlink deviation detection.

pub mod calculator;
pub mod chainlink;
pub mod exchange_client;

use crate::config::Config;
use crate::error::OracleResult;
use crate::indicators::{self, IndicatorConfig};
use crate::clob::state::ClobUiState;
use crate::clob::writer::ClobWriter;
use crate::models::{
    AppState, BbBreakoutEvent, DeviationApproachEvent, ExchangePrice, ExchangeStatus,
    MarketContext, PreTriggerAlertEvent, PriceUpdate, RoundDirection, RoundSettledEvent,
    RoundTriggeredEvent, WsEvent,
};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time::Duration;
use tracing::{info, warn};

/// Spawns exchange + Chainlink tasks and drives the aggregation loop.
///
/// Every `config.aggregation_interval_ms`:
/// 1. Takes the median of fresh exchange prices
/// 2. Reads the latest Chainlink baseline from state
/// 3. Computes deviation % and `round_imminent`
/// 4. Calculates technical indicators
/// 5. Writes a fully enriched `PriceUpdate` to state
pub async fn run_aggregator(
    state: Arc<RwLock<AppState>>,
    clob_ui_state: Arc<RwLock<ClobUiState>>,
    config: Config,
    event_tx: broadcast::Sender<WsEvent>,
    db_writer: ClobWriter,
) -> OracleResult<()> {
    info!("Starting price aggregator...");

    // ── Exchange price channel ──────────────────────────────────────────────
    let (tx, mut rx) = mpsc::channel::<ExchangePrice>(1000);

    let tx_b = tx.clone();
    tokio::spawn(async move { exchange_client::BinanceClient::run(tx_b).await });

    let tx_c = tx.clone();
    tokio::spawn(async move { exchange_client::CoinbaseClient::run(tx_c).await });

    let tx_k = tx.clone();
    tokio::spawn(async move { exchange_client::KrakenClient::run(tx_k).await });

    drop(tx);

    // ── Chainlink baseline task ─────────────────────────────────────────────
    let chainlink_client = chainlink::ChainlinkClient::new(
        config.polygon_rpc_url.clone(),
        config.chainlink_poll_secs,
        state.clone(),
    );
    tokio::spawn(async move { chainlink_client.run().await });

    // ── Aggregation loop ────────────────────────────────────────────────────
    let bars_per_min = (60_000u64 / config.aggregation_interval_ms.max(50)).max(1) as usize;
    let indicator_cfg = IndicatorConfig {
        ema_short_period: config.ema_short_period,
        ema_long_period: config.ema_long_period,
        rsi_period: config.rsi_period,
        volatility_period: config.volatility_period,
        mro_5_bars: 5 * bars_per_min,
        mro_10_bars: 10 * bars_per_min,
        mro_15_bars: 15 * bars_per_min,
        mro_rank_window: (30 * bars_per_min).max(120),
    };
    let capacity = config.price_history_capacity;

    let mut price_buffer: HashMap<String, ExchangePrice> = HashMap::new();
    let mut interval =
        tokio::time::interval(Duration::from_millis(config.aggregation_interval_ms));
    let mut update_count: u64 = 0;

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Some(ep) => {
                        // Update exchange status — DashMap handles its own locking,
                        // so a read lock on AppState is sufficient here.
                        {
                            let s = state.read().await;
                            s.exchange_status.insert(ep.exchange.clone(), ExchangeStatus {
                                exchange: ep.exchange.clone(),
                                connected: true,
                                last_price: Some(ep.price),
                                last_update: Some(ep.timestamp),
                                error_count: 0,
                                last_error: None,
                            });
                        }
                        price_buffer.insert(ep.exchange.clone(), ep);
                    }
                    None => {
                        warn!("All exchange senders dropped — aggregator shutting down");
                        break;
                    }
                }
            }

            _ = interval.tick() => {
                let now = Utc::now();

                // Only include prices fresh within the last 30 seconds
                let fresh: Vec<ExchangePrice> = price_buffer
                    .values()
                    .filter(|p| now.signed_duration_since(p.timestamp).num_seconds() <= 30)
                    .cloned()
                    .collect();

                if fresh.is_empty() {
                    continue;
                }

                let market_price = match calculator::calculate_aggregated_price(fresh.clone()) {
                    Ok(p) => p,
                    Err(e) => { warn!("Aggregation error: {e}"); continue; }
                };

                let mut s = state.write().await;

                // Chainlink deviation
                let (chainlink_price, chainlink_age_secs, deviation_pct, round_imminent) =
                    if let Some(ref baseline) = s.chainlink_baseline {
                        let dev = calculator::deviation_pct(market_price, baseline.price);
                        let imminent = calculator::is_round_imminent(dev);
                        (
                            Some(baseline.price),
                            Some(baseline.age_secs()),
                            Some(dev),
                            imminent,
                        )
                    } else {
                        (None, None, None, false)
                    };

                // Price history + indicators
                s.price_history.push(market_price);
                if s.price_history.len() > capacity {
                    s.price_history.remove(0);
                }

                let indicators = indicators::calculate_all_indicators(
                    &s.price_history,
                    &indicator_cfg,
                );

                let exchange_prices: HashMap<String, f64> =
                    fresh.iter().map(|p| (p.exchange.clone(), p.price)).collect();
                let exchange_latency_ms: HashMap<String, i64> = s
                    .exchange_status
                    .iter()
                    .filter_map(|kv| {
                        kv.last_update.map(|lu| {
                            let age = now.signed_duration_since(lu).num_milliseconds().max(0);
                            (kv.exchange.clone(), age)
                        })
                    })
                    .collect();

                let (market_context, market_context_5m) = {
                    let clob = clob_ui_state.read().await;
                    let to_ctx = |rq: crate::clob::state::RoundQuote| {
                        let age_up = now.signed_duration_since(rq.up.updated_at).num_milliseconds();
                        let age_down = now.signed_duration_since(rq.down.updated_at).num_milliseconds();
                        let book_age_ms = Some(age_up.max(age_down).max(0));
                        MarketContext {
                            condition_id: rq.condition_id,
                            token_yes_id: rq.up.token_id,
                            token_no_id: rq.down.token_id,
                            up_bid: rq.up.best_bid,
                            up_ask: rq.up.best_ask,
                            down_bid: rq.down.best_bid,
                            down_ask: rq.down.best_ask,
                            up_spread: rq.up.spread,
                            down_spread: rq.down.spread,
                            round_close_ts: rq.close_time,
                            book_age_ms,
                        }
                    };
                    (
                        clob.round_quote_at("BTC", "15m", now, 3600, 45).map(to_ctx),
                        clob.round_quote_at("BTC", "5m", now, 3600, 45).map(to_ctx),
                    )
                };

                let condition_id = market_context.as_ref().map(|m| m.condition_id.clone());
                let token_yes_id = market_context.as_ref().map(|m| m.token_yes_id.clone());
                let token_no_id = market_context.as_ref().map(|m| m.token_no_id.clone());
                let up_bid = market_context.as_ref().and_then(|m| m.up_bid);
                let up_ask = market_context.as_ref().and_then(|m| m.up_ask);
                let down_bid = market_context.as_ref().and_then(|m| m.down_bid);
                let down_ask = market_context.as_ref().and_then(|m| m.down_ask);
                let round_close_ts = market_context.as_ref().and_then(|m| m.round_close_ts.clone());
                let book_age_ms = market_context.as_ref().and_then(|m| m.book_age_ms);

                let condition_id_5m = market_context_5m.as_ref().map(|m| m.condition_id.clone());
                let up_bid_5m = market_context_5m.as_ref().and_then(|m| m.up_bid);
                let up_ask_5m = market_context_5m.as_ref().and_then(|m| m.up_ask);
                let down_bid_5m = market_context_5m.as_ref().and_then(|m| m.down_bid);
                let down_ask_5m = market_context_5m.as_ref().and_then(|m| m.down_ask);

                let active_lmsr = s
                    .lmsr_params
                    .values()
                    .filter(|p| p.effective_from <= now)
                    .max_by_key(|p| p.effective_from)
                    .cloned();

                let market_probability = if let (Some(b), Some(a)) = (up_bid, up_ask) {
                    let m = (b + a) * 0.5;
                    if m.is_finite() && m > 0.0 && m < 1.0 { Some(m) } else { None }
                } else if let (Some(b), Some(a)) = (down_bid, down_ask) {
                    let m = 1.0 - ((b + a) * 0.5);
                    if m.is_finite() && m > 0.0 && m < 1.0 { Some(m) } else { None }
                } else {
                    None
                };

                let market_probability_5m = if let (Some(b), Some(a)) = (up_bid_5m, up_ask_5m) {
                    let m = (b + a) * 0.5;
                    if m.is_finite() && m > 0.0 && m < 1.0 { Some(m) } else { None }
                } else if let (Some(b), Some(a)) = (down_bid_5m, down_ask_5m) {
                    let m = 1.0 - ((b + a) * 0.5);
                    if m.is_finite() && m > 0.0 && m < 1.0 { Some(m) } else { None }
                } else {
                    None
                };

                let expected_model = s.expected_p_model.clone().filter(|m| m.enabled);
                let p_expected = if let (Some(mdl), Some(p_mkt)) = (expected_model.as_ref(), market_probability) {
                    let tau_sec = round_close_ts
                        .as_ref()
                        .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
                        .map(|dt| dt.with_timezone(&chrono::Utc).signed_duration_since(now).num_seconds().max(0) as f64)
                        .unwrap_or(0.0);
                    let mut feat: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
                    feat.insert("tau_sec".to_string(), tau_sec);
                    feat.insert("p_market".to_string(), p_mkt);
                    feat.insert("deviation_pct".to_string(), deviation_pct.unwrap_or(0.0));
                    feat.insert("chainlink_age_secs".to_string(), chainlink_age_secs.unwrap_or(0) as f64);
                    feat.insert("up_bid".to_string(), up_bid.unwrap_or(0.0));
                    feat.insert("up_ask".to_string(), up_ask.unwrap_or(0.0));
                    feat.insert("down_bid".to_string(), down_bid.unwrap_or(0.0));
                    feat.insert("down_ask".to_string(), down_ask.unwrap_or(0.0));
                    feat.insert("up_spread".to_string(), market_context.as_ref().and_then(|m| m.up_spread).unwrap_or(0.0));
                    feat.insert("down_spread".to_string(), market_context.as_ref().and_then(|m| m.down_spread).unwrap_or(0.0));
                    let mut z = mdl.intercept;
                    for (k, w) in mdl.coefs.iter() {
                        let x = *feat.get(k).unwrap_or(&0.0);
                        let mu = *mdl.means.get(k).unwrap_or(&0.0);
                        let sd = mdl.stds.get(k).copied().unwrap_or(1.0).abs().max(1e-9);
                        z += w * ((x - mu) / sd);
                    }
                    Some(1.0 / (1.0 + (-z).exp()))
                } else {
                    None
                };

                let (p_lmsr, delta_lmsr, delta_lmsr_z, lmsr_version, alpha_live, b_live, p_lmsr_5m, delta_lmsr_5m, delta_lmsr_z_5m) =
                    if let (Some(p_e), Some(p_mkt)) = (p_expected, market_probability) {
                        let d = p_e - p_mkt;
                        (Some(p_e), Some(d), Some(d / 0.01_f64.max(1e-6)), expected_model.as_ref().map(|m| format!("expected-p:{}", m.version)), None, None, None, None, None)
                    } else if let Some(prm) = active_lmsr {
                        let calc = |p_mkt: Option<f64>| {
                            p_mkt.map(|p| {
                                let logit_m = (p / (1.0 - p)).ln();
                                let p_l = 1.0 / (1.0 + (-(prm.alpha * logit_m)).exp());
                                let d = p_l - p;
                                (p_l, d, d / 0.01_f64.max(1e-6))
                            })
                        };

                        let main = calc(market_probability);
                        let m5 = calc(market_probability_5m);

                        (
                            main.map(|x| x.0),
                            main.map(|x| x.1),
                            main.map(|x| x.2),
                            Some(prm.version),
                            Some(prm.alpha),
                            Some(prm.b),
                            m5.map(|x| x.0),
                            m5.map(|x| x.1),
                            m5.map(|x| x.2),
                        )
                    } else {
                        (None, None, None, None, None, None, None, None, None)
                    };
                s.current_price = Some(PriceUpdate {
                    timestamp: now,
                    symbol: "BTC/USD".to_string(),
                    market_context,
                    condition_id: condition_id.clone(),
                    token_yes_id,
                    token_no_id,
                    up_bid,
                    up_ask,
                    down_bid,
                    down_ask,
                    round_close_ts: round_close_ts.clone(),
                    book_age_ms,
                    market_context_5m,
                    condition_id_5m,
                    up_bid_5m,
                    up_ask_5m,
                    down_bid_5m,
                    down_ask_5m,
                    market_price,
                    chainlink_price,
                    chainlink_age_secs,
                    deviation_pct,
                    round_imminent,
                    p_lmsr,
                    delta_lmsr,
                    delta_lmsr_z,
                    lmsr_version,
                    alpha_live,
                    b_live,
                    p_expected,
                    p_lmsr_5m,
                    delta_lmsr_5m,
                    delta_lmsr_z_5m,
                    exchange_prices: exchange_prices.clone(),
                    exchange_latency_ms: if exchange_latency_ms.is_empty() { None } else { Some(exchange_latency_ms.clone()) },
                    indicators: indicators.clone(),
                });
                s.last_update = Some(now);

                // ── Event detection (rising-edge signals) ───────────────────
                // Collect events locally, then release the write lock before
                // broadcasting so we don't hold state locked during channel sends.

                let mut outbound: Vec<WsEvent> = Vec::new();

                // Tick — always emitted
                outbound.push(WsEvent::Tick {
                    ts: now,
                    data: s.current_price.clone().unwrap(),
                });

                // 1. round_settled — chainlink baseline changed since last tick
                if let (Some(prev_cl), Some(curr_cl)) = (s.prev_chainlink_price, chainlink_price) {
                    if (curr_cl - prev_cl).abs() > 0.001 {
                        let delta = curr_cl - prev_cl;
                        let delta_pct = (delta / prev_cl) * 100.0;
                        let duration = s.round_imminent_since.map(|t| {
                            now.signed_duration_since(t).num_seconds().max(0) as u64
                        });
                        info!(
                            "🔗 CHAINLINK SETTLED: ${prev_cl:.2} → ${curr_cl:.2}  \
                             delta={delta_pct:+.3}%  window={}s",
                            duration.unwrap_or(0)
                        );
                        outbound.push(WsEvent::RoundSettled {
                            ts: now,
                            data: RoundSettledEvent {
                                prev_price: prev_cl,
                                new_price: curr_cl,
                                price_delta: delta,
                                delta_pct,
                                round_duration_secs: duration,
                            },
                        });
                        s.round_imminent_since = None;
                    }
                }
                s.prev_chainlink_price = chainlink_price;

                // 2. round_triggered — rising edge of round_imminent
                if round_imminent && !s.prev_round_imminent {
                    if let (Some(dev), Some(cl), Some(age)) =
                        (deviation_pct, chainlink_price, chainlink_age_secs)
                    {
                        let direction = if dev > 0.0 { RoundDirection::Up } else { RoundDirection::Down };
                        let dir_str = if dev > 0.0 { "UP" } else { "DOWN" };
                        let close_ts_dbg = round_close_ts.clone();
                        let slug_dbg = close_ts_dbg.as_ref().and_then(|s| {
                            chrono::DateTime::parse_from_rfc3339(s)
                                .ok()
                                .map(|dt| format!("btc-updown-15m-{}", dt.timestamp() - 900))
                        });
                        let url_dbg = slug_dbg.as_ref().map(|slug| format!("https://polymarket.com/event/{slug}"));
                        info!(
                            "⚡ ROUND TRIGGERED {dir_str}: market=${market_price:.2}  \
                             chainlink=${cl:.2}  deviation={dev:+.3}%  age={age}s  \
                             map(condition_id={:?} close_ts={:?} slug={:?} url={:?})",
                            condition_id,
                            close_ts_dbg,
                            slug_dbg,
                            url_dbg,
                        );
                        outbound.push(WsEvent::RoundTriggered {
                            ts: now,
                            data: RoundTriggeredEvent {
                                direction,
                                market_price,
                                chainlink_price: cl,
                                chainlink_age_secs: age,
                                deviation_pct: dev,
                                exchange_prices: exchange_prices.clone(),
                            },
                        });
                        s.round_imminent_since = Some(now);
                    }
                }
                s.prev_round_imminent = round_imminent;

                // 3. deviation_approach — rising edge into 0.07–0.10% zone
                let dev_approaching = deviation_pct
                    .map(calculator::is_deviation_approaching)
                    .unwrap_or(false);
                if dev_approaching && !s.prev_deviation_approaching {
                    if let (Some(dev), Some(cl), Some(age)) =
                        (deviation_pct, chainlink_price, chainlink_age_secs)
                    {
                        let direction = if dev > 0.0 { RoundDirection::Up } else { RoundDirection::Down };
                        let dir_str = if dev > 0.0 { "UP" } else { "DOWN" };
                        info!(
                            "〰 DEVIATION APPROACHING {dir_str}: {dev:+.3}%  \
                             market=${market_price:.2}  chainlink=${cl:.2}"
                        );
                        outbound.push(WsEvent::DeviationApproach {
                            ts: now,
                            data: DeviationApproachEvent {
                                direction,
                                deviation_pct: dev,
                                market_price,
                                chainlink_price: cl,
                                chainlink_age_secs: age,
                            },
                        });
                    }
                }
                s.prev_deviation_approaching = dev_approaching;

                // 4. bb_breakout — rising edge of price exiting Bollinger Bands
                let curr_bb_width = match (indicators.bb_upper, indicators.bb_lower, indicators.bb_middle) {
                    (Some(u), Some(l), Some(m)) => Some(calculator::bb_width_pct(u, l, m)),
                    _ => None,
                };
                let curr_bb_dir = calculator::bb_breakout_direction(
                    market_price,
                    indicators.bb_upper,
                    indicators.bb_lower,
                );
                if curr_bb_dir.is_some() && s.prev_bb_breakout_dir.is_none() {
                    if let (Some(dir), Some(upper), Some(lower), Some(width)) = (
                        curr_bb_dir.clone(),
                        indicators.bb_upper,
                        indicators.bb_lower,
                        curr_bb_width,
                    ) {
                        let expanding = s.prev_bb_width_pct.map(|pw| width > pw).unwrap_or(false);
                        let dir_str = if matches!(dir, RoundDirection::Up) { "UP" } else { "DOWN" };
                        info!(
                            "📈 BB BREAKOUT {dir_str}: market=${market_price:.2}  \
                             width={width:.3}%  expanding={expanding}  \
                             deviation={:.3}%",
                            deviation_pct.unwrap_or(0.0)
                        );
                        outbound.push(WsEvent::BbBreakout {
                            ts: now,
                            data: BbBreakoutEvent {
                                direction: dir,
                                market_price,
                                bb_upper: upper,
                                bb_lower: lower,
                                bb_width_pct: width,
                                bb_expanding: expanding,
                                deviation_pct,
                            },
                        });
                    }
                }
                s.prev_bb_breakout_dir = curr_bb_dir;
                s.prev_bb_width_pct = curr_bb_width;

                // 5. pre_trigger_alert — multi-signal convergence (rising edge)
                let pre_trigger_result = calculator::detect_pre_trigger_signals(
                    market_price,
                    deviation_pct,
                    indicators.bb_upper,
                    indicators.bb_lower,
                    indicators.momentum_10,
                    indicators.rsi_14,
                );
                let pre_trigger_active = pre_trigger_result.is_some();
                if pre_trigger_active && !s.prev_pre_trigger {
                    if let (Some((direction, signals)), Some(cl), Some(age)) =
                        (pre_trigger_result, chainlink_price, chainlink_age_secs)
                    {
                        let dev = deviation_pct.unwrap_or(0.0);
                        let dir_str = if matches!(direction, RoundDirection::Up) { "UP" } else { "DOWN" };
                        let sig_str = signals.join(", ");
                        let close_ts_dbg = round_close_ts.clone();
                        let slug_dbg = close_ts_dbg.as_ref().and_then(|s| {
                            chrono::DateTime::parse_from_rfc3339(s)
                                .ok()
                                .map(|dt| format!("btc-updown-15m-{}", dt.timestamp() - 900))
                        });
                        let url_dbg = slug_dbg.as_ref().map(|slug| format!("https://polymarket.com/event/{slug}"));
                        info!(
                            "🎯 PRE-TRIGGER ALERT {dir_str}: dev={dev:+.3}%  \
                             market=${market_price:.2}  signals=[{sig_str}]  \
                             map(condition_id={:?} close_ts={:?} slug={:?} url={:?})",
                            condition_id,
                            close_ts_dbg,
                            slug_dbg,
                            url_dbg,
                        );
                        outbound.push(WsEvent::PreTriggerAlert {
                            ts: now,
                            data: PreTriggerAlertEvent {
                                direction,
                                signals,
                                deviation_pct: dev,
                                market_price,
                                chainlink_price: cl,
                                chainlink_age_secs: age,
                                bb_width_pct: curr_bb_width,
                                rsi_14: indicators.rsi_14,
                                momentum_10: indicators.momentum_10,
                                mro_5: indicators.mro_5,
                                mro_10: indicators.mro_10,
                                mro_15: indicators.mro_15,
                            },
                        });
                    }
                }
                s.prev_pre_trigger = pre_trigger_active;

                // Release write lock before broadcasting
                drop(s);

                for evt in outbound {
                    // Ignore SendError (no subscribers yet is fine)
                    let _ = event_tx.send(evt.clone());
                    if !db_writer.try_enqueue_oracle_event(evt) {
                        warn!("db writer queue full; dropping oracle ws event");
                    }
                }

                // ── Periodic status log (every ~30s) ───────────────────────
                let history_len = {
                    let s = state.read().await;
                    s.price_history.len()
                };
                update_count += 1;
                if update_count % 60 == 1 {
                    match (deviation_pct, chainlink_price) {
                        (Some(dev), Some(cl)) => info!(
                            "BTC/USD market=${market_price:.2}  chainlink=${cl:.2}  \
                             deviation={dev:+.3}%{}  sources={}  history={}pts",
                            if round_imminent { "  ⚡ROUND IMMINENT" } else { "" },
                            fresh.len(),
                            history_len
                        ),
                        _ => info!(
                            "BTC/USD ${market_price:.2}  (awaiting Chainlink baseline)  \
                             sources={}  history={}pts",
                            fresh.len(),
                            history_len
                        ),
                    }
                }
            }
        }
    }

    Ok(())
}
