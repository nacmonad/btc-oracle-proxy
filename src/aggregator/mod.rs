//! Price aggregation from multiple exchange sources + Chainlink deviation detection.

pub mod calculator;
pub mod chainlink;
pub mod exchange_client;

use crate::config::Config;
use crate::error::OracleResult;
use crate::indicators::{self, IndicatorConfig};
use crate::models::{AppState, ExchangePrice, ExchangeStatus, PriceUpdate};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
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
pub async fn run_aggregator(state: Arc<RwLock<AppState>>, config: Config) -> OracleResult<()> {
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
        state.clone(),
    );
    tokio::spawn(async move { chainlink_client.run().await });

    // ── Aggregation loop ────────────────────────────────────────────────────
    let indicator_cfg = IndicatorConfig {
        ema_short_period: config.ema_short_period,
        ema_long_period: config.ema_long_period,
        rsi_period: config.rsi_period,
        volatility_period: config.volatility_period,
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

                s.current_price = Some(PriceUpdate {
                    timestamp: now,
                    symbol: "BTC/USD".to_string(),
                    market_price,
                    chainlink_price,
                    chainlink_age_secs,
                    deviation_pct,
                    round_imminent,
                    exchange_prices,
                    indicators,
                });
                s.last_update = Some(now);

                update_count += 1;
                // Log roughly every 30 seconds (60 × 500 ms intervals)
                if update_count % 60 == 1 {
                    match (deviation_pct, chainlink_price) {
                        (Some(dev), Some(cl)) => info!(
                            "BTC/USD market=${market_price:.2}  chainlink=${cl:.2}  \
                             deviation={dev:+.3}%{}  sources={}  history={}pts",
                            if round_imminent { "  ⚡ROUND IMMINENT" } else { "" },
                            fresh.len(),
                            s.price_history.len()
                        ),
                        _ => info!(
                            "BTC/USD ${market_price:.2}  (awaiting Chainlink baseline)  \
                             sources={}  history={}pts",
                            fresh.len(),
                            s.price_history.len()
                        ),
                    }
                }

                // Always log when a round becomes imminent (regardless of log throttle)
                if round_imminent {
                    if let (Some(dev), Some(cl)) = (deviation_pct, chainlink_price) {
                        let direction = if dev > 0.0 { "UP" } else { "DOWN" };
                        info!(
                            "⚡ CHAINLINK ROUND TRIGGERING {direction}: \
                             market=${market_price:.2}  chainlink=${cl:.2}  \
                             deviation={dev:+.3}%  age={}s",
                            chainlink_age_secs.unwrap_or(0)
                        );
                    }
                }
            }
        }
    }

    Ok(())
}
