use std::env;

use duckdb::{params, Connection};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::models::{RoundDirection, WsEvent};

#[derive(Clone)]
pub struct OracleDbWriter {
    tx: mpsc::Sender<WsEvent>,
}

impl OracleDbWriter {
    pub fn new(queue_size: usize) -> (Self, mpsc::Receiver<WsEvent>) {
        let (tx, rx) = mpsc::channel(queue_size);
        (Self { tx }, rx)
    }

    pub fn try_enqueue(&self, evt: WsEvent) -> bool {
        self.tx.try_send(evt).is_ok()
    }
}

fn dir_to_str(d: &RoundDirection) -> &'static str {
    match d {
        RoundDirection::Up => "UP",
        RoundDirection::Down => "DOWN",
    }
}

fn open_db() -> anyhow::Result<Connection> {
    let db_path = env::var("DB_PATH").unwrap_or_else(|_| "../data/researcher.db".to_string());
    let conn = Connection::open(&db_path)?;
    info!(db_path=%db_path, "oracle db writer connected to duckdb");
    Ok(conn)
}

fn flush_batch(conn: &mut Connection, rows: &mut Vec<WsEvent>) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let tx = conn.transaction()?;
    {
        let mut tick_stmt = tx.prepare(
            r#"
            INSERT OR IGNORE INTO oracle_ticks
                (ts, symbol, market_price, chainlink_price, chainlink_age_secs,
                 deviation_pct, round_imminent,
                 price_binance, price_coinbase, price_kraken,
                 ema_12, ema_26, ema_50, rsi_14,
                 momentum_10, momentum_20, volatility,
                 bb_upper, bb_middle, bb_lower,
                 macd, macd_signal, macd_histogram)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            "#,
        )?;

        let mut sig_stmt = tx.prepare(
            r#"
            INSERT INTO signal_events
                (ts, event_type, symbol, direction, deviation_pct,
                 market_price, chainlink_price, chainlink_age_secs,
                 prev_price, new_price, price_delta, delta_pct, round_duration_secs,
                 signals_json, bb_width_pct, rsi_14, momentum_10,
                 bb_upper, bb_lower, bb_expanding,
                 raw_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            "#,
        )?;

        for evt in rows.iter() {
            match evt {
                WsEvent::Tick { ts, data } => {
                    tick_stmt.execute(params![
                        ts.to_rfc3339(),
                        data.symbol,
                        data.market_price,
                        data.chainlink_price,
                        data.chainlink_age_secs.map(|v| v as i64),
                        data.deviation_pct,
                        data.round_imminent,
                        data.exchange_prices.get("binance").copied(),
                        data.exchange_prices.get("coinbase").copied(),
                        data.exchange_prices.get("kraken").copied(),
                        data.indicators.ema_12,
                        data.indicators.ema_26,
                        data.indicators.ema_50,
                        data.indicators.rsi_14,
                        data.indicators.momentum_10,
                        data.indicators.momentum_20,
                        data.indicators.volatility,
                        data.indicators.bb_upper,
                        data.indicators.bb_middle,
                        data.indicators.bb_lower,
                        data.indicators.macd,
                        data.indicators.macd_signal,
                        data.indicators.macd_histogram,
                    ])?;
                }
                WsEvent::PreTriggerAlert { ts, data } => {
                    let raw_json = serde_json::to_string(evt).ok();
                    sig_stmt.execute(params![
                        ts.to_rfc3339(),
                        "pre_trigger_alert",
                        "BTC/USD",
                        dir_to_str(&data.direction),
                        Some(data.deviation_pct),
                        Some(data.market_price),
                        Some(data.chainlink_price),
                        Some(data.chainlink_age_secs as i64),
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<i64>::None,
                        Some(serde_json::to_string(&data.signals).unwrap_or_else(|_| "[]".to_string())),
                        data.bb_width_pct,
                        data.rsi_14,
                        data.momentum_10,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<bool>::None,
                        raw_json,
                    ])?;
                }
                WsEvent::RoundTriggered { ts, data } => {
                    let raw_json = serde_json::to_string(evt).ok();
                    sig_stmt.execute(params![
                        ts.to_rfc3339(),
                        "round_triggered",
                        "BTC/USD",
                        dir_to_str(&data.direction),
                        Some(data.deviation_pct),
                        Some(data.market_price),
                        Some(data.chainlink_price),
                        Some(data.chainlink_age_secs as i64),
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<i64>::None,
                        Option::<String>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<bool>::None,
                        raw_json,
                    ])?;
                }
                WsEvent::RoundSettled { ts, data } => {
                    if data.round_duration_secs.is_none() {
                        continue;
                    }
                    let raw_json = serde_json::to_string(evt).ok();
                    sig_stmt.execute(params![
                        ts.to_rfc3339(),
                        "round_settled",
                        "BTC/USD",
                        Option::<String>::None,
                        Some(data.delta_pct),
                        Some(data.new_price),
                        Option::<f64>::None,
                        Option::<i64>::None,
                        Some(data.prev_price),
                        Some(data.new_price),
                        Some(data.price_delta),
                        Some(data.delta_pct),
                        data.round_duration_secs.map(|v| v as i64),
                        Option::<String>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<bool>::None,
                        raw_json,
                    ])?;
                }
                WsEvent::DeviationApproach { ts, data } => {
                    let raw_json = serde_json::to_string(evt).ok();
                    sig_stmt.execute(params![
                        ts.to_rfc3339(),
                        "deviation_approach",
                        "BTC/USD",
                        dir_to_str(&data.direction),
                        Some(data.deviation_pct),
                        Some(data.market_price),
                        Some(data.chainlink_price),
                        Some(data.chainlink_age_secs as i64),
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<i64>::None,
                        Option::<String>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<bool>::None,
                        raw_json,
                    ])?;
                }
                WsEvent::BbBreakout { ts, data } => {
                    let raw_json = serde_json::to_string(evt).ok();
                    sig_stmt.execute(params![
                        ts.to_rfc3339(),
                        "bb_breakout",
                        "BTC/USD",
                        dir_to_str(&data.direction),
                        data.deviation_pct,
                        Some(data.market_price),
                        Option::<f64>::None,
                        Option::<i64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Option::<i64>::None,
                        Option::<String>::None,
                        Some(data.bb_width_pct),
                        Option::<f64>::None,
                        Option::<f64>::None,
                        Some(data.bb_upper),
                        Some(data.bb_lower),
                        Some(data.bb_expanding),
                        raw_json,
                    ])?;
                }
                WsEvent::ExchangeStatus { .. } => {}
            }
        }
    }
    tx.commit()?;
    rows.clear();
    Ok(())
}

pub async fn run_writer_loop(mut rx: mpsc::Receiver<WsEvent>, flush_every_ms: u64) {
    let mut conn = match open_db() {
        Ok(c) => c,
        Err(e) => {
            error!(error=%e, "failed to open oracle db writer connection");
            return;
        }
    };

    let mut buf: Vec<WsEvent> = Vec::with_capacity(512);
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(flush_every_ms.max(100)));

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(evt) => {
                        buf.push(evt);
                        if buf.len() >= 256 {
                            if let Err(e) = flush_batch(&mut conn, &mut buf) {
                                error!(error=%e, "oracle writer batch flush failed");
                                if !buf.is_empty() { buf.remove(0); }
                            }
                        }
                    }
                    None => {
                        if let Err(e) = flush_batch(&mut conn, &mut buf) {
                            error!(error=%e, "oracle writer shutdown flush failed");
                        }
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                if let Err(e) = flush_batch(&mut conn, &mut buf) {
                    error!(error=%e, "oracle writer periodic flush failed");
                    if !buf.is_empty() { buf.remove(0); }
                }
            }
        }
    }

    warn!("oracle db writer loop exited");
}
