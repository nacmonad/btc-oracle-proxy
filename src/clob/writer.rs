use chrono::{DateTime, SecondsFormat, Utc};
use duckdb::{params, Connection};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

use crate::clob::state::ClobUiState;

#[derive(Debug, Clone)]
pub struct DepthLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone)]
pub struct SnapshotRow {
    pub ts: DateTime<Utc>,
    pub condition_id: String,
    pub token_id: String,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub mid_price: Option<f64>,
    pub spread: Option<f64>,
    pub bid_depth_1: Option<f64>,
    pub ask_depth_1: Option<f64>,
    pub bid_depth_5: Option<f64>,
    pub ask_depth_5: Option<f64>,
    pub bid_depth_10: Option<f64>,
    pub ask_depth_10: Option<f64>,
    pub depth_imbalance_5: Option<f64>,
    pub depth_imbalance_10: Option<f64>,
    pub slippage_100: Option<f64>,
    pub slippage_1000: Option<f64>,
    pub total_bid_levels: i32,
    pub total_ask_levels: i32,
    pub book_timestamp: Option<i64>,
    pub source: String,
    pub last_trade_price: Option<f64>,
    pub last_trade_size: Option<f64>,

    // Full L2 snapshot at this timestamp (for pm_order_book_levels)
    pub bid_levels: Vec<DepthLevel>,
    pub ask_levels: Vec<DepthLevel>,
}

#[derive(Clone)]
pub struct ClobWriter {
    tx: mpsc::Sender<SnapshotRow>,
}

impl ClobWriter {
    pub fn new(queue_size: usize) -> (Self, mpsc::Receiver<SnapshotRow>) {
        let (tx, rx) = mpsc::channel(queue_size);
        (Self { tx }, rx)
    }

    pub async fn enqueue(&self, row: SnapshotRow) {
        if let Err(e) = self.tx.send(row).await {
            warn!(error=%e, "clob writer queue send failed");
        }
    }

    pub fn try_enqueue(&self, row: SnapshotRow) -> bool {
        self.tx.try_send(row).is_ok()
    }
}

fn open_db() -> anyhow::Result<Connection> {
    let db_path = std::env::var("RESEARCHER_DB_PATH")
        .or_else(|_| std::env::var("DB_PATH"))
        .unwrap_or_else(|_| "../data/researcher.db".to_string());

    if let Some(parent) = Path::new(&db_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let conn = Connection::open(&db_path)?;
    conn.execute("SET threads TO 4", [])?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS pm_snapshots (
            ts                   TIMESTAMPTZ NOT NULL,
            condition_id         VARCHAR     NOT NULL,
            token_id             VARCHAR     NOT NULL,
            best_bid             DOUBLE,
            best_ask             DOUBLE,
            mid_price            DOUBLE,
            spread               DOUBLE,
            bid_depth_1          DOUBLE,
            ask_depth_1          DOUBLE,
            bid_depth_5          DOUBLE,
            ask_depth_5          DOUBLE,
            bid_depth_10         DOUBLE,
            ask_depth_10         DOUBLE,
            depth_imbalance_5    DOUBLE,
            depth_imbalance_10   DOUBLE,
            slippage_100         DOUBLE,
            slippage_1000        DOUBLE,
            total_bid_levels     INTEGER,
            total_ask_levels     INTEGER,
            book_timestamp       BIGINT,
            source               VARCHAR,
            last_trade_price     DOUBLE,
            last_trade_size      DOUBLE,
            PRIMARY KEY (ts, token_id)
        );

        CREATE TABLE IF NOT EXISTS pm_order_book_levels (
            ts                   TIMESTAMPTZ NOT NULL,
            condition_id         VARCHAR     NOT NULL,
            token_id             VARCHAR     NOT NULL,
            side                 VARCHAR     NOT NULL,
            level                INTEGER     NOT NULL,
            price                DOUBLE      NOT NULL,
            size                 DOUBLE      NOT NULL,
            cumulative_size      DOUBLE,
            source               VARCHAR,
            PRIMARY KEY (ts, token_id, side, level)
        );

        CREATE INDEX IF NOT EXISTS idx_pm_snapshots_cond ON pm_snapshots (condition_id, ts);
        CREATE INDEX IF NOT EXISTS idx_ob_levels_token_ts ON pm_order_book_levels (token_id, ts);
        "#,
    )?;
    info!(db_path=%db_path, "clob writer connected to duckdb");
    Ok(conn)
}

fn ts_sql(ts: &DateTime<Utc>) -> String {
    // DuckDB TIMESTAMPTZ parsing is most reliable with microsecond precision.
    ts.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn flush_batch(conn: &mut Connection, rows: &mut Vec<SnapshotRow>) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let tx = conn.transaction()?;
    {
        let mut snap_stmt = tx.prepare(
            "
            INSERT INTO pm_snapshots (
                ts, condition_id, token_id,
                best_bid, best_ask, mid_price, spread,
                bid_depth_1, ask_depth_1,
                bid_depth_5, ask_depth_5,
                bid_depth_10, ask_depth_10,
                depth_imbalance_5, depth_imbalance_10,
                slippage_100, slippage_1000,
                total_bid_levels, total_ask_levels,
                book_timestamp, source,
                last_trade_price, last_trade_size
            ) VALUES (
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?
            )
            "
        )?;

        let mut lvl_stmt = tx.prepare(
            "
            INSERT INTO pm_order_book_levels (
                ts, condition_id, token_id, side, level, price, size, cumulative_size, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            "
        )?;

        for r in rows.iter() {
            snap_stmt.execute(params![
                ts_sql(&r.ts),
                r.condition_id,
                r.token_id,
                r.best_bid,
                r.best_ask,
                r.mid_price,
                r.spread,
                r.bid_depth_1,
                r.ask_depth_1,
                r.bid_depth_5,
                r.ask_depth_5,
                r.bid_depth_10,
                r.ask_depth_10,
                r.depth_imbalance_5,
                r.depth_imbalance_10,
                r.slippage_100,
                r.slippage_1000,
                r.total_bid_levels,
                r.total_ask_levels,
                r.book_timestamp,
                r.source,
                r.last_trade_price,
                r.last_trade_size,
            ])?;

            // Full depth archival (BID side)
            let mut cumulative = 0.0f64;
            for (idx, lvl) in r.bid_levels.iter().enumerate() {
                cumulative += lvl.size;
                lvl_stmt.execute(params![
                    ts_sql(&r.ts),
                    r.condition_id,
                    r.token_id,
                    "BID",
                    (idx as i32) + 1,
                    lvl.price,
                    lvl.size,
                    cumulative,
                    r.source,
                ])?;
            }

            // Full depth archival (ASK side)
            cumulative = 0.0;
            for (idx, lvl) in r.ask_levels.iter().enumerate() {
                cumulative += lvl.size;
                lvl_stmt.execute(params![
                    ts_sql(&r.ts),
                    r.condition_id,
                    r.token_id,
                    "ASK",
                    (idx as i32) + 1,
                    lvl.price,
                    lvl.size,
                    cumulative,
                    r.source,
                ])?;
            }
        }
    }
    tx.commit()?;

    info!(rows=rows.len(), "clob writer flush complete");
    rows.clear();
    Ok(())
}

pub async fn run_writer_loop(mut rx: mpsc::Receiver<SnapshotRow>, flush_every_ms: u64, ui_state: Arc<RwLock<ClobUiState>>) {
    let mut conn = match open_db() {
        Ok(c) => c,
        Err(e) => {
            error!(error=%e, "failed to open duckdb writer connection");
            return;
        }
    };

    let mut buf: Vec<SnapshotRow> = Vec::with_capacity(2048);
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(flush_every_ms));
    let mut stats_tick = tokio::time::interval(std::time::Duration::from_secs(10));
    let mut received_rows: u64 = 0;
    let mut flush_error_count: u64 = 0;
    let mut recovered_drop_count: u64 = 0;

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(row) => {
                        received_rows += 1;
                        buf.push(row);
                        if buf.len() >= 1000 {
                            if let Err(e) = flush_batch(&mut conn, &mut buf) {
                                flush_error_count += 1;
                                error!(buffered_rows=buf.len(), flush_error_count, "clob writer batch flush failed: {:#}", e);
                                if let Some(dropped) = (!buf.is_empty()).then(|| buf.remove(0)) {
                                    recovered_drop_count += 1;
                                    error!(token_id=%dropped.token_id, condition_id=%dropped.condition_id, ts=%dropped.ts.to_rfc3339(), recovered_drop_count, "clob writer dropped one buffered row to recover from persistent flush failure");
                                }
                            }
                        }
                    }
                    None => {
                        if let Err(e) = flush_batch(&mut conn, &mut buf) {
                            error!(buffered_rows=buf.len(), "clob writer shutdown flush failed: {:#}", e);
                        }
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                if let Err(e) = flush_batch(&mut conn, &mut buf) {
                    flush_error_count += 1;
                    error!(buffered_rows=buf.len(), flush_error_count, "clob writer periodic flush failed: {:#}", e);
                    if let Some(dropped) = (!buf.is_empty()).then(|| buf.remove(0)) {
                        recovered_drop_count += 1;
                        error!(token_id=%dropped.token_id, condition_id=%dropped.condition_id, ts=%dropped.ts.to_rfc3339(), recovered_drop_count, "clob writer dropped one buffered row to recover from persistent periodic flush failure");
                    }
                }
            }
            _ = stats_tick.tick() => {
                {
                    let mut s = ui_state.write().await;
                    s.set_writer_stats(received_rows, buf.len() as u64, flush_error_count, recovered_drop_count);
                }
                info!(
                    writer_received_rows=received_rows,
                    writer_buffered_rows=buf.len(),
                    writer_flush_errors=flush_error_count,
                    writer_recovered_drops=recovered_drop_count,
                    "clob writer stats"
                );
            }
        }
    }
}
