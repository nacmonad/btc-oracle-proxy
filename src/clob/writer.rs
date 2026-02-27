use chrono::{DateTime, Utc};
use duckdb::{params, Connection};
use std::path::Path;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

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
    info!(db_path=%db_path, "clob writer connected to duckdb");
    Ok(conn)
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
                r.ts.to_rfc3339(),
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
                    r.ts.to_rfc3339(),
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
                    r.ts.to_rfc3339(),
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

pub async fn run_writer_loop(mut rx: mpsc::Receiver<SnapshotRow>, flush_every_ms: u64) {
    let mut conn = match open_db() {
        Ok(c) => c,
        Err(e) => {
            error!(error=%e, "failed to open duckdb writer connection");
            return;
        }
    };

    let mut buf: Vec<SnapshotRow> = Vec::with_capacity(2048);
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(flush_every_ms));

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(row) => {
                        buf.push(row);
                        if buf.len() >= 1000 {
                            if let Err(e) = flush_batch(&mut conn, &mut buf) {
                                error!(error=%e, "clob writer batch flush failed");
                            }
                        }
                    }
                    None => {
                        if let Err(e) = flush_batch(&mut conn, &mut buf) {
                            error!(error=%e, "clob writer shutdown flush failed");
                        }
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                if let Err(e) = flush_batch(&mut conn, &mut buf) {
                    error!(error=%e, "clob writer periodic flush failed");
                }
            }
        }
    }
}
