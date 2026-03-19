use chrono::{DateTime, SecondsFormat, Utc};
use duckdb::{params, Connection};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

use crate::clob::state::ClobUiState;
use crate::models::{RoundDirection, WsEvent};

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

#[derive(Debug, Clone)]
pub struct MarketUpsertRow {
    pub condition_id: String,
    pub question: String,
    pub asset: String,
    pub timeframe: String,
    pub close_time: String,
    pub resolved_at: Option<String>,
    pub outcome: Option<String>,
    pub winning_price: Option<f64>,
    pub token_yes_id: String,
    pub token_no_id: String,
    pub pivot_price: Option<f64>,
}

#[derive(Debug, Clone)]
pub enum DbOp {
    Snapshot(SnapshotRow),
    OracleEvent(WsEvent),
    MarketUpserts(Vec<MarketUpsertRow>),
}

#[derive(Clone)]
pub struct ClobWriter {
    tx: mpsc::Sender<DbOp>,
}

impl ClobWriter {
    pub fn new(queue_size: usize) -> (Self, mpsc::Receiver<DbOp>) {
        let (tx, rx) = mpsc::channel(queue_size);
        (Self { tx }, rx)
    }

    pub fn try_enqueue_snapshot(&self, row: SnapshotRow) -> bool {
        self.tx.try_send(DbOp::Snapshot(row)).is_ok()
    }

    pub fn try_enqueue_oracle_event(&self, evt: WsEvent) -> bool {
        self.tx.try_send(DbOp::OracleEvent(evt)).is_ok()
    }

    pub fn try_enqueue_markets(&self, rows: Vec<MarketUpsertRow>) -> bool {
        self.tx.try_send(DbOp::MarketUpserts(rows)).is_ok()
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
        CREATE TABLE IF NOT EXISTS pm_markets (
            condition_id         VARCHAR     PRIMARY KEY,
            question             VARCHAR     NOT NULL,
            asset                VARCHAR     NOT NULL,
            timeframe            VARCHAR,
            direction            VARCHAR,
            close_time           TIMESTAMPTZ,
            resolved_at          TIMESTAMPTZ,
            outcome              VARCHAR,
            winning_price        DOUBLE,
            token_yes_id         VARCHAR,
            token_no_id          VARCHAR,
            pivot_price          DOUBLE,
            created_at           TIMESTAMPTZ DEFAULT now(),
            last_updated         TIMESTAMPTZ
        );

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

        ALTER TABLE pm_markets ADD COLUMN IF NOT EXISTS pivot_price DOUBLE;

        CREATE INDEX IF NOT EXISTS idx_pm_markets_close_time ON pm_markets (close_time);
        CREATE INDEX IF NOT EXISTS idx_pm_snapshots_cond ON pm_snapshots (condition_id, ts);
        CREATE INDEX IF NOT EXISTS idx_ob_levels_token_ts ON pm_order_book_levels (token_id, ts);

        CREATE TABLE IF NOT EXISTS oracle_ticks (
            ts                   TIMESTAMPTZ NOT NULL,
            symbol               VARCHAR     NOT NULL,
            market_price         DOUBLE,
            chainlink_price      DOUBLE,
            chainlink_age_secs   BIGINT,
            deviation_pct        DOUBLE,
            round_imminent       BOOLEAN,
            price_binance        DOUBLE,
            price_coinbase       DOUBLE,
            price_kraken         DOUBLE,
            latency_binance_ms   BIGINT,
            latency_coinbase_ms  BIGINT,
            latency_kraken_ms    BIGINT,
            ema_12               DOUBLE,
            ema_26               DOUBLE,
            ema_50               DOUBLE,
            rsi_14               DOUBLE,
            momentum_10          DOUBLE,
            momentum_20          DOUBLE,
            mro_5                DOUBLE,
            mro_10               DOUBLE,
            mro_15               DOUBLE,
            volatility           DOUBLE,
            bb_upper             DOUBLE,
            bb_middle            DOUBLE,
            bb_lower             DOUBLE,
            macd                 DOUBLE,
            macd_signal          DOUBLE,
            macd_histogram       DOUBLE,
            p_lmsr               DOUBLE,
            delta_lmsr           DOUBLE,
            delta_lmsr_z         DOUBLE,
            lmsr_version         VARCHAR,
            alpha_live           DOUBLE,
            b_live               DOUBLE,
            PRIMARY KEY (ts, symbol)
        );

        ALTER TABLE oracle_ticks ADD COLUMN IF NOT EXISTS latency_binance_ms BIGINT;
        ALTER TABLE oracle_ticks ADD COLUMN IF NOT EXISTS latency_coinbase_ms BIGINT;
        ALTER TABLE oracle_ticks ADD COLUMN IF NOT EXISTS latency_kraken_ms BIGINT;
        ALTER TABLE oracle_ticks ADD COLUMN IF NOT EXISTS p_lmsr DOUBLE;
        ALTER TABLE oracle_ticks ADD COLUMN IF NOT EXISTS delta_lmsr DOUBLE;
        ALTER TABLE oracle_ticks ADD COLUMN IF NOT EXISTS delta_lmsr_z DOUBLE;
        ALTER TABLE oracle_ticks ADD COLUMN IF NOT EXISTS lmsr_version VARCHAR;
        ALTER TABLE oracle_ticks ADD COLUMN IF NOT EXISTS alpha_live DOUBLE;
        ALTER TABLE oracle_ticks ADD COLUMN IF NOT EXISTS b_live DOUBLE;
        ALTER TABLE oracle_ticks ADD COLUMN IF NOT EXISTS mro_5 DOUBLE;
        ALTER TABLE oracle_ticks ADD COLUMN IF NOT EXISTS mro_10 DOUBLE;
        ALTER TABLE oracle_ticks ADD COLUMN IF NOT EXISTS mro_15 DOUBLE;

        CREATE TABLE IF NOT EXISTS signal_events (
            id                   BIGINT,
            ts                   TIMESTAMPTZ NOT NULL,
            event_type           VARCHAR     NOT NULL,
            symbol               VARCHAR     NOT NULL,
            direction            VARCHAR,
            deviation_pct        DOUBLE,
            market_price         DOUBLE,
            chainlink_price      DOUBLE,
            chainlink_age_secs   BIGINT,
            prev_price           DOUBLE,
            new_price            DOUBLE,
            price_delta          DOUBLE,
            delta_pct            DOUBLE,
            round_duration_secs  BIGINT,
            signals_json         VARCHAR,
            bb_width_pct         DOUBLE,
            rsi_14               DOUBLE,
            momentum_10          DOUBLE,
            mro_5                DOUBLE,
            mro_10               DOUBLE,
            mro_15               DOUBLE,
            bb_upper             DOUBLE,
            bb_lower             DOUBLE,
            bb_expanding         BOOLEAN,
            raw_json             VARCHAR
        );
        ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS mro_5 DOUBLE;
        ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS mro_10 DOUBLE;
        ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS mro_15 DOUBLE;

        CREATE INDEX IF NOT EXISTS idx_signal_events_ts ON signal_events (ts);
        CREATE INDEX IF NOT EXISTS idx_signal_events_type ON signal_events (event_type, ts);
        "#,
    )?;
    info!(db_path=%db_path, "clob writer connected to duckdb");
    Ok(conn)
}

fn ts_sql(ts: &DateTime<Utc>) -> String {
    // DuckDB TIMESTAMPTZ parsing is most reliable with microsecond precision.
    ts.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn flush_batch(conn: &mut Connection, rows: &mut Vec<SnapshotRow>, write_levels: bool, max_levels: usize) -> anyhow::Result<()> {
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

        let mut lvl_stmt = if write_levels {
            Some(tx.prepare(
                "
                INSERT INTO pm_order_book_levels (
                    ts, condition_id, token_id, side, level, price, size, cumulative_size, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                "
            )?)
        } else {
            None
        };

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

            if let Some(lvl_stmt) = lvl_stmt.as_mut() {
                // Full depth archival (BID side)
                let mut cumulative = 0.0f64;
                for (idx, lvl) in r.bid_levels.iter().take(max_levels).enumerate() {
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
                for (idx, lvl) in r.ask_levels.iter().take(max_levels).enumerate() {
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
    }
    tx.commit()?;

    info!(rows=rows.len(), "clob writer flush complete");
    rows.clear();
    Ok(())
}

fn dir_to_str(d: &RoundDirection) -> &'static str {
    match d {
        RoundDirection::Up => "UP",
        RoundDirection::Down => "DOWN",
    }
}

fn flush_oracle_events(conn: &mut Connection, events: &mut Vec<WsEvent>, write_ticks: bool) -> anyhow::Result<()> {
    if events.is_empty() { return Ok(()); }
    let tx = conn.transaction()?;
    {
        let mut tick_stmt = if write_ticks {
            Some(tx.prepare("INSERT OR IGNORE INTO oracle_ticks (ts,symbol,market_price,chainlink_price,chainlink_age_secs,deviation_pct,round_imminent,price_binance,price_coinbase,price_kraken,latency_binance_ms,latency_coinbase_ms,latency_kraken_ms,ema_12,ema_26,ema_50,rsi_14,momentum_10,momentum_20,mro_5,mro_10,mro_15,volatility,bb_upper,bb_middle,bb_lower,macd,macd_signal,macd_histogram,p_lmsr,delta_lmsr,delta_lmsr_z,lmsr_version,alpha_live,b_live) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")?)
        } else { None };
        let mut sig_stmt = tx.prepare("INSERT INTO signal_events (ts,event_type,symbol,direction,deviation_pct,market_price,chainlink_price,chainlink_age_secs,prev_price,new_price,price_delta,delta_pct,round_duration_secs,signals_json,bb_width_pct,rsi_14,momentum_10,mro_5,mro_10,mro_15,bb_upper,bb_lower,bb_expanding,raw_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")?;

        for evt in events.iter() {
            match evt {
                WsEvent::Tick { ts, data } => {
                    if let Some(tick_stmt) = tick_stmt.as_mut() {
                        tick_stmt.execute(params![
                            ts.to_rfc3339(), data.symbol, data.market_price, data.chainlink_price, data.chainlink_age_secs.map(|v| v as i64), data.deviation_pct, data.round_imminent,
                            data.exchange_prices.get("binance").copied(), data.exchange_prices.get("coinbase").copied(), data.exchange_prices.get("kraken").copied(),
                            data.exchange_latency_ms.as_ref().and_then(|m| m.get("binance").copied()), data.exchange_latency_ms.as_ref().and_then(|m| m.get("coinbase").copied()), data.exchange_latency_ms.as_ref().and_then(|m| m.get("kraken").copied()),
                            data.indicators.ema_12, data.indicators.ema_26, data.indicators.ema_50, data.indicators.rsi_14,
                            data.indicators.momentum_10, data.indicators.momentum_20, data.indicators.mro_5, data.indicators.mro_10, data.indicators.mro_15, data.indicators.volatility,
                            data.indicators.bb_upper, data.indicators.bb_middle, data.indicators.bb_lower,
                            data.indicators.macd, data.indicators.macd_signal, data.indicators.macd_histogram,
                            data.p_lmsr, data.delta_lmsr, data.delta_lmsr_z, data.lmsr_version.clone(), data.alpha_live, data.b_live,
                        ])?;
                    }
                }
                WsEvent::PreTriggerAlert { ts, data } => {
                    sig_stmt.execute(params![ts.to_rfc3339(),"pre_trigger_alert","BTC/USD",dir_to_str(&data.direction),Some(data.deviation_pct),Some(data.market_price),Some(data.chainlink_price),Some(data.chainlink_age_secs as i64),Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<i64>::None,Some(serde_json::to_string(&data.signals).unwrap_or_else(|_|"[]".to_string())),data.bb_width_pct,data.rsi_14,data.momentum_10,data.mro_5,data.mro_10,data.mro_15,Option::<f64>::None,Option::<f64>::None,Option::<bool>::None,serde_json::to_string(evt).ok()])?;
                }
                WsEvent::RoundTriggered { ts, data } => {
                    sig_stmt.execute(params![ts.to_rfc3339(),"round_triggered","BTC/USD",dir_to_str(&data.direction),Some(data.deviation_pct),Some(data.market_price),Some(data.chainlink_price),Some(data.chainlink_age_secs as i64),Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<i64>::None,Option::<String>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<bool>::None,serde_json::to_string(evt).ok()])?;
                }
                WsEvent::RoundSettled { ts, data } => {
                    if data.round_duration_secs.is_none() { continue; }
                    sig_stmt.execute(params![ts.to_rfc3339(),"round_settled","BTC/USD",Option::<String>::None,Some(data.delta_pct),Some(data.new_price),Option::<f64>::None,Option::<i64>::None,Some(data.prev_price),Some(data.new_price),Some(data.price_delta),Some(data.delta_pct),data.round_duration_secs.map(|v|v as i64),Option::<String>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<bool>::None,serde_json::to_string(evt).ok()])?;
                }
                WsEvent::DeviationApproach { ts, data } => {
                    sig_stmt.execute(params![ts.to_rfc3339(),"deviation_approach","BTC/USD",dir_to_str(&data.direction),Some(data.deviation_pct),Some(data.market_price),Some(data.chainlink_price),Some(data.chainlink_age_secs as i64),Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<i64>::None,Option::<String>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<bool>::None,serde_json::to_string(evt).ok()])?;
                }
                WsEvent::BbBreakout { ts, data } => {
                    sig_stmt.execute(params![
                        ts.to_rfc3339(),"bb_breakout","BTC/USD",dir_to_str(&data.direction),
                        data.deviation_pct,Some(data.market_price),Option::<f64>::None,Option::<i64>::None,
                        Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,Option::<i64>::None,
                        Option::<String>::None,
                        Some(data.bb_width_pct),Option::<f64>::None,Option::<f64>::None,
                        Option::<f64>::None,Option::<f64>::None,Option::<f64>::None,
                        Some(data.bb_upper),Some(data.bb_lower),Some(data.bb_expanding),
                        serde_json::to_string(evt).ok()
                    ])?;
                }
                WsEvent::ExchangeStatus { .. } => {}
            }
        }
    }
    tx.commit()?;
    events.clear();
    Ok(())
}

fn flush_market_upserts(conn: &mut Connection, batches: &mut Vec<Vec<MarketUpsertRow>>) -> anyhow::Result<()> {
    if batches.is_empty() { return Ok(()); }
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare("INSERT INTO pm_markets (condition_id,question,asset,timeframe,direction,close_time,resolved_at,outcome,winning_price,token_yes_id,token_no_id,pivot_price,last_updated) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, now()) ON CONFLICT(condition_id) DO UPDATE SET question=excluded.question,asset=excluded.asset,timeframe=excluded.timeframe,close_time=excluded.close_time,resolved_at=COALESCE(excluded.resolved_at,pm_markets.resolved_at),outcome=COALESCE(excluded.outcome,pm_markets.outcome),winning_price=COALESCE(excluded.winning_price,pm_markets.winning_price),token_yes_id=excluded.token_yes_id,token_no_id=excluded.token_no_id,pivot_price=COALESCE(excluded.pivot_price,pm_markets.pivot_price),last_updated=now()")?;
        for batch in batches.iter() {
            for m in batch {
                stmt.execute(params![m.condition_id,m.question,m.asset,m.timeframe,m.close_time,m.resolved_at,m.outcome,m.winning_price,m.token_yes_id,m.token_no_id,m.pivot_price])?;
            }
        }
    }
    tx.commit()?;
    batches.clear();
    Ok(())
}

pub async fn run_writer_loop(mut rx: mpsc::Receiver<DbOp>, flush_every_ms: u64, ui_state: Arc<RwLock<ClobUiState>>) {
    let writes_enabled = std::env::var("DB_WRITES_ENABLED")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(true);

    if !writes_enabled {
        warn!("db writer disabled via DB_WRITES_ENABLED=false; consuming queue without persistence");
        let mut received_rows: u64 = 0;
        let mut stats_tick = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    match maybe {
                        Some(_op) => { received_rows += 1; }
                        None => break,
                    }
                }
                _ = stats_tick.tick() => {
                    {
                        let mut s = ui_state.write().await;
                        s.set_writer_stats(received_rows, 0, 0, 0);
                    }
                    info!(writer_received_rows=received_rows, "db writer disabled stats");
                }
            }
        }
        return;
    }

    let mut conn = match open_db() {
        Ok(c) => c,
        Err(e) => {
            error!(error=%e, "failed to open duckdb writer connection");
            return;
        }
    };

    let write_levels = std::env::var("CLOB_WRITE_LEVELS")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    info!(write_levels, "clob writer level archival mode");

    let write_ticks = std::env::var("ORACLE_WRITE_TICKS")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    let max_levels = std::env::var("CLOB_MAX_LEVELS")
        .ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(10);
    let tick_every_n = std::env::var("ORACLE_TICK_DB_EVERY_N")
        .ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(5).max(1);
    info!(write_levels, write_ticks, max_levels, tick_every_n, "db writer throughput controls");

    let mut buf_snap: Vec<SnapshotRow> = Vec::with_capacity(2048);
    let mut buf_oracle: Vec<WsEvent> = Vec::with_capacity(2048);
    let mut buf_markets: Vec<Vec<MarketUpsertRow>> = Vec::with_capacity(32);

    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(flush_every_ms));
    let mut stats_tick = tokio::time::interval(std::time::Duration::from_secs(10));
    let mut received_rows: u64 = 0;
    let mut flush_error_count: u64 = 0;
    let mut recovered_drop_count: u64 = 0;
    let mut tick_seen: u64 = 0;

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(op) => {
                        received_rows += 1;
                        match op {
                            DbOp::Snapshot(row) => buf_snap.push(row),
                            DbOp::OracleEvent(evt) => {
                                let keep = match &evt {
                                    WsEvent::Tick { .. } => {
                                        tick_seen += 1;
                                        (tick_seen % tick_every_n) == 0
                                    }
                                    _ => true,
                                };
                                if keep {
                                    buf_oracle.push(evt);
                                }
                            },
                            DbOp::MarketUpserts(rows) => if !rows.is_empty() { buf_markets.push(rows); },
                        }

                        if buf_snap.len() >= 1000 || buf_oracle.len() >= 1000 || buf_markets.len() >= 8 {
                            if let Err(e) = flush_batch(&mut conn, &mut buf_snap, write_levels, max_levels)
                                .and_then(|_| flush_oracle_events(&mut conn, &mut buf_oracle, write_ticks))
                                .and_then(|_| flush_market_upserts(&mut conn, &mut buf_markets))
                            {
                                flush_error_count += 1;
                                error!(snap_buf=buf_snap.len(), oracle_buf=buf_oracle.len(), markets_buf=buf_markets.len(), flush_error_count, "db writer batch flush failed: {:#}", e);
                                if let Some(dropped) = (!buf_snap.is_empty()).then(|| buf_snap.remove(0)) {
                                    recovered_drop_count += 1;
                                    error!(token_id=%dropped.token_id, condition_id=%dropped.condition_id, ts=%dropped.ts.to_rfc3339(), recovered_drop_count, "db writer dropped one snapshot row to recover from persistent flush failure");
                                } else if !buf_oracle.is_empty() {
                                    buf_oracle.remove(0);
                                    recovered_drop_count += 1;
                                }
                            }
                        }
                    }
                    None => {
                        if let Err(e) = flush_batch(&mut conn, &mut buf_snap, write_levels, max_levels)
                            .and_then(|_| flush_oracle_events(&mut conn, &mut buf_oracle, write_ticks))
                            .and_then(|_| flush_market_upserts(&mut conn, &mut buf_markets))
                        {
                            error!(snap_buf=buf_snap.len(), oracle_buf=buf_oracle.len(), markets_buf=buf_markets.len(), "db writer shutdown flush failed: {:#}", e);
                        }
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                if let Err(e) = flush_batch(&mut conn, &mut buf_snap, write_levels, max_levels)
                    .and_then(|_| flush_oracle_events(&mut conn, &mut buf_oracle, write_ticks))
                    .and_then(|_| flush_market_upserts(&mut conn, &mut buf_markets))
                {
                    flush_error_count += 1;
                    error!(snap_buf=buf_snap.len(), oracle_buf=buf_oracle.len(), markets_buf=buf_markets.len(), flush_error_count, "db writer periodic flush failed: {:#}", e);
                    if let Some(dropped) = (!buf_snap.is_empty()).then(|| buf_snap.remove(0)) {
                        recovered_drop_count += 1;
                        error!(token_id=%dropped.token_id, condition_id=%dropped.condition_id, ts=%dropped.ts.to_rfc3339(), recovered_drop_count, "db writer dropped one snapshot row to recover from persistent periodic flush failure");
                    } else if !buf_oracle.is_empty() {
                        buf_oracle.remove(0);
                        recovered_drop_count += 1;
                    }
                }
            }
            _ = stats_tick.tick() => {
                {
                    let mut s = ui_state.write().await;
                    s.set_writer_stats(received_rows, (buf_snap.len()+buf_oracle.len()) as u64, flush_error_count, recovered_drop_count);
                }
                info!(writer_received_rows=received_rows, snap_buf=buf_snap.len(), oracle_buf=buf_oracle.len(), markets_buf=buf_markets.len(), writer_flush_errors=flush_error_count, writer_recovered_drops=recovered_drop_count, "db writer stats");
            }
        }
    }
}
