use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::clob::writer::SnapshotRow;

#[derive(Debug, Clone, Default)]
pub struct ClobTokenStats {
    pub condition_id: String,
    pub token_id: String,
    pub timeframe: String, // best effort (from market mapping when available)
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread: Option<f64>,
    pub bid_depth_5: Option<f64>,
    pub ask_depth_5: Option<f64>,
    pub depth_imbalance_5: Option<f64>,
    pub slippage_100: Option<f64>,
    pub total_bid_levels: i32,
    pub total_ask_levels: i32,
    pub updated_at: DateTime<Utc>,
    pub bid_ladder: Vec<(f64, f64)>, // (price, size) top levels
    pub ask_ladder: Vec<(f64, f64)>,
}

#[derive(Debug, Clone, Default)]
pub struct MarketMeta {
    pub condition_id: String,
    pub asset: String,
    pub timeframe: String,
    pub close_time: String,
    pub token_id: String,
    pub side: String, // UP | DOWN
}

#[derive(Debug, Clone, Default)]
pub struct ClobUiState {
    pub reconnects: u64,
    pub dropped_rows: u64,
    pub enqueued_rows: u64,
    pub last_backoff_ms: u64,
    pub last_message_at: Option<DateTime<Utc>>,
    pub tokens: HashMap<String, ClobTokenStats>, // key=token_id
    pub markets: HashMap<String, MarketMeta>,     // key=token_id
}

impl ClobUiState {
    pub fn on_reconnect(&mut self, backoff_ms: u64) {
        self.reconnects += 1;
        self.last_backoff_ms = backoff_ms;
    }

    pub fn on_drop(&mut self) {
        self.dropped_rows += 1;
    }

    pub fn on_enqueue(&mut self) {
        self.enqueued_rows += 1;
    }

    pub fn set_market_meta(&mut self, token_id: String, condition_id: String, asset: String, timeframe: String, close_time: String, side: String) {
        self.markets.insert(token_id.clone(), MarketMeta { token_id, condition_id, asset, timeframe, close_time, side });
    }

    pub fn update_from_row(&mut self, row: &SnapshotRow) {
        self.last_message_at = Some(Utc::now());
        self.tokens.insert(
            row.token_id.clone(),
            ClobTokenStats {
                condition_id: row.condition_id.clone(),
                token_id: row.token_id.clone(),
                timeframe: "?".to_string(),
                best_bid: row.best_bid,
                best_ask: row.best_ask,
                spread: row.spread,
                bid_depth_5: row.bid_depth_5,
                ask_depth_5: row.ask_depth_5,
                depth_imbalance_5: row.depth_imbalance_5,
                slippage_100: row.slippage_100,
                total_bid_levels: row.total_bid_levels,
                total_ask_levels: row.total_ask_levels,
                updated_at: row.ts,
                bid_ladder: row.bid_levels.iter().take(5).map(|l| (l.price, l.size)).collect(),
                ask_ladder: row.ask_levels.iter().take(5).map(|l| (l.price, l.size)).collect(),
            },
        );
    }
}
