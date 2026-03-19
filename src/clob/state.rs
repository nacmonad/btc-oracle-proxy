use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::clob::writer::SnapshotRow;

#[derive(Debug, Clone)]
pub struct SideQuote {
    pub token_id: String,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread: Option<f64>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct RoundQuote {
    pub condition_id: String,
    pub close_time: Option<String>,
    pub up: SideQuote,
    pub down: SideQuote,
}

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
    pub pivot: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct ClobUiState {
    pub reconnects: u64,
    pub dropped_rows: u64,
    pub enqueued_rows: u64,
    pub malformed_rows: u64,
    pub skipped_non_current_rows: u64,
    pub sampler_ingested_rows: u64,
    pub sampler_emitted_rows: u64,
    pub writer_received_rows: u64,
    pub writer_buffered_rows: u64,
    pub writer_flush_errors: u64,
    pub writer_recovered_drops: u64,
    pub last_backoff_ms: u64,
    pub last_message_at: Option<DateTime<Utc>>,
    pub tokens: HashMap<String, ClobTokenStats>, // key=token_id
    pub markets: HashMap<String, MarketMeta>,     // key=token_id
}

fn parse_close_time_to_utc(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%z") {
        return Some(dt.with_timezone(&Utc));
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(chrono::DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc));
    }
    None
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

    pub fn on_malformed(&mut self) {
        self.malformed_rows += 1;
    }

    pub fn on_skipped_non_current(&mut self) {
        self.skipped_non_current_rows += 1;
    }

    pub fn set_sampler_stats(&mut self, ingested: u64, emitted: u64) {
        self.sampler_ingested_rows = ingested;
        self.sampler_emitted_rows = emitted;
    }

    pub fn set_writer_stats(&mut self, received: u64, buffered: u64, flush_errors: u64, recovered_drops: u64) {
        self.writer_received_rows = received;
        self.writer_buffered_rows = buffered;
        self.writer_flush_errors = flush_errors;
        self.writer_recovered_drops = recovered_drops;
    }

    pub fn set_market_meta(&mut self, token_id: String, condition_id: String, asset: String, timeframe: String, close_time: String, side: String, pivot: Option<f64>) {
        self.markets.insert(token_id.clone(), MarketMeta { token_id, condition_id, asset, timeframe, close_time, side, pivot });
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

    pub fn round_quote_at(
        &self,
        asset: &str,
        preferred_timeframe: &str,
        ref_ts: DateTime<Utc>,
        max_lookahead_secs: i64,
        past_grace_secs: i64,
    ) -> Option<RoundQuote> {
        let mut candidates: HashMap<String, (Option<DateTime<Utc>>, Option<String>, Option<String>, Option<String>)> = HashMap::new();
        for m in self.markets.values() {
            if !m.asset.eq_ignore_ascii_case(asset) {
                continue;
            }
            // Strict timeframe match to avoid leaking 5m IDs into 15m strategy flow.
            if m.timeframe != preferred_timeframe {
                continue;
            }
            let entry = candidates
                .entry(m.condition_id.clone())
                .or_insert((parse_close_time_to_utc(&m.close_time), Some(m.close_time.clone()), None, None));

            if m.side.eq_ignore_ascii_case("UP") {
                entry.2 = Some(m.token_id.clone());
            } else if m.side.eq_ignore_ascii_case("DOWN") {
                entry.3 = Some(m.token_id.clone());
            }
        }

        let mut picked: Option<(String, Option<DateTime<Utc>>, Option<String>, String, String)> = None;
        let mut picked_any: Option<(String, Option<DateTime<Utc>>, Option<String>, String, String)> = None;

        for (cid, (close_dt, close_raw, up, down)) in candidates {
            let (Some(up_id), Some(down_id)) = (up, down) else { continue };
            let delta = close_dt.map(|dt| dt.signed_duration_since(ref_ts).num_seconds()).unwrap_or(i64::MAX);
            let score = delta.abs();

            // Best-any fallback (no time window) to avoid starving oracle frame
            // when close_time parsing/windowing misses but L1/L2 is live.
            match &picked_any {
                None => picked_any = Some((cid.clone(), close_dt, close_raw.clone(), up_id.clone(), down_id.clone())),
                Some((_, best_dt, _, _, _)) => {
                    let best_delta = best_dt.map(|dt| dt.signed_duration_since(ref_ts).num_seconds()).unwrap_or(i64::MAX);
                    if score < best_delta.abs() {
                        picked_any = Some((cid.clone(), close_dt, close_raw.clone(), up_id.clone(), down_id.clone()));
                    }
                }
            }

            // Accept close times slightly in the past (grace) and up to configured lookahead.
            if delta < -past_grace_secs || delta > max_lookahead_secs {
                continue;
            }

            match &picked {
                None => picked = Some((cid, close_dt, close_raw, up_id, down_id)),
                Some((_, best_dt, _, _, _)) => {
                    let best_delta = best_dt.map(|dt| dt.signed_duration_since(ref_ts).num_seconds()).unwrap_or(i64::MAX);
                    if score < best_delta.abs() {
                        picked = Some((cid, close_dt, close_raw, up_id, down_id));
                    }
                }
            }
        }

        let (condition_id, _close_dt, close_raw, up_id, down_id) = picked.or(picked_any)?;
        let up = self.tokens.get(&up_id)?;
        let down = self.tokens.get(&down_id)?;

        Some(RoundQuote {
            condition_id,
            close_time: close_raw,
            up: SideQuote {
                token_id: up_id,
                best_bid: up.best_bid,
                best_ask: up.best_ask,
                spread: up.spread,
                updated_at: up.updated_at,
            },
            down: SideQuote {
                token_id: down_id,
                best_bid: down.best_bid,
                best_ask: down.best_ask,
                spread: down.spread,
                updated_at: down.updated_at,
            },
        })
    }

    pub fn latest_round_quote(&self, asset: &str, preferred_timeframe: &str) -> Option<RoundQuote> {
        self.round_quote_at(asset, preferred_timeframe, Utc::now(), 3600, 45)
    }
}

