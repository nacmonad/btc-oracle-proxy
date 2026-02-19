//! Reads the Chainlink BTC/USD on-chain price from the Polygon aggregator contract.
//!
//! Polls `latestRoundData()` on the BTC/USD Chainlink feed via a Polygon JSON-RPC
//! endpoint every 5 seconds. This gives us the last *committed* price — the stale
//! baseline we measure deviation against.
//!
//! Contract: 0xc907E116054Ad103354f2D350FD2514433D57F6F (BTC/USD, Polygon mainnet)
//! Decimals: 8  (price = answer / 10^8)
//!
//! `latestRoundData()` ABI-encoded response (5 × 32-byte words):
//!   [0]  roundId        uint80
//!   [1]  answer         int256   ← price × 10^8
//!   [2]  startedAt      uint256
//!   [3]  updatedAt      uint256  ← Unix timestamp of last on-chain commit
//!   [4]  answeredInRound uint80

use crate::error::OracleError;
use crate::models::{AppState, ChainlinkBaseline};
use chrono::{DateTime, TimeZone, Utc};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Chainlink BTC/USD aggregator on Polygon mainnet.
const CONTRACT: &str = "0xc907E116054Ad103354f2D350FD2514433D57F6F";

/// `latestRoundData()` function selector.
const SELECTOR: &str = "0xfeaf968c";

/// Poll interval in seconds.
const POLL_SECS: u64 = 5;

pub struct ChainlinkClient {
    rpc_url: String,
    state: Arc<RwLock<AppState>>,
}

impl ChainlinkClient {
    pub fn new(rpc_url: String, state: Arc<RwLock<AppState>>) -> Self {
        Self { rpc_url, state }
    }

    /// Polls on-chain Chainlink price forever, writing updates to `AppState`.
    pub async fn run(self) {
        info!("Chainlink poller starting (RPC: {})", self.rpc_url);
        let client = reqwest::Client::new();
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_secs(POLL_SECS));

        loop {
            interval.tick().await;
            match Self::fetch(&client, &self.rpc_url).await {
                Ok((price, updated_at)) => {
                    let age = Utc::now()
                        .signed_duration_since(updated_at)
                        .num_seconds();
                    debug!("Chainlink on-chain: ${price:.2}  (committed {age}s ago)");

                    let mut s = self.state.write().await;
                    s.chainlink_baseline = Some(ChainlinkBaseline { price, updated_at });
                }
                Err(e) => error!("Chainlink RPC error: {e}"),
            }
        }
    }

    async fn fetch(
        client: &reqwest::Client,
        rpc_url: &str,
    ) -> Result<(f64, DateTime<Utc>), OracleError> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method":  "eth_call",
            "params":  [{ "to": CONTRACT, "data": SELECTOR }, "latest"],
            "id":      1
        });

        let resp = client
            .post(rpc_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| OracleError::HttpError(e.to_string()))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| OracleError::HttpError(e.to_string()))?;

        if let Some(err) = resp.get("error") {
            return Err(OracleError::HttpError(format!("RPC error: {err}")));
        }

        let hex = resp
            .get("result")
            .and_then(|r| r.as_str())
            .ok_or_else(|| OracleError::InvalidPriceData("Missing result".to_string()))?;

        decode_latest_round_data(hex)
            .ok_or_else(|| OracleError::InvalidPriceData("ABI decode failed".to_string()))
    }
}

/// Decodes the ABI-encoded `latestRoundData()` response.
/// Returns `(price_usd, updated_at)`.
fn decode_latest_round_data(hex: &str) -> Option<(f64, DateTime<Utc>)> {
    let hex = hex.trim_start_matches("0x");

    // Need at least 5 words × 64 hex chars = 320 chars
    if hex.len() < 320 {
        warn!("Chainlink ABI response too short: {} chars", hex.len());
        return None;
    }

    // Word 1 (chars 64–127): answer (int256, always positive for BTC)
    // Take the last 16 hex chars of the word = 8 bytes = u64
    // BTC at $1M with 8 decimals = 10^13 < u64::MAX (1.8×10^19)  ✓
    let price_raw = u64::from_str_radix(&hex[112..128], 16).ok()?;
    let price = price_raw as f64 / 1e8;

    if price <= 0.0 || price > 10_000_000.0 {
        warn!("Chainlink decoded price out of range: {price}");
        return None;
    }

    // Word 3 (chars 192–255): updatedAt (uint256, Unix timestamp)
    let updated_at_unix = u64::from_str_radix(&hex[240..256], 16).ok()?;
    let updated_at = Utc
        .timestamp_opt(updated_at_unix as i64, 0)
        .single()
        .unwrap_or_else(Utc::now);

    Some((price, updated_at))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_sample_response() {
        // Synthesised ABI response: answer = 6723450000000 (= $67234.50)
        // updatedAt = 1726137296 (some Unix timestamp)
        // answer hex: 0x00000000000000000000000000000000000000000000000000000620C0F24000
        // updatedAt hex: 0x0000000000000000000000000000000000000000000000000000000066E02510

        let answer_raw: u64 = 6_723_450_000_000;
        let updated_at_raw: u64 = 1_726_137_296;

        // Build a fake 5-word (320 hex char) ABI response
        let word = |v: u64| format!("{v:0>64x}");
        let hex = format!(
            "{}{}{}{}{}",
            word(0),            // roundId
            word(answer_raw),   // answer
            word(0),            // startedAt
            word(updated_at_raw), // updatedAt
            word(0),            // answeredInRound
        );

        let (price, ts) = decode_latest_round_data(&hex).unwrap();
        assert!((price - 67234.50).abs() < 0.001, "price={price}");
        assert_eq!(ts.timestamp() as u64, updated_at_raw);
    }
}
