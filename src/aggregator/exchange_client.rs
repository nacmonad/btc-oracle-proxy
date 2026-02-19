//! Exchange WebSocket clients — primary sources used by the Chainlink BTC/USD oracle.
//! Binance (aggTrade), Coinbase Exchange (ticker), Kraken (ticker v1)
//! Each client runs forever, reconnecting with exponential backoff on any error.

use crate::error::OracleError;
use crate::models::ExchangePrice;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

const MAX_BACKOFF_SECS: u64 = 60;

// ── Binance ──────────────────────────────────────────────────────────────────

pub struct BinanceClient;

impl BinanceClient {
    pub async fn run(sender: mpsc::Sender<ExchangePrice>) {
        let mut backoff = 1u64;
        loop {
            info!("Connecting to Binance...");
            match Self::connect(&sender).await {
                Ok(()) => {
                    warn!("Binance stream ended, reconnecting...");
                    backoff = 1;
                }
                Err(e) => {
                    error!("Binance error: {e}, retrying in {backoff}s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF_SECS);
                }
            }
        }
    }

    async fn connect(sender: &mpsc::Sender<ExchangePrice>) -> Result<(), OracleError> {
        let (ws, _) = connect_async("wss://stream.binance.com:9443/ws/btcusdt@aggTrade")
            .await
            .map_err(|e| OracleError::ExchangeConnectionError(format!("Binance: {e}")))?;

        info!("Binance connected");
        let (mut write, mut read) = ws.split();

        while let Some(msg) = read.next().await {
            match msg.map_err(|e| OracleError::WebSocketError(e.to_string()))? {
                Message::Text(text) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        // aggTrade: {"e":"aggTrade","p":"<price>","q":"<qty>",...}
                        if v.get("e").and_then(|e| e.as_str()) == Some("aggTrade") {
                            if let Some(price) = parse_f64_field(&v, "p") {
                                let volume = parse_f64_field(&v, "q");
                                debug!("Binance  BTC/USD ${price:.2}");
                                if sender
                                    .send(ExchangePrice {
                                        exchange: "binance".to_string(),
                                        price,
                                        timestamp: Utc::now(),
                                        volume,
                                    })
                                    .await
                                    .is_err()
                                {
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
                Message::Ping(data) => {
                    write
                        .send(Message::Pong(data))
                        .await
                        .map_err(|e| OracleError::WebSocketError(e.to_string()))?;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }

        Ok(())
    }
}

// ── Coinbase Exchange ─────────────────────────────────────────────────────────

pub struct CoinbaseClient;

impl CoinbaseClient {
    pub async fn run(sender: mpsc::Sender<ExchangePrice>) {
        let mut backoff = 1u64;
        loop {
            info!("Connecting to Coinbase...");
            match Self::connect(&sender).await {
                Ok(()) => {
                    warn!("Coinbase stream ended, reconnecting...");
                    backoff = 1;
                }
                Err(e) => {
                    error!("Coinbase error: {e}, retrying in {backoff}s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF_SECS);
                }
            }
        }
    }

    async fn connect(sender: &mpsc::Sender<ExchangePrice>) -> Result<(), OracleError> {
        let (ws, _) = connect_async("wss://ws-feed.exchange.coinbase.com")
            .await
            .map_err(|e| OracleError::ExchangeConnectionError(format!("Coinbase: {e}")))?;

        info!("Coinbase connected");
        let (mut write, mut read) = ws.split();

        let sub = serde_json::json!({
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channels": ["ticker"]
        });
        write
            .send(Message::Text(sub.to_string()))
            .await
            .map_err(|e| OracleError::WebSocketError(e.to_string()))?;

        while let Some(msg) = read.next().await {
            match msg.map_err(|e| OracleError::WebSocketError(e.to_string()))? {
                Message::Text(text) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        // ticker: {"type":"ticker","price":"<price>","last_size":"<qty>",...}
                        if v.get("type").and_then(|t| t.as_str()) == Some("ticker") {
                            if let Some(price) = parse_f64_field(&v, "price") {
                                let volume = parse_f64_field(&v, "last_size");
                                debug!("Coinbase  BTC/USD ${price:.2}");
                                if sender
                                    .send(ExchangePrice {
                                        exchange: "coinbase".to_string(),
                                        price,
                                        timestamp: Utc::now(),
                                        volume,
                                    })
                                    .await
                                    .is_err()
                                {
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
                Message::Ping(data) => {
                    write
                        .send(Message::Pong(data))
                        .await
                        .map_err(|e| OracleError::WebSocketError(e.to_string()))?;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }

        Ok(())
    }
}

// ── Kraken ───────────────────────────────────────────────────────────────────

pub struct KrakenClient;

impl KrakenClient {
    pub async fn run(sender: mpsc::Sender<ExchangePrice>) {
        let mut backoff = 1u64;
        loop {
            info!("Connecting to Kraken...");
            match Self::connect(&sender).await {
                Ok(()) => {
                    warn!("Kraken stream ended, reconnecting...");
                    backoff = 1;
                }
                Err(e) => {
                    error!("Kraken error: {e}, retrying in {backoff}s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF_SECS);
                }
            }
        }
    }

    async fn connect(sender: &mpsc::Sender<ExchangePrice>) -> Result<(), OracleError> {
        let (ws, _) = connect_async("wss://ws.kraken.com/")
            .await
            .map_err(|e| OracleError::ExchangeConnectionError(format!("Kraken: {e}")))?;

        info!("Kraken connected");
        let (mut write, mut read) = ws.split();

        let sub = serde_json::json!({
            "event": "subscribe",
            "pair": ["XBT/USD"],
            "subscription": { "name": "ticker" }
        });
        write
            .send(Message::Text(sub.to_string()))
            .await
            .map_err(|e| OracleError::WebSocketError(e.to_string()))?;

        while let Some(msg) = read.next().await {
            match msg.map_err(|e| OracleError::WebSocketError(e.to_string()))? {
                Message::Text(text) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        // Ticker array: [channelId, {ticker_data}, "ticker", "XBT/USD"]
                        // c[0] = last trade close price
                        if let Some(arr) = v.as_array() {
                            if arr.len() == 4 && arr[2].as_str() == Some("ticker") {
                                if let Some(price) = arr[1]
                                    .get("c")
                                    .and_then(|c| c.as_array())
                                    .and_then(|c| c.first())
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<f64>().ok())
                                {
                                    debug!("Kraken   BTC/USD ${price:.2}");
                                    if sender
                                        .send(ExchangePrice {
                                            exchange: "kraken".to_string(),
                                            price,
                                            timestamp: Utc::now(),
                                            volume: None,
                                        })
                                        .await
                                        .is_err()
                                    {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                }
                Message::Ping(data) => {
                    write
                        .send(Message::Pong(data))
                        .await
                        .map_err(|e| OracleError::WebSocketError(e.to_string()))?;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }

        Ok(())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Extracts a string field from JSON and parses it as f64.
fn parse_f64_field(v: &Value, key: &str) -> Option<f64> {
    v.get(key)?.as_str()?.parse().ok()
}
