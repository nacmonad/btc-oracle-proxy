//! Configuration management for OracleProxy

use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub ws_listen_addr: String,
    pub http_listen_addr: String,
    pub log_level: String,
    
    // Exchange configuration
    pub binance_ws_url: String,
    pub coinbase_ws_url: String,
    pub kraken_ws_url: String,

    // Polygon JSON-RPC — used to read Chainlink BTC/USD contract on-chain
    pub polygon_rpc_url: String,
    
    // Indicator settings
    pub ema_short_period: usize,
    pub ema_long_period: usize,
    pub rsi_period: usize,
    pub macd_fast_period: usize,
    pub macd_slow_period: usize,
    pub macd_signal_period: usize,
    pub volatility_period: usize,
    
    // Performance tuning
    pub price_history_capacity: usize,
    pub aggregation_interval_ms: u64,
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        Ok(Config {
            ws_listen_addr: env::var("WS_LISTEN_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8080".to_string()),
            http_listen_addr: env::var("HTTP_LISTEN_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8081".to_string()),
            log_level: env::var("LOG_LEVEL")
                .unwrap_or_else(|_| "info".to_string()),
            
            binance_ws_url: env::var("BINANCE_WS_URL")
                .unwrap_or_else(|_| "wss://stream.binance.com:9443/ws".to_string()),
            coinbase_ws_url: env::var("COINBASE_WS_URL")
                .unwrap_or_else(|_| "wss://ws-feed.exchange.coinbase.com".to_string()),
            kraken_ws_url: env::var("KRAKEN_WS_URL")
                .unwrap_or_else(|_| "wss://ws.kraken.com/".to_string()),
            polygon_rpc_url: env::var("POLYGON_RPC_URL")
                .unwrap_or_else(|_| "https://polygon-rpc.com".to_string()),

            ema_short_period: 12,
            ema_long_period: 26,
            rsi_period: 14,
            macd_fast_period: 12,
            macd_slow_period: 26,
            macd_signal_period: 9,
            volatility_period: 20,
            
            price_history_capacity: 1000,
            aggregation_interval_ms: 500,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = Config::load().unwrap();
        assert_eq!(config.ema_short_period, 12);
        assert_eq!(config.rsi_period, 14);
    }
}
