//! Custom error types for OracleProxy

use thiserror::Error;

#[derive(Error, Debug)]
pub enum OracleError {
    #[error("Exchange connection error: {0}")]
    ExchangeConnectionError(String),

    #[error("Invalid price data: {0}")]
    InvalidPriceData(String),

    #[error("Aggregation error: {0}")]
    AggregationError(String),

    #[error("Indicator calculation error: {0}")]
    IndicatorError(String),

    #[error("WebSocket error: {0}")]
    WebSocketError(String),

    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Data store error: {0}")]
    StoreError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

pub type OracleResult<T> = Result<T, OracleError>;
