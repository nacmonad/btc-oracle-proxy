//! Real-time data storage and history management

pub mod price_store;
pub mod history;

use crate::models::PriceUpdate;
use crate::error::OracleResult;

/// Trait for price storage
pub trait PriceStore: Send + Sync {
    fn store_price(&self, price: PriceUpdate) -> OracleResult<()>;
    fn get_latest(&self) -> OracleResult<Option<PriceUpdate>>;
    fn get_history(&self, limit: usize) -> OracleResult<Vec<PriceUpdate>>;
    fn clear_old(&self, keep_count: usize) -> OracleResult<()>;
}
