//! In-memory price storage using DashMap

use crate::models::PriceUpdate;
use crate::error::OracleResult;
use dashmap::DashMap;
use std::sync::Arc;

pub struct InMemoryPriceStore {
    data: Arc<DashMap<String, PriceUpdate>>,
}

impl InMemoryPriceStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
        }
    }

    pub fn store_price(&self, price: PriceUpdate) -> OracleResult<()> {
        let key = format!("{}-{}", price.symbol, price.timestamp);
        self.data.insert(key, price);
        Ok(())
    }

    pub fn get_latest(&self) -> OracleResult<Option<PriceUpdate>> {
        // Get most recent price
        if let Some(entry) = self.data.iter().next() {
            Ok(Some(entry.value().clone()))
        } else {
            Ok(None)
        }
    }
}
