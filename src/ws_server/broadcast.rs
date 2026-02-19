//! Broadcasting price updates to connected WebSocket clients

use crate::models::PriceUpdate;
use crate::error::OracleResult;
use tokio::sync::broadcast;

pub struct PriceBroadcaster {
    tx: broadcast::Sender<PriceUpdate>,
}

impl PriceBroadcaster {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(100);
        Self { tx }
    }

    pub fn broadcast_price(&self, price: PriceUpdate) -> OracleResult<()> {
        // Ignore if no subscribers
        let _ = self.tx.send(price);
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PriceUpdate> {
        self.tx.subscribe()
    }
}
