//! WebSocket message handler

use crate::models::WsClientMessage;
use crate::error::OracleResult;

/// Handles inbound client → server messages.
pub async fn handle_message(msg: WsClientMessage) -> OracleResult<()> {
    match msg {
        WsClientMessage::Subscribe { channels, assets, timeframes } => {
            tracing::debug!("Subscribe request channels={:?} assets={:?} timeframes={:?}", channels, assets, timeframes);
            Ok(())
        }
        WsClientMessage::Unsubscribe { channels, assets, timeframes } => {
            tracing::debug!("Unsubscribe request channels={:?} assets={:?} timeframes={:?}", channels, assets, timeframes);
            Ok(())
        }
    }
}
