//! WebSocket message handler

use crate::models::WsClientMessage;
use crate::error::OracleResult;

/// Handles inbound client → server messages.
pub async fn handle_message(msg: WsClientMessage) -> OracleResult<()> {
    match msg {
        WsClientMessage::Subscribe { channels } => {
            // TODO: register client subscription
            tracing::debug!("Subscribe request: {:?}", channels);
            Ok(())
        }
        WsClientMessage::Unsubscribe { channels } => {
            // TODO: remove client subscription
            tracing::debug!("Unsubscribe request: {:?}", channels);
            Ok(())
        }
    }
}
