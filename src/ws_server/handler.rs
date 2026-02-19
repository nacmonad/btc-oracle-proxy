//! WebSocket message handler

use crate::models::WsMessage;
use crate::error::OracleResult;

/// Handles incoming WebSocket messages from clients
pub async fn handle_message(msg: WsMessage) -> OracleResult<()> {
    match msg {
        WsMessage::Subscribe { channels } => {
            // TODO: Subscribe client to channels
            println!("Subscribe to: {:?}", channels);
            Ok(())
        }
        WsMessage::Unsubscribe { channels } => {
            // TODO: Unsubscribe from channels
            println!("Unsubscribe from: {:?}", channels);
            Ok(())
        }
        _ => Ok(()),
    }
}
