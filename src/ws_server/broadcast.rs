//! Broadcast channel type aliases for WsEvent distribution.

use crate::models::WsEvent;
use tokio::sync::broadcast;

/// Capacity chosen to absorb bursts while keeping memory bounded.
/// At ~500ms ticks + occasional signal events, 1024 is ~8 minutes of backlog.
pub const CHANNEL_CAPACITY: usize = 1024;

pub type EventSender = broadcast::Sender<WsEvent>;
pub type EventReceiver = broadcast::Receiver<WsEvent>;

pub fn new_channel() -> (EventSender, EventReceiver) {
    broadcast::channel(CHANNEL_CAPACITY)
}
