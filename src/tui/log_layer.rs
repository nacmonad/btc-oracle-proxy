//! Custom tracing `Layer` that captures log events into a shared ring buffer
//! so the TUI footer can display them without writing to stdout.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

pub struct TuiLogLayer {
    buffer: Arc<Mutex<VecDeque<String>>>,
    max_lines: usize,
}

impl TuiLogLayer {
    pub fn new(buffer: Arc<Mutex<VecDeque<String>>>, max_lines: usize) -> Self {
        Self { buffer, max_lines }
    }
}

struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

impl<S: Subscriber> Layer<S> for TuiLogLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor {
            message: String::new(),
        };
        event.record(&mut visitor);

        if visitor.message.is_empty() {
            return;
        }

        let level_str = match *event.metadata().level() {
            Level::ERROR => "ERROR",
            Level::WARN => "WARN ",
            Level::INFO => "INFO ",
            Level::DEBUG => "DEBUG",
            Level::TRACE => "TRACE",
        };

        let now = chrono::Utc::now();
        let line = format!(
            "{} [{}] {}",
            now.format("%H:%M:%S"),
            level_str,
            visitor.message
        );

        if let Ok(mut buf) = self.buffer.lock() {
            buf.push_back(line);
            while buf.len() > self.max_lines {
                buf.pop_front();
            }
        }
    }
}
