//! Terminal UI — four-panel layout:
//!   bar    : server status (WS + HTTP)
//!   top    : feed health + per-exchange latency
//!   center : live price summary (market vs chainlink, deviation, per-exchange)
//!   footer : rolling log tail

pub mod log_layer;

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use crossterm::{
    event::{self, DisableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap},
    Frame, Terminal,
};
use tokio::sync::RwLock;

use crate::models::AppState;

pub type LogBuffer = Arc<Mutex<VecDeque<String>>>;

pub fn new_log_buffer() -> LogBuffer {
    Arc::new(Mutex::new(VecDeque::with_capacity(200)))
}

// ---------------------------------------------------------------------------
// Snapshot — cheap clone of the data the TUI needs, taken under a read lock
// ---------------------------------------------------------------------------

struct Snapshot {
    market_price: Option<f64>,
    chainlink_price: Option<f64>,
    chainlink_age_secs: Option<u64>,
    deviation_pct: Option<f64>,
    round_imminent: bool,
    exchange_prices: HashMap<String, f64>,
    /// (name, connected, ms_since_last_update)
    exchange_statuses: Vec<(String, bool, Option<i64>)>,
    history_len: usize,
    last_update: Option<DateTime<Utc>>,
}

impl Snapshot {
    fn from_state(state: &AppState) -> Self {
        let now = Utc::now();

        let (market_price, chainlink_price, chainlink_age_secs, deviation_pct, round_imminent, exchange_prices) =
            if let Some(ref u) = state.current_price {
                (
                    Some(u.market_price),
                    u.chainlink_price,
                    u.chainlink_age_secs,
                    u.deviation_pct,
                    u.round_imminent,
                    u.exchange_prices.clone(),
                )
            } else {
                (None, None, None, None, false, HashMap::new())
            };

        let mut exchange_statuses: Vec<(String, bool, Option<i64>)> = state
            .exchange_status
            .iter()
            .map(|e| {
                let age_ms = e
                    .last_update
                    .map(|t| now.signed_duration_since(t).num_milliseconds());
                (e.exchange.clone(), e.connected, age_ms)
            })
            .collect();
        exchange_statuses.sort_by(|a, b| a.0.cmp(&b.0));

        Self {
            market_price,
            chainlink_price,
            chainlink_age_secs,
            deviation_pct,
            round_imminent,
            exchange_prices,
            exchange_statuses,
            history_len: state.price_history.len(),
            last_update: state.last_update,
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub async fn run_tui(
    state: Arc<RwLock<AppState>>,
    logs: LogBuffer,
    ws_addr: String,
    http_addr: String,
) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal, state, logs, ws_addr, http_addr).await;

    // Always restore the terminal, even on error
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    result
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: Arc<RwLock<AppState>>,
    logs: LogBuffer,
    ws_addr: String,
    http_addr: String,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_millis(100));

    loop {
        ticker.tick().await;

        // Snapshot shared state (brief async read lock)
        let snapshot = {
            let s = state.read().await;
            Snapshot::from_state(&s)
        };

        // Grab the most-recent log lines (newest first)
        let log_lines: Vec<String> = {
            logs.lock()
                .unwrap()
                .iter()
                .rev()
                .take(30)
                .cloned()
                .collect()
        };

        terminal.draw(|f| draw(f, &snapshot, &log_lines, &ws_addr, &http_addr))?;

        // Drain any pending key events (poll with zero timeout = non-blocking)
        while event::poll(Duration::ZERO)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press
                    && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
                {
                    return Ok(());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Drawing
// ---------------------------------------------------------------------------

fn draw(f: &mut Frame, snap: &Snapshot, log_lines: &[String], ws_addr: &str, http_addr: &str) {
    let area = f.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // server status bar
            Constraint::Length(3),  // feed health
            Constraint::Min(10),    // prices
            Constraint::Length(10), // logs footer
        ])
        .split(area);

    draw_server_status(f, chunks[0], ws_addr, http_addr);
    draw_health(f, chunks[1], snap);
    draw_prices(f, chunks[2], snap);
    draw_logs(f, chunks[3], log_lines);
}

// ── Top bar: server status ───────────────────────────────────────────────────

fn draw_server_status(f: &mut Frame, area: Rect, ws_addr: &str, http_addr: &str) {
    // Servers are not yet implemented — always shown as offline.
    // Replace `false` with actual health state once WS/HTTP tasks report status.
    let servers: &[(&str, &str, &str, bool)] = &[
        ("WS", "ws://", ws_addr, false),
        ("HTTP", "http://", http_addr, false),
    ];

    let block = Block::default()
        .title(" Server Status ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut spans: Vec<Span> = Vec::new();

    for (label, scheme, addr, online) in servers {
        let (dot_color, status_color, status_str) = if *online {
            (Color::Green, Color::Green, "online ")
        } else {
            (Color::Red, Color::Red, "offline")
        };

        spans.push(Span::styled("● ", Style::default().fg(dot_color)));
        spans.push(Span::styled(
            format!("{label} "),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::styled(
            format!("{scheme}{addr}  "),
            Style::default().fg(Color::DarkGray),
        ));
        spans.push(Span::styled(
            format!("{status_str}     "),
            Style::default().fg(status_color),
        ));
    }

    f.render_widget(
        Paragraph::new(Line::from(spans)),
        inner,
    );
}

// ── Top: feed health ────────────────────────────────────────────────────────

fn draw_health(f: &mut Frame, area: Rect, snap: &Snapshot) {
    let block = Block::default()
        .title(" Feed Health ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut spans: Vec<Span> = Vec::new();

    for (name, connected, age_ms) in &snap.exchange_statuses {
        let (dot_color, label_color, age_str) = if *connected {
            match age_ms {
                Some(ms) if *ms < 2_000 => (Color::Green, Color::White, format!("{ms}ms")),
                Some(ms) if *ms < 10_000 => {
                    (Color::Yellow, Color::Yellow, format!("{ms}ms"))
                }
                Some(ms) => (Color::Red, Color::Red, format!("{ms}ms stale")),
                None => (Color::DarkGray, Color::DarkGray, "---".to_string()),
            }
        } else {
            (Color::Red, Color::Red, "disconnected".to_string())
        };

        spans.push(Span::styled("● ", Style::default().fg(dot_color)));
        spans.push(Span::styled(
            format!("{name} "),
            Style::default()
                .fg(label_color)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::styled(
            format!("{age_str}    "),
            Style::default().fg(Color::DarkGray),
        ));
    }

    // Chainlink on-chain age
    let (cl_dot, cl_val_style, cl_str) = match snap.chainlink_age_secs {
        Some(age) if age < 30 => (
            Color::Green,
            Style::default().fg(Color::Green),
            format!("{age}s ago"),
        ),
        Some(age) if age < 120 => (
            Color::Yellow,
            Style::default().fg(Color::Yellow),
            format!("{age}s ago"),
        ),
        Some(age) => (
            Color::Red,
            Style::default().fg(Color::Red),
            format!("{age}s ago"),
        ),
        None => (
            Color::DarkGray,
            Style::default().fg(Color::DarkGray),
            "awaiting...".to_string(),
        ),
    };

    spans.push(Span::styled("⬡ ", Style::default().fg(cl_dot)));
    spans.push(Span::styled(
        "Chainlink  ",
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    ));
    spans.push(Span::styled(cl_str, cl_val_style));

    f.render_widget(
        Paragraph::new(Line::from(spans)).alignment(Alignment::Left),
        inner,
    );
}

// ── Center: price summary ───────────────────────────────────────────────────

fn draw_prices(f: &mut Frame, area: Rect, snap: &Snapshot) {
    let outer = Block::default()
        .title(" BTC / USD ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = outer.inner(area);
    f.render_widget(outer, area);

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(inner);

    draw_main_stats(f, cols[0], snap);
    draw_exchange_prices(f, cols[1], snap);
}

fn draw_main_stats(f: &mut Frame, area: Rect, snap: &Snapshot) {
    let mut lines: Vec<Line> = vec![Line::from("")];

    // Market price
    let market_str = snap
        .market_price
        .map(|p| format!("${:>14}", fmt_price(p)))
        .unwrap_or_else(|| "      awaiting...".to_string());
    lines.push(Line::from(vec![
        Span::styled(
            "  Market Price    ",
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            market_str,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
    ]));

    lines.push(Line::from(""));

    // Chainlink price
    let cl_str = snap
        .chainlink_price
        .map(|p| format!("${:>14}", fmt_price(p)))
        .unwrap_or_else(|| "      awaiting...".to_string());
    let cl_age = snap
        .chainlink_age_secs
        .map(|a| format!("  ({a}s ago)"))
        .unwrap_or_default();
    lines.push(Line::from(vec![
        Span::styled(
            "  Chainlink       ",
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(cl_str, Style::default().fg(Color::Cyan)),
        Span::styled(cl_age, Style::default().fg(Color::DarkGray)),
    ]));

    lines.push(Line::from(""));

    // Deviation
    match snap.deviation_pct {
        Some(dev) => {
            let abs_dev = dev.abs();
            let (dev_color, bg) = if abs_dev >= 0.10 {
                (Color::White, Some(Color::Red))
            } else if abs_dev >= 0.07 {
                (Color::Yellow, None)
            } else {
                (Color::Green, None)
            };

            let arrow = if dev > 0.0 { "↑" } else { "↓" };
            let dev_str = format!("{arrow} {:>+.3}%", dev);

            let dev_style = match bg {
                Some(bg_color) => Style::default()
                    .fg(dev_color)
                    .bg(bg_color)
                    .add_modifier(Modifier::BOLD),
                None => Style::default()
                    .fg(dev_color)
                    .add_modifier(Modifier::BOLD),
            };

            let mut row = vec![
                Span::styled(
                    "  Deviation       ",
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(format!("{dev_str:>16}"), dev_style),
            ];

            if snap.round_imminent {
                row.push(Span::styled(
                    "   ⚡ ROUND IMMINENT",
                    Style::default()
                        .fg(Color::White)
                        .bg(Color::Red)
                        .add_modifier(Modifier::BOLD),
                ));
            }

            lines.push(Line::from(row));
        }
        None => {
            lines.push(Line::from(vec![
                Span::styled(
                    "  Deviation       ",
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled("      awaiting...", Style::default().fg(Color::DarkGray)),
            ]));
        }
    }

    lines.push(Line::from(""));

    // History depth
    let freshness = snap
        .last_update
        .map(|t| {
            let ms = Utc::now()
                .signed_duration_since(t)
                .num_milliseconds();
            format!("last tick {ms}ms ago")
        })
        .unwrap_or_else(|| "no updates yet".to_string());

    lines.push(Line::from(vec![
        Span::styled(
            "  History         ",
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            format!("{} pts  ", snap.history_len),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(freshness, Style::default().fg(Color::DarkGray)),
    ]));

    f.render_widget(Paragraph::new(lines), area);
}

fn draw_exchange_prices(f: &mut Frame, area: Rect, snap: &Snapshot) {
    let block = Block::default()
        .title(" Exchange Prices ")
        .borders(Borders::LEFT)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    f.render_widget(block, area);

    if snap.exchange_prices.is_empty() {
        f.render_widget(
            Paragraph::new("  awaiting feeds...")
                .style(Style::default().fg(Color::DarkGray)),
            inner,
        );
        return;
    }

    let mut rows: Vec<Row> = vec![Row::new(vec![Cell::from("")])]; // top padding

    let mut sorted: Vec<(&String, &f64)> = snap.exchange_prices.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));

    for (name, price) in sorted {
        let cap_name = {
            let mut s = name.clone();
            if let Some(c) = s.get_mut(0..1) {
                c.make_ascii_uppercase();
            }
            s
        };
        rows.push(Row::new(vec![
            Cell::from(format!("  {cap_name}"))
                .style(Style::default().fg(Color::DarkGray)),
            Cell::from(format!("${:>12}", fmt_price(*price))).style(
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
        ]));
    }

    let table = Table::new(
        rows,
        [Constraint::Percentage(38), Constraint::Percentage(62)],
    );
    f.render_widget(table, inner);
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Format a price with thousands separators: 66524.70 → "66,524.70"
fn fmt_price(p: f64) -> String {
    let s = format!("{:.2}", p);
    let mut parts = s.splitn(2, '.');
    let int_part = parts.next().unwrap_or("0");
    let dec_part = parts.next().unwrap_or("00");

    let with_commas: String = int_part
        .chars()
        .rev()
        .enumerate()
        .flat_map(|(i, c)| {
            if i > 0 && i % 3 == 0 {
                vec![',', c]
            } else {
                vec![c]
            }
        })
        .collect::<String>()
        .chars()
        .rev()
        .collect();

    format!("{with_commas}.{dec_part}")
}

// ── Footer: logs ─────────────────────────────────────────────────────────────

fn draw_logs(f: &mut Frame, area: Rect, log_lines: &[String]) {
    let block = Block::default()
        .title(" Logs   [q] quit ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let lines: Vec<Line> = log_lines
        .iter()
        .map(|l| {
            let color = if l.contains("[ERROR]") {
                Color::Red
            } else if l.contains("[WARN ]") || l.contains('⚡') {
                Color::Yellow
            } else {
                Color::DarkGray
            };
            Line::from(Span::styled(l.as_str(), Style::default().fg(color)))
        })
        .collect();

    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), inner);
}
