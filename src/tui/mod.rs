//! Terminal UI — four-panel layout:
//!   bar    : server status (WS + HTTP)
//!   top    : feed health + per-exchange latency
//!   center : live price summary (market vs chainlink, deviation, per-exchange)
//!   footer : rolling log tail

pub mod log_layer;

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

use crate::clob::ClobUiState;
use crate::models::{AppState, IndicatorValues};

pub type LogBuffer = Arc<Mutex<VecDeque<String>>>;

pub fn new_log_buffer() -> LogBuffer {
    Arc::new(Mutex::new(VecDeque::with_capacity(200)))
}

// ---------------------------------------------------------------------------
// Snapshot — cheap clone of the data the TUI needs, taken under a read lock
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Page {
    Oracle,
    Clob,
}

#[derive(Clone, Default)]
struct ClobSideView {
    token_id: String,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    spread: Option<f64>,
    bid_depth5: Option<f64>,
    ask_depth5: Option<f64>,
    imbalance5: Option<f64>,
    slip100: Option<f64>,
    updated_at: String,
}

#[derive(Clone, Default)]
struct ClobMarketRow {
    close_time: String,
    asset: String,
    timeframe: String,
    condition_id: String,
    up: Option<ClobSideView>,
    down: Option<ClobSideView>,
}

struct Snapshot {
    market_price: Option<f64>,
    chainlink_price: Option<f64>,
    chainlink_age_secs: Option<u64>,
    deviation_pct: Option<f64>,
    round_imminent: bool,
    exchange_prices: HashMap<String, f64>,
    /// (name, connected, ms_since_last_update)
    exchange_statuses: Vec<(String, bool, Option<i64>)>,
    indicators: IndicatorValues,
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

        let indicators = state
            .current_price
            .as_ref()
            .map(|u| u.indicators.clone())
            .unwrap_or_default();

        Self {
            market_price,
            chainlink_price,
            chainlink_age_secs,
            deviation_pct,
            round_imminent,
            exchange_prices,
            exchange_statuses,
            indicators,
            history_len: state.price_history.len(),
            last_update: state.last_update,
        }
    }
}

fn format_close_time_compact(s: &str) -> String {
    // Example input: 2026-02-28 01:15:00+00  -> 2026-02-28 01:15
    if s.len() >= 16 {
        s[..16].to_string()
    } else {
        s.to_string()
    }
}

fn timeframe_rank_minutes(tf: &str) -> i64 {
    let t = tf.trim().to_lowercase();
    if let Some(n) = t.strip_suffix('m').and_then(|x| x.parse::<i64>().ok()) {
        return n;
    }
    if let Some(n) = t.strip_suffix('h').and_then(|x| x.parse::<i64>().ok()) {
        return n * 60;
    }
    if let Some(n) = t.strip_suffix('d').and_then(|x| x.parse::<i64>().ok()) {
        return n * 24 * 60;
    }
    i64::MAX / 2
}

fn build_clob_market_rows(state: &ClobUiState) -> Vec<ClobMarketRow> {
    use std::collections::HashMap;
    let mut by_market: HashMap<String, ClobMarketRow> = HashMap::new();

    for (token_id, tok) in state.tokens.iter() {
        let meta = state.markets.get(token_id);
        let timeframe = meta.map(|m| m.timeframe.clone()).unwrap_or_else(|| tok.timeframe.clone());
        if timeframe != "5m" && timeframe != "15m" { continue; }
        let close_time_raw = meta.map(|m| m.close_time.clone()).unwrap_or_else(|| "?".to_string());
        let close_time = format_close_time_compact(&close_time_raw);
        let asset = meta.map(|m| m.asset.clone()).unwrap_or_else(|| "?".to_string());
        let condition_id = meta.map(|m| m.condition_id.clone()).unwrap_or_else(|| tok.condition_id.clone());
        let side = meta.map(|m| m.side.clone()).unwrap_or_else(|| "?".to_string());

        let sv = ClobSideView {
            token_id: token_id.clone(),
            best_bid: tok.best_bid,
            best_ask: tok.best_ask,
            spread: tok.spread,
            bid_depth5: tok.bid_depth_5,
            ask_depth5: tok.ask_depth_5,
            imbalance5: tok.depth_imbalance_5,
            slip100: tok.slippage_100,
            updated_at: tok.updated_at.to_rfc3339(),
        };

        let e = by_market.entry(condition_id.clone()).or_insert_with(|| ClobMarketRow {
            close_time: close_time.clone(),
            asset: asset.clone(),
            timeframe: timeframe.clone(),
            condition_id: condition_id.clone(),
            up: None,
            down: None,
        });

        if side == "UP" {
            e.up = Some(sv);
        } else if side == "DOWN" {
            e.down = Some(sv);
        }
    }

    let mut rows: Vec<ClobMarketRow> = by_market.into_values().collect();
    rows.sort_by(|a, b| {
        a.close_time.cmp(&b.close_time)
            .then(timeframe_rank_minutes(&a.timeframe).cmp(&timeframe_rank_minutes(&b.timeframe)))
            .then(a.condition_id.cmp(&b.condition_id))
    });
    rows
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub async fn run_tui(
    state: Arc<RwLock<AppState>>,
    clob_state: Arc<RwLock<ClobUiState>>,
    logs: LogBuffer,
    ws_addr: String,
    ws_online: Arc<AtomicBool>,
    ws_clients: Arc<AtomicUsize>,
) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal, state, clob_state, logs, ws_addr, ws_online, ws_clients).await;

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
    clob_state: Arc<RwLock<ClobUiState>>,
    logs: LogBuffer,
    ws_addr: String,
    ws_online: Arc<AtomicBool>,
    ws_clients: Arc<AtomicUsize>,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    let mut page = Page::Oracle;
    let mut clob_rows_cache: Vec<ClobMarketRow> = Vec::new();
    let mut clob_selected: usize = 0;

    loop {
        ticker.tick().await;

        let snapshot = {
            let s = state.read().await;
            Snapshot::from_state(&s)
        };

        let log_lines: Vec<String> = {
            logs.lock()
                .unwrap()
                .iter()
                .rev()
                .take(30)
                .cloned()
                .collect()
        };

        let online = ws_online.load(Ordering::Relaxed);
        let clients = ws_clients.load(Ordering::Relaxed);

        if matches!(page, Page::Clob) {
            let cs = clob_state.read().await;
            clob_rows_cache = build_clob_market_rows(&cs);
            if clob_selected >= clob_rows_cache.len() {
                clob_selected = clob_rows_cache.len().saturating_sub(1);
            }
        }

        terminal.draw(|f| {
            draw(
                f,
                &snapshot,
                &clob_rows_cache,
                clob_selected,
                &log_lines,
                &ws_addr,
                online,
                clients,
                page,
            )
        })?;

        // Drain any pending key events (poll with zero timeout = non-blocking)
        while event::poll(Duration::ZERO)? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    KeyCode::Tab => {
                        page = match page {
                            Page::Oracle => Page::Clob,
                            Page::Clob => Page::Oracle,
                        }
                    }
                    KeyCode::Char('1') => page = Page::Oracle,
                    KeyCode::Char('2') => page = Page::Clob,
                    KeyCode::Up if matches!(page, Page::Clob) => {
                        clob_selected = clob_selected.saturating_sub(1);
                    }
                    KeyCode::Down if matches!(page, Page::Clob) => {
                        if !clob_rows_cache.is_empty() {
                            clob_selected = (clob_selected + 1).min(clob_rows_cache.len() - 1);
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Drawing
// ---------------------------------------------------------------------------

fn draw(
    f: &mut Frame,
    snap: &Snapshot,
    clob_rows: &[ClobMarketRow],
    clob_selected: usize,
    log_lines: &[String],
    ws_addr: &str,
    ws_online: bool,
    ws_clients: usize,
    page: Page,
) {
    let area = f.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // server status bar
            Constraint::Length(3),  // feed health
            Constraint::Min(10),    // main panel
            Constraint::Length(10), // logs footer
        ])
        .split(area);

    draw_server_status(f, chunks[0], ws_addr, ws_online, ws_clients, page);
    draw_health(f, chunks[1], snap);
    match page {
        Page::Oracle => draw_prices(f, chunks[2], snap),
        Page::Clob => draw_clob_page(f, chunks[2], clob_rows, clob_selected),
    }
    draw_logs(f, chunks[3], log_lines);
}

// ── Top bar: server status ───────────────────────────────────────────────────

fn draw_server_status(
    f: &mut Frame,
    area: Rect,
    ws_addr: &str,
    ws_online: bool,
    ws_clients: usize,
    page: Page,
) {
    let block = Block::default()
        .title(" Server Status ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let (dot_color, status_str) = if ws_online {
        (Color::Green, "online".to_string())
    } else {
        (Color::Red, "offline".to_string())
    };

    let client_str = if ws_online {
        format!("  ({ws_clients} client{})", if ws_clients == 1 { "" } else { "s" })
    } else {
        String::new()
    };

    let page_label = match page {
        Page::Oracle => "Page 1: Oracle",
        Page::Clob => "Page 2: CLOB/L2",
    };

    let spans = vec![
        Span::styled("● ", Style::default().fg(dot_color)),
        Span::styled(
            "WS ",
            Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("ws://{ws_addr}  "),
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(status_str, Style::default().fg(dot_color)),
        Span::styled(client_str, Style::default().fg(Color::DarkGray)),
        Span::styled("    ", Style::default().fg(Color::DarkGray)),
        Span::styled(page_label, Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::styled("   [1] Oracle  [2] CLOB  [Tab] switch  [q] quit", Style::default().fg(Color::DarkGray)),
    ];

    f.render_widget(Paragraph::new(Line::from(spans)), inner);
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
        .constraints([
            Constraint::Percentage(40), // market price / chainlink / deviation
            Constraint::Percentage(25), // exchange prices
            Constraint::Percentage(35), // indicators
        ])
        .split(inner);

    draw_main_stats(f, cols[0], snap);
    draw_exchange_prices(f, cols[1], snap);
    draw_indicators(f, cols[2], snap);
}

fn make_bar(v: f64, max_v: f64, width: usize, ch: char) -> String {
    if max_v <= 0.0 || width == 0 { return "".to_string(); }
    let n = ((v / max_v) * width as f64).round() as usize;
    std::iter::repeat(ch).take(n.min(width)).collect::<String>()
}

fn draw_clob_page(f: &mut Frame, area: Rect, rows: &[ClobMarketRow], selected: usize) {
    let outer = Block::default()
        .title(" CLOB / L2 (BTC 5m & 15m) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = outer.inner(area);
    f.render_widget(outer, area);

    if rows.is_empty() {
        f.render_widget(
            Paragraph::new(vec![
                Line::from(""),
                Line::from(Span::styled("  No BTC 5m/15m CLOB rows yet.", Style::default().fg(Color::DarkGray))),
                Line::from(Span::styled("  Check pm_markets discovery + CLOB writer task.", Style::default().fg(Color::DarkGray))),
            ]),
            inner,
        );
        return;
    }

    let split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(inner);

    let header = Row::new(vec![
        Cell::from("Close"),
        Cell::from("Tok"),
        Cell::from("TF"),
        Cell::from("Cond"),
        Cell::from("UP bid/ask"),
        Cell::from("DOWN bid/ask"),
    ])
    .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));

    let table_rows: Vec<Row> = rows.iter().enumerate().map(|(i, r)| {
        let style = if i == selected {
            Style::default().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::White)
        };
        let up = r.up.as_ref().map(|s| {
            format!("{}/{}",
                s.best_bid.map(|v| format!("{v:.3}")).unwrap_or_else(|| "-".into()),
                s.best_ask.map(|v| format!("{v:.3}")).unwrap_or_else(|| "-".into()))
        }).unwrap_or_else(|| "-/-".into());
        let down = r.down.as_ref().map(|s| {
            format!("{}/{}",
                s.best_bid.map(|v| format!("{v:.3}")).unwrap_or_else(|| "-".into()),
                s.best_ask.map(|v| format!("{v:.3}")).unwrap_or_else(|| "-".into()))
        }).unwrap_or_else(|| "-/-".into());

        Row::new(vec![
            Cell::from(r.close_time.clone()),
            Cell::from(r.asset.clone()),
            Cell::from(r.timeframe.clone()),
            Cell::from(r.condition_id.chars().take(10).collect::<String>()),
            Cell::from(up),
            Cell::from(down),
        ]).style(style)
    }).collect();

    let table = Table::new(
        table_rows,
        [
            Constraint::Length(16),
            Constraint::Length(4),
            Constraint::Length(5),
            Constraint::Length(12),
            Constraint::Length(14),
            Constraint::Length(14),
        ],
    )
    .header(header)
    .column_spacing(1)
    .block(Block::default().title("Markets (↑/↓)").borders(Borders::ALL));
    f.render_widget(table, split[0]);

    let sel = &rows[selected.min(rows.len()-1)];
    let up = sel.up.clone().unwrap_or_default();
    let down = sel.down.clone().unwrap_or_default();

    let up_bid5 = up.bid_depth5.unwrap_or(0.0);
    let up_ask5 = up.ask_depth5.unwrap_or(0.0);
    let up_max = up_bid5.max(up_ask5).max(1.0);

    let dn_bid5 = down.bid_depth5.unwrap_or(0.0);
    let dn_ask5 = down.ask_depth5.unwrap_or(0.0);
    let dn_max = dn_bid5.max(dn_ask5).max(1.0);

    let detail = Paragraph::new(vec![
        Line::from(Span::styled("Selected Market", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))),
        Line::from(format!("close: {}", sel.close_time)),
        Line::from(format!("asset: {}", sel.asset)),
        Line::from(format!("tf: {}", sel.timeframe)),
        Line::from(format!("condition: {}", sel.condition_id)),
        Line::from(""),
        Line::from(Span::styled("UP token", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))),
        Line::from(format!("id: {}", up.token_id)),
        Line::from(format!("bid/ask: {}/{}",
            up.best_bid.map(|v| format!("{v:.4}")).unwrap_or_else(|| "-".into()),
            up.best_ask.map(|v| format!("{v:.4}")).unwrap_or_else(|| "-".into()))),
        Line::from(format!("spread {}  imb5 {}  slip$100 {}",
            up.spread.map(|v| format!("{v:.4}")).unwrap_or_else(|| "-".into()),
            up.imbalance5.map(|v| format!("{v:+.4}")).unwrap_or_else(|| "-".into()),
            up.slip100.map(|v| format!("{v:.4}")).unwrap_or_else(|| "-".into()))),
        Line::from(Span::styled(format!("UP BID d5 {:>9.2} {}", up_bid5, make_bar(up_bid5, up_max, 18, '█')), Style::default().fg(Color::Green))),
        Line::from(Span::styled(format!("UP ASK d5 {:>9.2} {}", up_ask5, make_bar(up_ask5, up_max, 18, '█')), Style::default().fg(Color::Red))),
        Line::from(""),
        Line::from(Span::styled("DOWN token", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))),
        Line::from(format!("id: {}", down.token_id)),
        Line::from(format!("bid/ask: {}/{}",
            down.best_bid.map(|v| format!("{v:.4}")).unwrap_or_else(|| "-".into()),
            down.best_ask.map(|v| format!("{v:.4}")).unwrap_or_else(|| "-".into()))),
        Line::from(format!("spread {}  imb5 {}  slip$100 {}",
            down.spread.map(|v| format!("{v:.4}")).unwrap_or_else(|| "-".into()),
            down.imbalance5.map(|v| format!("{v:+.4}")).unwrap_or_else(|| "-".into()),
            down.slip100.map(|v| format!("{v:.4}")).unwrap_or_else(|| "-".into()))),
        Line::from(Span::styled(format!("DN BID d5 {:>9.2} {}", dn_bid5, make_bar(dn_bid5, dn_max, 18, '█')), Style::default().fg(Color::Green))),
        Line::from(Span::styled(format!("DN ASK d5 {:>9.2} {}", dn_ask5, make_bar(dn_ask5, dn_max, 18, '█')), Style::default().fg(Color::Red))),
        Line::from(format!("updated up={} dn={}", up.updated_at, down.updated_at)),
    ])
    .block(Block::default().title("Details (UP/DOWN)").borders(Borders::ALL));
    f.render_widget(detail, split[1]);
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

fn draw_indicators(f: &mut Frame, area: Rect, snap: &Snapshot) {
    let block = Block::default()
        .title(" Indicators ")
        .borders(Borders::LEFT)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let ind = &snap.indicators;
    let waiting = Span::styled("  —", Style::default().fg(Color::DarkGray));

    // Helper: format an optional f64 with given decimal places
    let fmt_opt = |v: Option<f64>, decimals: usize| -> Span<'static> {
        match v {
            Some(n) => Span::styled(
                format!("{n:>10.*}", decimals),
                Style::default().fg(Color::White),
            ),
            None => waiting.clone(),
        }
    };

    // RSI color: overbought >70 red, oversold <30 green, else white
    let rsi_span = match ind.rsi_14 {
        Some(r) => {
            let color = if r >= 70.0 {
                Color::Red
            } else if r <= 30.0 {
                Color::Green
            } else {
                Color::White
            };
            Span::styled(format!("{r:>10.1}"), Style::default().fg(color))
        }
        None => waiting.clone(),
    };

    // MACD histogram color: positive green, negative red
    let hist_span = match ind.macd_histogram {
        Some(h) => {
            let color = if h > 0.0 { Color::Green } else { Color::Red };
            Span::styled(format!("{h:>+10.2}"), Style::default().fg(color))
        }
        None => waiting.clone(),
    };

    // ROC color: positive green, negative red
    let roc_span = |v: Option<f64>| -> Span<'static> {
        match v {
            Some(r) => {
                let color = if r > 0.0 { Color::Green } else { Color::Red };
                Span::styled(format!("{r:>+10.3}%"), Style::default().fg(color))
            }
            None => waiting.clone(),
        }
    };

    let label = |s: &'static str| {
        Span::styled(s, Style::default().fg(Color::DarkGray))
    };

    let rows: Vec<Line> = vec![
        Line::from(""),
        Line::from(vec![label("  EMA 12   "), fmt_opt(ind.ema_12.map(|v| v), 2)]),
        Line::from(vec![label("  EMA 26   "), fmt_opt(ind.ema_26, 2)]),
        Line::from(vec![label("  EMA 50   "), fmt_opt(ind.ema_50, 2)]),
        Line::from(""),
        Line::from(vec![label("  RSI 14   "), rsi_span]),
        Line::from(vec![label("  ROC 10   "), roc_span(ind.momentum_10)]),
        Line::from(vec![label("  ROC 20   "), roc_span(ind.momentum_20)]),
        Line::from(""),
        Line::from(vec![label("  StdDev   "), fmt_opt(ind.volatility, 2)]),
        Line::from(vec![label("  BB Upper "), fmt_opt(ind.bb_upper, 2)]),
        Line::from(vec![label("  BB Mid   "), fmt_opt(ind.bb_middle, 2)]),
        Line::from(vec![label("  BB Lower "), fmt_opt(ind.bb_lower, 2)]),
        Line::from(""),
        Line::from(vec![label("  MACD     "), fmt_opt(ind.macd, 2)]),
        Line::from(vec![label("  Signal   "), fmt_opt(ind.macd_signal, 2)]),
        Line::from(vec![label("  Hist     "), hist_span]),
    ];

    f.render_widget(Paragraph::new(rows), inner);
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
