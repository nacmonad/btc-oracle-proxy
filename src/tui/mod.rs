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
use std::time::{Duration, Instant};

use duckdb::Connection;

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

struct ClobSummary {
    timeframe: String,
    tokens: i64,
    avg_spread: Option<f64>,
    avg_imb5: Option<f64>,
    avg_slip100: Option<f64>,
    last_ts: Option<String>,
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

fn clob_db_path() -> String {
    std::env::var("RESEARCHER_DB_PATH")
        .or_else(|_| std::env::var("DB_PATH"))
        .unwrap_or_else(|_| "../data/researcher.db".to_string())
}

fn fetch_clob_summary() -> Vec<ClobSummary> {
    let mut out = Vec::new();
    let db_path = clob_db_path();

    let conn = match Connection::open(db_path) {
        Ok(c) => c,
        Err(_) => return out,
    };

    let sql = r#"
        WITH latest AS (
            SELECT
                ps.*,
                pm.timeframe,
                ROW_NUMBER() OVER (PARTITION BY ps.token_id ORDER BY ps.ts DESC) AS rn
            FROM pm_snapshots ps
            JOIN pm_markets pm ON pm.condition_id = ps.condition_id
            WHERE pm.asset = 'BTC'
              AND pm.timeframe IN ('5m', '15m')
        )
        SELECT
            timeframe,
            COUNT(*) AS tokens,
            AVG(spread) AS avg_spread,
            AVG(depth_imbalance_5) AS avg_imb5,
            AVG(slippage_100) AS avg_slip100,
            CAST(MAX(ts) AS VARCHAR) AS last_ts
        FROM latest
        WHERE rn = 1
        GROUP BY timeframe
        ORDER BY timeframe
    "#;

    if let Ok(mut stmt) = conn.prepare(sql) {
        if let Ok(mut rows) = stmt.query([]) {
            while let Ok(Some(r)) = rows.next() {
                out.push(ClobSummary {
                    timeframe: r.get::<usize, String>(0).unwrap_or_else(|_| "?".to_string()),
                    tokens: r.get::<usize, i64>(1).unwrap_or(0),
                    avg_spread: r.get::<usize, Option<f64>>(2).unwrap_or(None),
                    avg_imb5: r.get::<usize, Option<f64>>(3).unwrap_or(None),
                    avg_slip100: r.get::<usize, Option<f64>>(4).unwrap_or(None),
                    last_ts: r.get::<usize, Option<String>>(5).unwrap_or(None),
                });
            }
        }
    }

    out
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub async fn run_tui(
    state: Arc<RwLock<AppState>>,
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

    let result = run_loop(&mut terminal, state, logs, ws_addr, ws_online, ws_clients).await;

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
    ws_online: Arc<AtomicBool>,
    ws_clients: Arc<AtomicUsize>,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    let mut page = Page::Oracle;
    let mut clob_summary_cache: Vec<ClobSummary> = Vec::new();
    let mut last_clob_refresh = Instant::now() - Duration::from_secs(10);

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

        if matches!(page, Page::Clob) && last_clob_refresh.elapsed() >= Duration::from_secs(1) {
            clob_summary_cache = fetch_clob_summary();
            last_clob_refresh = Instant::now();
        }

        terminal.draw(|f| {
            draw(
                f,
                &snapshot,
                &clob_summary_cache,
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
    clob_summary: &[ClobSummary],
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
        Page::Clob => draw_clob_page(f, chunks[2], clob_summary),
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

fn draw_clob_page(f: &mut Frame, area: Rect, summary: &[ClobSummary]) {
    let outer = Block::default()
        .title(" CLOB / L2 (BTC 5m & 15m) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = outer.inner(area);
    f.render_widget(outer, area);

    if summary.is_empty() {
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

    let header = Row::new(vec![
        Cell::from("Timeframe"),
        Cell::from("Tokens"),
        Cell::from("Avg Spread"),
        Cell::from("Avg Imb(5)"),
        Cell::from("Avg Slip $100"),
        Cell::from("Last TS"),
    ])
    .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = summary
        .iter()
        .map(|s| {
            Row::new(vec![
                Cell::from(format!("{}", s.timeframe)),
                Cell::from(format!("{}", s.tokens)),
                Cell::from(format!("{}", s.avg_spread.map(|v| format!("{v:.4}" )).unwrap_or_else(|| "-".into()))),
                Cell::from(format!("{}", s.avg_imb5.map(|v| format!("{v:+.4}" )).unwrap_or_else(|| "-".into()))),
                Cell::from(format!("{}", s.avg_slip100.map(|v| format!("{v:.4}" )).unwrap_or_else(|| "-".into()))),
                Cell::from(s.last_ts.clone().unwrap_or_else(|| "-".into())),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(14),
            Constraint::Min(20),
        ],
    )
    .header(header)
    .column_spacing(2)
    .block(Block::default().borders(Borders::NONE));

    f.render_widget(table, inner);
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
