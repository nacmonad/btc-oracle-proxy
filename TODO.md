# OracleProxy - Chainlink Round Trigger Detector

**Goal:** Build a Rust service that aggregates BTC prices from major exchanges, tracks the last committed Chainlink on-chain price, detects when the 0.1% deviation threshold is crossed (triggering an OCR2 round), and exposes a WebSocket feed for Python trading bot integration.

The system does **not** need to replicate Chainlink's exact price — it needs to reliably detect meaningful impulses that will force a Chainlink price update, giving the bot a 10-60 second edge before the new price is committed on-chain and Polymarket settles.

---

## RESEARCH FINDINGS

### Polymarket's Oracle Architecture

#### Current Setup (Sept 2025)
- **Partnership:** Chainlink Data Streams + Chainlink Automation
- **Network:** Polygon mainnet (primary deployment)
- **Data Source:** Polymarket RTDS (Real-Time Data Service)
- **Status:** Live for price-based markets (crypto pairs, assets)

#### Two Data Sources Available
1. **Binance Source (`crypto_prices`)**
   - Real-time price data from Binance exchange
   - Symbol format: lowercase concatenated (e.g., `btcusdt`, `ethusdt`)
   - No authentication required
   - Updates: As market prices change

2. **Chainlink Source (`crypto_prices_chainlink`)**
   - Chainlink oracle network aggregation
   - Symbol format: slash-separated (e.g., `btc/usd`, `eth/usd`)
   - No authentication required
   - Updates: 0.1% deviation threshold + periodic heartbeats

### Chainlink BTC/USD Feed on Polygon (Mainnet)

#### Product Details
- **Product Name:** BTC/USD-RefPrice-DF-Matic-001
- **Product Type:** Price Feed (Reference Price)
- **Base Asset:** BTC_CR (CryptoCompare composite — Chainlink nodes pull from CryptoCompare CCCAGG)
- **Quote Asset:** USD_FX
- **Network:** Polygon mainnet
- **Tier:** Low Market Risk (high confidence)

#### Price Update Parameters
- **Deviation Threshold:** 0.1% (triggers immediate OCR2 round)
- **Heartbeat:** Periodic updates even without deviation (up to 24h for BTC/USD)
- **Decimals:** 8 (prices in wei, e.g., 67234.50 = 6723450000000)

#### How Chainlink Aggregates (OCR2)
1. Each of 19+ node operators independently fetches from their configured data providers (primarily CryptoCompare CCCAGG, Kaiko, Brave New Coin)
2. Nodes gossip their observations off-chain via P2P
3. A leader node sorts all observations and proposes the **median**
4. Other nodes co-sign the median
5. One transaction is submitted on-chain with the aggregated answer

**Key points:**
- Aggregation is pure **median** — there is no explicit 2-sigma or IQR outlier drop
- Outlier resistance comes from the median itself (robust to up to 1/3 corrupt submissions)
- Each node's data provider (CCCAGG) does its own percentage-band exchange exclusion upstream
- `minAnswer`/`maxAnswer` circuit breakers in the aggregator contract reject extreme submissions
- Total on-chain lag from a spot move: **~15-60 seconds** (OCR2 consensus + Polygon block)

#### What This Means For Us
We don't need to replicate Chainlink's internal node architecture. We need to:
1. Know the **last committed Chainlink price** (the "stale baseline")
2. Track the **current market price** (median of exchange feeds)
3. Emit a signal when our price crosses **±0.1%** from the baseline → OCR2 round is triggering

---

## ARCHITECTURE DESIGN

### High-Level System Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    OracleProxy Service                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────────┐                      ┌──────────────────┐  │
│  │  Exchange APIs   │                      │  Chainlink RTDS  │  │
│  │  WebSockets      │                      │  (baseline)      │  │
│  │                  │                      │                  │  │
│  │• Binance         │                      │ Polymarket RTDS  │  │
│  │• Coinbase        │                      │ crypto_prices    │  │
│  │• Kraken          │                      │ _chainlink feed  │  │
│  └─────────┬────────┘                      └────────┬─────────┘  │
│            │                                        │            │
│            └────────────────┬───────────────────────┘            │
│                             │                                    │
│                 ┌───────────▼─────────────┐                      │
│                 │  Price Aggregator       │                      │
│                 │  - Median of exchanges  │                      │
│                 │  - Track chainlink_base │                      │
│                 │  - Calc deviation %     │                      │
│                 │  - Emit round_imminent  │                      │
│                 └───────────┬─────────────┘                      │
│                             │                                    │
│                 ┌───────────▼─────────────┐                      │
│                 │  Indicator Calculation  │                      │
│                 │  - EMA (12, 26, 50)     │                      │
│                 │  - Momentum (ROC)       │                      │
│                 │  - RSI                  │                      │
│                 │  - Volatility (StdDev)  │                      │
│                 │  - MACD                 │                      │
│                 └───────────┬─────────────┘                      │
│                             │                                    │
│                 ┌───────────▼─────────────┐                      │
│                 │  Real-Time Data Store   │                      │
│                 │  - DashMap (concurrent) │                      │
│                 │  - Price history (ring) │                      │
│                 │  - Indicators cache     │                      │
│                 └───────────┬─────────────┘                      │
│                             │                                    │
│         ┌───────────────────┼──────────────────────┐             │
│         │                   │                      │             │
│  ┌──────▼───────┐  ┌────────▼───────┐  ┌──────────▼──────┐      │
│  │ WebSocket    │  │  HTTP REST API │  │  Metrics &      │      │
│  │ Server       │  │                │  │  Logging        │      │
│  │ (Python bot) │  │ GET /price     │  │                 │      │
│  │              │  │ GET /indicators│  │ Prometheus      │      │
│  │ {price,      │  │ GET /health    │  │ OpenTelemetry   │      │
│  │  deviation,  │  │                │  │                 │      │
│  │  round_flag} │  │                │  │                 │      │
│  └──────────────┘  └────────────────┘  └─────────────────┘      │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Module Structure

```
src/
├── main.rs                 # Entry point, service initialization
├── config.rs              # Configuration management (env, defaults)
├── error.rs               # Custom error types
├── models.rs              # Data structures (Price, Indicator, etc)
├── aggregator/
│   ├── mod.rs
│   ├── exchange_client.rs  # Exchange WebSocket clients (Binance, Coinbase, Kraken)
│   ├── chainlink.rs        # Polymarket RTDS reader for Chainlink baseline price
│   └── calculator.rs       # Median aggregation + deviation calculation
├── indicators/
│   ├── mod.rs
│   ├── ema.rs             # Exponential Moving Average
│   ├── momentum.rs         # Rate of Change, RSI, MACD
│   ├── volatility.rs       # Standard Deviation, Bands
│   └── composite.rs        # Combined indicator calculation
├── store/
│   ├── mod.rs
│   ├── price_store.rs     # Real-time price cache (DashMap)
│   └── history.rs         # Rolling price history
├── ws_server/
│   ├── mod.rs
│   ├── handler.rs         # WebSocket message handler
│   └── broadcast.rs       # Broadcasting to connected clients
├── http_api/
│   ├── mod.rs
│   ├── routes.rs          # HTTP endpoint definitions
│   └── handlers.rs        # Request handlers
└── monitoring/
    ├── mod.rs
    ├── metrics.rs         # Prometheus metrics
    └── health.rs          # Health check endpoints
```

---

## EXCHANGE WEBSOCKET IMPLEMENTATION PLAN

### Primary Exchanges (Binance, Coinbase, Kraken — all that's needed)

#### Binance
- **Endpoint:** `wss://stream.binance.com:9443/ws`
- **Stream:** `btcusdt@aggTrade`
- **Update Freq:** Real-time (~100ms)
- **Auth:** Not required for public streams
- **Note:** ~40% of BTC spot volume; most important signal source

```rust
{"method": "SUBSCRIBE", "params": ["btcusdt@aggTrade"], "id": 1}

// Response
{
  "e": "aggTrade",
  "T": 1694518496789,
  "p": "67234.50",
  "q": "0.01250000"
}
```

#### Coinbase
- **Endpoint:** `wss://ws-feed.exchange.coinbase.com`
- **Channel:** `ticker` for BTC-USD
- **Update Freq:** Real-time
- **Auth:** Not required for public ticker
- **Note:** ~15% of BTC spot volume; high USD liquidity, no USDT noise

```rust
{
  "type": "subscribe",
  "product_ids": ["BTC-USD"],
  "channels": ["ticker"]
}
```

#### Kraken
- **Endpoint:** `wss://ws.kraken.com/`
- **Subscription:** `ticker` for XBT/USD
- **Update Freq:** Real-time
- **Auth:** Not required

```rust
{
  "event": "subscribe",
  "pair": ["XBT/USD"],
  "subscription": {"name": "ticker"}
}
```

### Resilience Features
- **Reconnection:** Exponential backoff (1s → 2s → 4s → ... → 60s)
- **Health Checks:** Stale price detection (>5s without update = warn, >30s = circuit break)
- **Fallback:** If only 2 of 3 exchanges are live, continue with median of 2

### Chainlink Baseline Reader (`aggregator/chainlink.rs`)

Subscribe to Polymarket RTDS `crypto_prices_chainlink` channel:
- WebSocket: `wss://data-api.polymarket.com/` (or equivalent RTDS endpoint)
- Parse `btc/usd` price updates
- Store as `chainlink_baseline` — the anchor against which deviation is measured
- This updates lazily (only on 0.1% moves or heartbeat), which is exactly the staleness we're exploiting

```rust
// Stored state
struct ChainlinkBaseline {
    price: f64,
    updated_at: Instant,
    round_id: u64,
}
```

---

## AGGREGATION & DEVIATION LOGIC (`aggregator/calculator.rs`)

### Price Aggregation
- Collect latest price from each live exchange
- Take the **median** — that's it. No weighted average, no IQR filter, no sigma drop
- The median is inherently outlier-resistant; a rogue exchange feed gets ignored naturally

```rust
fn aggregate(prices: &[f64]) -> f64 {
    let mut sorted = prices.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    }
}
```

### Deviation Calculation
```rust
fn deviation_pct(market_price: f64, chainlink_base: f64) -> f64 {
    ((market_price - chainlink_base) / chainlink_base) * 100.0
}

// Signal thresholds
const ROUND_TRIGGER_THRESHOLD: f64 = 0.10;  // % — Chainlink OCR2 round likely triggering
const APPROACH_THRESHOLD: f64 = 0.07;       // % — heads-up, getting close
```

### Output Fields Added to Every Message
```json
{
  "chainlink_price": 97850.00,
  "chainlink_age_secs": 42,
  "market_price": 97985.00,
  "deviation_pct": 0.138,
  "round_imminent": true
}
```

---

## INDICATOR CALCULATION APPROACH

These remain useful for the bot's position sizing and entry timing, even though they're not part of the deviation detection core.

### Core Indicators

#### 1. Exponential Moving Average (EMA)
```
EMA = Price × α + EMA_prev × (1 - α)
where α = 2 / (N + 1)

Periods: EMA-12 (fast), EMA-26 (slow), EMA-50 (trend)
```

#### 2. Momentum (Rate of Change - ROC)
```
ROC = (Current Price - Price N periods ago) / Price N periods ago × 100
Periods: ROC-10, ROC-20
```

#### 3. Relative Strength Index (RSI)
```
RSI = 100 - (100 / (1 + RS))
RS = Average Gain / Average Loss over N periods
Period: 14
```

#### 4. Volatility (Standard Deviation & Bollinger Bands)
```
StdDev = sqrt(Σ(Price - MA)² / N)
Bands = MA ± (StdDev × 2)
Window: 20 periods
```

#### 5. MACD
```
MACD Line = EMA-12 - EMA-26
Signal Line = EMA-9(MACD Line)
Histogram = MACD Line - Signal Line
```

### Calculation Update Strategy
- Ring buffer of last 50 price points
- Recalculate on each new aggregated price tick

---

## WEBSOCKET SERVER FOR PYTHON BOT INTEGRATION

### Message Format (Server → Client)
```json
{
  "type": "price_update",
  "data": {
    "timestamp": "2025-09-12T12:34:56.789Z",
    "symbol": "BTC/USD",
    "market_price": 97985.00,
    "chainlink_price": 97850.00,
    "chainlink_age_secs": 42,
    "deviation_pct": 0.138,
    "round_imminent": true,
    "exchange_prices": {
      "binance": 97980.00,
      "coinbase": 97990.00,
      "kraken": 97985.00
    },
    "indicators": {
      "ema_12": 97900.00,
      "ema_26": 97800.00,
      "ema_50": 97600.00,
      "rsi_14": 65.50,
      "roc_10": 0.14,
      "roc_20": 0.28,
      "volatility": 450.25,
      "bb_upper": 98400.00,
      "bb_middle": 97900.00,
      "bb_lower": 97400.00,
      "macd": 100.00,
      "macd_signal": 92.00,
      "macd_histogram": 8.00
    }
  }
}
```

### Python Bot Integration Example
```python
import asyncio
import json
import websockets

async def oracle_consumer():
    async with websockets.connect('ws://localhost:8080/ws') as ws:
        await ws.send(json.dumps({"action": "subscribe", "channels": ["BTC/USD"]}))
        
        async for message in ws:
            data = json.loads(message)
            
            if data['type'] == 'price_update':
                d = data['data']
                
                if d['round_imminent']:
                    direction = "UP" if d['deviation_pct'] > 0 else "DOWN"
                    print(f"CHAINLINK ROUND TRIGGERING {direction}: "
                          f"market={d['market_price']}, "
                          f"chainlink={d['chainlink_price']}, "
                          f"deviation={d['deviation_pct']:.3f}%")
```

---

## IMPLEMENTATION ROADMAP

### Phase 1: Core Pipeline
- [ ] Project setup, Cargo dependencies, config management
- [ ] Data models (include `ChainlinkBaseline`, `DeviationAlert`)
- [ ] Binance + Coinbase WebSocket clients
- [ ] Median aggregator + deviation calculator
- [ ] Polymarket RTDS client for Chainlink baseline
- [ ] Unit tests for aggregator + deviation logic

### Phase 2: Indicators & WebSocket Server
- [ ] Indicator calculations (EMA, RSI, MACD, ROC, StdDev)
- [ ] Real-time store (DashMap) + history ring buffer
- [ ] WebSocket server + broadcast system
- [ ] Kraken WebSocket client (third feed for median robustness)
- [ ] Python bot integration testing

### Phase 3: Production Hardening
- [ ] HTTP REST API (health, price, indicators)
- [ ] Prometheus metrics + structured logging
- [ ] Reconnection logic + stale-feed circuit breakers
- [ ] Load testing

### Phase 4: Deployment
- [ ] Docker containerization
- [ ] Monitoring dashboards (Grafana)
- [ ] Runbooks for exchange outage / Chainlink delay scenarios

> **Note:** Persistence, historical archival, and backtesting are intentionally out of scope for this service.
> Those concerns live in a separate `polymarket-researcher` Python project that subscribes to this WS feed.

---

## OUTPUT SCHEMA

### WebSocket Event Types (server → client)

All messages share a common envelope:
```json
{ "type": "<event>", "ts": "<ISO-8601 UTC>", "data": { ... } }
```

#### `tick` — every aggregation interval (~500ms)
The standard streaming update. Consumers should use this for real-time indicator feeds.
```json
{
  "type": "tick",
  "ts": "2026-02-19T15:06:03.669Z",
  "data": {
    "symbol": "BTC/USD",
    "market_price": 66511.60,
    "chainlink_price": 66598.68,
    "chainlink_age_secs": 13,
    "deviation_pct": -0.131,
    "round_imminent": true,
    "exchange_prices": {
      "binance": 66508.00,
      "coinbase": 66514.00,
      "kraken": 66511.60
    },
    "indicators": {
      "ema_12": 66490.00,
      "ema_26": 66450.00,
      "ema_50": 66320.00,
      "rsi_14": 44.2,
      "roc_10": -0.14,
      "roc_20": -0.28,
      "std_dev": 82.50,
      "bb_upper": 66764.00,
      "bb_middle": 66511.00,
      "bb_lower": 66258.00,
      "macd": 40.00,
      "macd_signal": 35.00,
      "macd_histogram": 5.00
    }
  }
}
```

#### `round_triggered` — rising edge only, fires once per trigger event
Emitted when `deviation_pct` first crosses ±0.10% (state transition: not-imminent → imminent).
The most actionable event — this is the signal to consider a Polymarket position.
```json
{
  "type": "round_triggered",
  "ts": "2026-02-19T15:06:03.669Z",
  "data": {
    "direction": "DOWN",
    "market_price": 66511.60,
    "chainlink_price": 66598.68,
    "chainlink_age_secs": 13,
    "deviation_pct": -0.131,
    "exchange_prices": {
      "binance": 66508.00,
      "coinbase": 66514.00,
      "kraken": 66511.60
    }
  }
}
```

#### `round_settled` — chainlink baseline updated on-chain
Emitted when the Chainlink poller detects a new committed price (baseline changed).
Marks the end of the opportunity window — Polymarket will settle against this new price.
```json
{
  "type": "round_settled",
  "ts": "2026-02-19T15:06:47.210Z",
  "data": {
    "prev_price": 66598.68,
    "new_price": 66511.00,
    "price_delta": -87.68,
    "delta_pct": -0.132,
    "round_duration_secs": 44
  }
}
```

#### `deviation_approach` — rising edge, deviation enters 0.07–0.10% zone
Early warning before the round trigger. Fires once per approach event.
Typical lead time over `round_triggered`: 5–20 seconds.
```json
{
  "type": "deviation_approach",
  "ts": "2026-02-19T15:05:58.000Z",
  "data": {
    "direction": "DOWN",
    "deviation_pct": -0.073,
    "market_price": 66549.00,
    "chainlink_price": 66598.68,
    "chainlink_age_secs": 8
  }
}
```

#### `bb_breakout` — price crosses outside Bollinger Band (rising edge)
Fires when price exits its 2σ Bollinger Band. `bb_expanding: true` means volatility
is growing (not a reversion spike) — higher conviction signal.
Typical lead time over `round_triggered`: 10–30 seconds.
```json
{
  "type": "bb_breakout",
  "ts": "2026-02-19T15:05:55.000Z",
  "data": {
    "direction": "DOWN",
    "market_price": 66560.00,
    "bb_upper": 66680.00,
    "bb_lower": 66570.00,
    "bb_width_pct": 0.165,
    "bb_expanding": true,
    "deviation_pct": -0.058
  }
}
```

#### `pre_trigger_alert` — multi-signal convergence (rising edge, highest conviction)
Fires when ≥2 independent signals align AND deviation ≥ 0.05%.
This is the primary actionable signal for Polymarket position entry.

**Signals checked:**
| Signal | Condition |
|---|---|
| `bb_breakout` | Price outside 2σ band in deviation direction |
| `deviation_approach` | `abs(dev) >= 0.07%` |
| `momentum_surge` | ROC-10 > 0.02% aligned with deviation direction |
| `rsi_extreme` | RSI > 65 (up) or < 35 (down) aligned with direction |

```json
{
  "type": "pre_trigger_alert",
  "ts": "2026-02-19T15:05:55.500Z",
  "data": {
    "direction": "DOWN",
    "signals": ["bb_breakout", "momentum_surge"],
    "deviation_pct": -0.062,
    "market_price": 66557.00,
    "chainlink_price": 66598.68,
    "chainlink_age_secs": 5,
    "bb_width_pct": 0.165,
    "rsi_14": 38.2,
    "momentum_10": -0.042
  }
}
```

#### `exchange_status` — feed connect/disconnect
```json
{
  "type": "exchange_status",
  "ts": "2026-02-19T15:06:00.000Z",
  "data": {
    "exchange": "binance",
    "status": "connected",
    "error": null
  }
}
```

### Event Timing Summary

```
t=0s   bb_breakout fires (price exits bands, bands expanding)
t=5s   pre_trigger_alert fires (bb_breakout + momentum_surge ≥ 2 signals)
t=8s   deviation_approach fires (deviation crosses 0.07%)
t=15s  round_triggered fires (deviation crosses 0.10%)  ← Chainlink OCR2 begins
t=44s  round_settled fires (new price committed on-chain)  ← Polymarket settles
```
Strategy window: between `pre_trigger_alert`/`round_triggered` and `round_settled`.

---

### HTTP Endpoints

#### `GET /health`
```json
{
  "status": "healthy",
  "uptime_secs": 3600,
  "exchanges": {
    "binance":  { "status": "connected", "last_price": 66508.00, "age_ms": 320 },
    "coinbase": { "status": "connected", "last_price": 66514.00, "age_ms": 410 },
    "kraken":   { "status": "connected", "last_price": 66511.60, "age_ms": 290 }
  },
  "chainlink_age_secs": 13,
  "price_freshness_ms": 80,
  "ws_clients": 3
}
```

#### `GET /api/v1/snapshot`
Returns the full current `tick` payload — equivalent to the last WS `tick` event.

#### `GET /metrics`
Prometheus text format. Key metrics:
- `oracle_market_price_usd`
- `oracle_chainlink_price_usd`
- `oracle_deviation_pct`
- `oracle_chainlink_age_secs`
- `oracle_round_trigger_total` (counter)
- `oracle_exchange_connected{exchange}` (gauge 0/1)
- `oracle_ws_clients`

---

## TESTING STRATEGY

### Unit Tests
- Median aggregation edge cases (1 exchange, 2 exchanges, all same price)
- Deviation calculation at exact threshold boundaries
- Indicator calculations (EMA, RSI, MACD)
- Stale feed detection

### Integration Tests
- End-to-end price feed → deviation alert pipeline
- WebSocket server client interaction
- Reconnection / failover behavior

### Load Tests
- 1,000+ simultaneous WebSocket connections
- Price update latency (<100ms P99)
- Memory stability over 72 hours

---

## DEPLOYMENT & MONITORING

### Health Checks
```
GET /health
{
  "status": "healthy",
  "uptime_seconds": 3600,
  "exchanges": {
    "binance": "connected",
    "coinbase": "connected",
    "kraken": "connected"
  },
  "chainlink_baseline_age_secs": 42,
  "price_freshness_ms": 450,
  "websocket_clients": 3
}
```

### Metrics Exposed
- `price_update_latency_ms` (histogram)
- `exchange_connection_status` (gauge)
- `aggregated_price_usd` (gauge)
- `chainlink_baseline_price_usd` (gauge)
- `chainlink_deviation_pct` (gauge)
- `chainlink_round_trigger_count` (counter)
- `indicator_values` (gauges for each)
- `websocket_client_count` (gauge)

### Alerting
- Exchange disconnected >5 minutes
- Price stale >30 seconds
- Chainlink baseline stale >2 hours (unexpected, heartbeat should fire)
- Latency P99 >500ms

---

## DEPENDENCIES & REQUIREMENTS

### System Requirements
- Rust 1.70+
- Tokio async runtime
- 1GB+ RAM (production)
- Reliable low-latency network connection

### External Dependencies
- Binance API (no key needed for public streams)
- Coinbase API (public ticker stream)
- Kraken API (public stream)
- Polymarket RTDS WebSocket (public, for Chainlink baseline)

### Development Tools
- `cargo` + `rustup`
- `tokio-console` (optional, for async debugging)
- `cargo-criterion` (benchmarking)
- `cargo-tarpaulin` (code coverage)

---

## PHASE 6 — MOVE CLOB COLLECTOR INTO RUST (rs-clob-client)

**Objective:** Consolidate live market-data ingestion (oracle + Polymarket CLOB) into `btc-oracle-proxy` for lower-latency execution and cleaner separation (Rust = live path, Python = research/backtesting).

### Scope Decision
- Keep `polymarket-researcher` as **DuckDB schema + analytics/backtest** layer.
- Move live CLOB collection responsibilities from Python `pm_clob_consumer.py` into Rust.
- Python should no longer be required in the live execution loop.

### Dependency
- [x] Integrate CLOB live ingestion path in Rust (`src/clob/ws.rs` + writer queue).
- [ ] Optional future: switch token discovery/subscription to official `rs-clob-client` crate.
- [ ] Pin crate revision/tag and document compatibility in `Cargo.toml` (if/when switched).
- [ ] Add integration tests to validate API responses and WS reconnection behavior.

### Rust Modules (new)
- [x] `src/clob/mod.rs`
- [~] `src/clob/client.rs` — placeholder models exist; full rs-clob-client wrapper still pending
- [x] `src/clob/state.rs` — in-memory order book/token UI state per token
- [x] `src/clob/metrics.rs` — L1 + L2 derived metrics
- [x] `src/clob/writer.rs` — async DB write queue to DuckDB (batch insert)
- [x] `src/clob/ws.rs` — WS-first ingest + sampling + market refresh + DB write gating

### Data Flow (Rust hot path)
1. Subscribe to active token IDs for target markets.
2. Maintain in-memory book state per token (full book or top-N levels).
3. Compute:
   - L1: `best_bid`, `best_ask`, `mid`, `spread`, `bid_depth_1`, `ask_depth_1`
   - L2 derived: `bid_depth_5/10`, `ask_depth_5/10`, `depth_imbalance_5/10`, `slippage_100/1000`, level counts.
4. Batch-write snapshots to `researcher.db` on an interval (non-blocking writer task).

### DuckDB Integration
- [x] Use one dedicated writer task/thread for DB IO (bounded mpsc queue).
- [x] Never block WS ingestion on DB writes.
- [x] Batch inserts on configurable interval (`CLOB_WRITER_FLUSH_MS`) or queue-size trigger.
- [x] Add drop/backpressure policy when queue is full (log + UI counters).
- [x] Use shared DB path: `../data/researcher.db` (repo root `Polymarket/data/`).
- [x] Added WS-side sampler (`CLOB_SAMPLE_INTERVAL_MS`, `CLOB_SAMPLE_FORCE_EMIT_MS`) to reduce write amplification.
- [x] Added malformed payload filtering before enqueue (finite checks + price/size sanity bounds).
- [x] Added active-window write gate: persist only current 5m/15m markets; keep 1h lookahead in-memory.

### Schema Tasks (researcher DB)
- [x] `pm_snapshots` includes L2-derived aggregate columns in live writes.
- [x] `pm_order_book_levels` currently enabled for full-depth archival.
- [x] `source` field populated (`live_rust`) for lineage.
- [ ] Add/verify indexes for `(token_id, ts)` and `(condition_id, ts)` if missing.
- [ ] Decide whether to keep full `pm_order_book_levels` always-on or sampled/periodic only.

### Backfill Strategy (important)
- Historical CLI endpoint provides **1-minute price series**, not full historical L2 book depth.
- [ ] Keep periodic backfill for price gaps via CLI `price-history`.
- [ ] Treat historical L2 as **forward-only** unless a true historical book source is introduced.
- [ ] Optional: run periodic Rust snapshots (e.g., every 5m) into `pm_order_book_levels` for richer future datasets.

### Performance Guardrails
- [ ] p95 ingest loop latency target: < 20ms (excluding network jitter).
- [ ] DB write queue depth alert threshold.
- [ ] WS reconnect backoff with jitter.
- [ ] Metrics: messages/sec, dropped updates, queue lag, snapshot writes/sec, 429/error counts.

### Migration Plan
- [x] Phase A: Run Rust collector in shadow mode; keep Python collector as reference.
- [x] Phase B: Compare row counts/quality and growth behavior; identify redundancy profile.
- [x] Phase C: Switch primary writes to Rust; Python no longer required in live path.
- [~] Phase D: Keep Python paths for offline backtesting + model experiments only (cleanup/documentation still pending).

### Deliverables
- [x] Rust CLOB collector running in production with DB writes.
- [~] Updated docs describing decoupled architecture (core docs updated, final pass pending).
- [ ] Validation report: Rust vs Python collector parity window (24-72h).

### 2026-02-28 Progress Snapshot
- Implemented WS-first CLOB ingest with slug-based market discovery (Gamma API), periodic refresh, and in-memory TUI state.
- Added DB write sampling (`CLOB_SAMPLE_INTERVAL_MS`, `CLOB_SAMPLE_FORCE_EMIT_MS`) so flush cadence no longer implies raw event write rate.
- Added active-window persistence gating: only current 5m/15m periods are persisted; 1h lookahead is still watched in-memory.
- Added malformed row filtering before enqueue (finite + bounded price/size checks) and writer recovery behavior for repeated flush failures.
- Performed one-time historical prune of redundant same-topbook rows:
  - deleted snapshots: 30,171
  - deleted order-book levels: 2,928,798

### Remaining Work (besides observing runtime for 48h)
1. Add writer/ingest observability in logs/TUI (rows/sec, levels/sec, malformed/skipped counters).
2. Confirm `pm_markets` metadata joins (timeframe/close_time) are fully populated for post-hoc analytics.
3. Decide long-term retention policy for `pm_order_book_levels` (always-on vs periodic/sample-only).
4. Add/verify DB indexes and run query-latency sanity checks after sustained ingest.
5. Produce short validation report (growth rate, redundancy ratio, and signal quality vs prior baseline).

---

## NOTES & REFERENCES

### Chainlink Aggregation Methodology
- OCR2 (Off-Chain Reporting 2): nodes gossip off-chain, one transaction submitted per round
- Aggregation is **median** across node submissions — no separate sigma/IQR step
- Primary data source per node: **CryptoCompare CCCAGG** (volume-weighted multi-exchange)
- 0.1% deviation threshold triggers a new round; otherwise heartbeat (up to 24h for BTC/USD)
- `minAnswer`/`maxAnswer` circuit breakers in the contract reject extreme node submissions
- Total lag from spot move to on-chain commit: ~15-60 seconds

### Why We Don't Need Byzantine Fault Tolerance
BFT is Chainlink's concern, not ours. We're a **single service** reading from 3 public exchange WebSockets. Using a median of 3 sources is sufficient: a single bad feed gets outvoted automatically. We have no adversarial nodes, no consensus protocol, no need for formal BFT guarantees.

### Why We Don't Need Explicit Outlier Detection
Median aggregation over 3+ sources already handles this. If Binance has a momentary price spike, the median of [Binance_spike, Coinbase_normal, Kraken_normal] will be `Coinbase_normal` — the outlier is discarded without any extra code. IQR/sigma filtering would add complexity with no material benefit given our small, trusted source set.

### Polymarket Integration
- RTDS WebSocket `crypto_prices_chainlink` provides the Chainlink price as Polymarket sees it
- This is faster than reading on-chain directly (no RPC needed)
- The staleness of this value is the core of our edge: it updates only on 0.1% deviation or heartbeat

### Future Enhancements
- [ ] Multi-asset feeds (ETH, SOL, etc)
- [ ] On-chain settlement trigger monitoring (watch for tx confirming new round)
- [ ] gRPC API for lower-latency bots
