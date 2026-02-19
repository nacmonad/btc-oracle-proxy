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
- [ ] Historical deviation analysis (how often does the signal fire, how far does price move post-trigger)
- [ ] Multi-asset feeds (ETH, SOL, etc)
- [ ] On-chain settlement trigger monitoring
- [ ] Historical data archival (SQLite/PostgreSQL)
- [ ] gRPC API for lower-latency bots
