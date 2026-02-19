# OracleProxy - Chainlink Round Trigger Detector

A Rust service that tracks BTC spot prices from major exchanges, monitors the last committed Chainlink on-chain price, and detects when the 0.1% deviation threshold is crossed — signalling that a Chainlink OCR2 round is triggering. Exposes the combined data via WebSocket for Python trading bot integration.

**Core insight:** Chainlink's BTC/USD feed on Polygon has a ~15-60 second lag between a spot move and the new price being committed on-chain. By detecting when that 0.1% trigger threshold is crossed in real-time, the bot gets an actionable edge before Polymarket settles.

## Features

### Deviation Detection
- **Chainlink Baseline Tracking**: Polls committed Chainlink BTC/USD price from Polygon RPC every 5s
- **Real-Time Deviation Calc**: Continuously measures `(market_price - chainlink_price) / chainlink_price`
- **Round Imminent Flag**: Emits `round_imminent: true` + `round_triggered` event when deviation crosses ±0.1%
- **Round Settled Detection**: Fires `round_settled` event when baseline updates on-chain — marks end of opportunity window
- **Baseline Age Tracking**: Surfaces how stale the Chainlink price is (staleness = opportunity window)

### Price Aggregation
- **3-Exchange Median**: Binance, Coinbase, Kraken — median is inherently outlier-resistant
- **No Explicit Outlier Filtering Needed**: With a median, a single bad feed gets outvoted automatically
- **Exchange Prices Included**: Raw per-exchange prices surfaced for bot inspection

### Technical Indicators (streaming, no persistence required)
- **EMA**: 12, 26, 50 period exponential moving averages
- **RSI**: Relative Strength Index (14 period) — overbought/oversold coloring in TUI
- **Momentum**: Rate of Change (ROC) at 10 and 20 periods
- **Volatility**: Standard Deviation and Bollinger Bands (2σ, 20-period)
- **MACD**: Moving Average Convergence Divergence with signal line and histogram

### Pre-Trigger Signal Events
Leading-indicator events that fire *before* the 0.1% Chainlink round trigger:

| Event | Trigger | Typical Lead |
|---|---|---|
| `deviation_approach` | `abs(dev) >= 0.07%` (rising edge) | 5–20s |
| `bb_breakout` | Price exits 2σ Bollinger Band while bands expanding | 10–30s |
| `pre_trigger_alert` | ≥2 signals align: BB breakout + approach + momentum + RSI extreme | 5–25s |

### TUI Dashboard
- **Server status bar**: WS + HTTP server health (live once servers implemented)
- **Feed health**: Per-exchange latency + Chainlink baseline age with color-coded freshness
- **Price panel**: Market price, Chainlink baseline, deviation with `⚡ ROUND IMMINENT` badge
- **Exchange prices**: Per-source prices side by side
- **Indicators panel**: EMA, RSI, ROC, StdDev, BB, MACD with directional coloring
- **Log footer**: Rolling event log with color-coded levels

### WebSocket API
- **Push-Based**: Low-latency event feed to Python bot
- **7 event types**: `tick`, `round_triggered`, `round_settled`, `deviation_approach`, `bb_breakout`, `pre_trigger_alert`, `exchange_status`
- **Rising-edge semantics**: Alert events fire once per state transition, not every tick

### HTTP REST API
- **Health Checks**: `/health` endpoint including Chainlink baseline age
- **Snapshot**: `/api/v1/snapshot` — latest full tick payload
- **Metrics**: `/metrics` for Prometheus scraping

## Project Structure

```
oracle-proxy/
├── Cargo.toml
├── TODO.md                    # Detailed implementation roadmap
├── README.md
├── src/
│   ├── main.rs               # Entry point and service orchestration
│   ├── config.rs             # Configuration management
│   ├── error.rs              # Custom error types
│   ├── models.rs             # Core data structures
│   ├── aggregator/
│   │   ├── mod.rs
│   │   ├── exchange_client.rs # Binance, Coinbase, Kraken WebSocket clients
│   │   ├── chainlink.rs      # Polymarket RTDS reader (Chainlink baseline price)
│   │   └── calculator.rs     # Median aggregation + deviation calculation
│   ├── indicators/
│   │   ├── mod.rs
│   │   ├── ema.rs
│   │   ├── momentum.rs       # RSI, ROC
│   │   ├── volatility.rs     # Std Dev, Bollinger Bands
│   │   └── composite.rs      # MACD
│   ├── store/
│   │   ├── mod.rs
│   │   ├── price_store.rs    # In-memory price cache
│   │   └── history.rs        # Rolling price history
│   ├── ws_server/
│   │   ├── mod.rs
│   │   ├── handler.rs
│   │   └── broadcast.rs
│   ├── http_api/
│   │   ├── mod.rs
│   │   ├── routes.rs
│   │   └── handlers.rs
│   └── monitoring/
│       ├── mod.rs
│       ├── metrics.rs
│       └── health.rs
```

## Getting Started

### Prerequisites

- **Rust 1.70+**: Install from https://rustup.rs/
- **System**: Linux, macOS, or Windows (with WSL2)

### Installation

```bash
git clone <repo-url> oracle-proxy
cd oracle-proxy
cargo build --release
cargo run
```

### Configuration

```bash
export WS_LISTEN_ADDR="127.0.0.1:8080"
export HTTP_LISTEN_ADDR="127.0.0.1:8081"
export LOG_LEVEL="info"
export BINANCE_WS_URL="wss://stream.binance.com:9443/ws"
export COINBASE_WS_URL="wss://ws-feed.exchange.coinbase.com"
export KRAKEN_WS_URL="wss://ws.kraken.com/"
export POLYMARKET_RTDS_URL="wss://data-api.polymarket.com/"
```

Or via `.env` file in project root.

## WebSocket API

### Subscribe
```json
{"type": "subscribe", "channels": ["BTC/USD"]}
```

### Event Timeline
```
t=0s   bb_breakout        — price exits Bollinger Band, bands expanding
t=5s   pre_trigger_alert  — ≥2 signals converging (bb_breakout + momentum)
t=8s   deviation_approach — deviation crosses 0.07%
t=15s  round_triggered    — deviation crosses 0.10%  ← OCR2 begins
t=44s  round_settled      — new price committed on-chain  ← Polymarket settles
```

### `pre_trigger_alert` — highest conviction pre-entry signal
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

### `round_triggered` — OCR2 round now in progress
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
    "exchange_prices": { "binance": 66508.00, "coinbase": 66514.00, "kraken": 66511.60 }
  }
}
```

### `round_settled` — opportunity window closed
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

### Key Fields for Trading Logic

| Field | Event | Description |
|---|---|---|
| `direction` | alert/trigger events | `"UP"` or `"DOWN"` |
| `signals` | `pre_trigger_alert` | Which signals converged |
| `deviation_pct` | all | % drift from last committed Chainlink price |
| `chainlink_age_secs` | all | How stale the Chainlink baseline is |
| `bb_width_pct` | `bb_breakout`, `pre_trigger_alert` | Band width as % of price — wider = more energy |
| `round_duration_secs` | `round_settled` | Window from trigger to settlement |

## Python Bot Integration

```python
import asyncio, json, websockets

async def oracle_consumer():
    async with websockets.connect('ws://localhost:8080/ws') as ws:
        await ws.send(json.dumps({"type": "subscribe", "channels": ["BTC/USD"]}))

        async for message in ws:
            ev = json.loads(message)
            t, d = ev['type'], ev.get('data', {})

            if t == 'pre_trigger_alert':
                print(f"🎯 PRE-TRIGGER {d['direction']}: "
                      f"dev={d['deviation_pct']:+.3f}%  "
                      f"signals={d['signals']}  "
                      f"bb_width={d.get('bb_width_pct', 0):.3f}%")
                # Consider entry here — ~5-25s before round_triggered

            elif t == 'round_triggered':
                print(f"⚡ ROUND TRIGGERED {d['direction']}: "
                      f"market={d['market_price']:.2f}  "
                      f"chainlink={d['chainlink_price']:.2f}  "
                      f"dev={d['deviation_pct']:+.3f}%")
                # Last chance entry / confirm existing position

            elif t == 'round_settled':
                print(f"🔗 SETTLED: {d['prev_price']:.2f} → {d['new_price']:.2f} "
                      f"({d['delta_pct']:+.3f}%)  window={d['round_duration_secs']}s")
                # Close / record outcome

asyncio.run(oracle_consumer())
```

## HTTP API Usage

```bash
curl http://localhost:8081/health
curl http://localhost:8081/api/v1/price/BTC-USD
curl http://localhost:8081/api/v1/indicators
curl http://localhost:8081/metrics
```

## Implementation Status

### ✅ Completed
- Project scaffolding and structure
- Core data models and error handling
- Configuration management
- Indicator calculation modules (EMA, RSI, ROC, StdDev, Bollinger Bands)
- Aggregation calculator foundation
- Price history management
- Monitoring infrastructure skeleton

### 🚧 In Progress
- Exchange WebSocket clients (Binance, Coinbase, Kraken)
- Polymarket RTDS client (Chainlink baseline reader)
- Median aggregator + deviation calculator
- WebSocket server + broadcast
- HTTP API endpoints

### 📋 TODO
See [TODO.md](TODO.md) for detailed implementation roadmap.

## Testing

```bash
cargo test
RUST_LOG=info cargo test -- --nocapture
cargo test test_deviation --lib
cargo tarpaulin --out Html
```

## Performance

**Target Metrics:**
- Price update to WebSocket emit: <50ms P99
- Support 100+ concurrent bot connections
- Memory: <256MB stable
- CPU: <10% on 2-core system

## Monitoring

### Health Check
```json
{
  "status": "healthy",
  "uptime_seconds": 3600,
  "exchanges": {
    "binance": "connected",
    "coinbase": "connected",
    "kraken": "connected"
  },
  "chainlink_baseline_age_secs": 42,
  "price_freshness_ms": 80,
  "websocket_clients": 3
}
```

### Key Prometheus Metrics
- `chainlink_deviation_pct` — current deviation from Chainlink baseline
- `chainlink_round_trigger_count` — how many times round_imminent has fired
- `chainlink_baseline_age_secs` — staleness of the Chainlink baseline
- `aggregated_price_usd` — current median market price
- `exchange_connection_status` — per-exchange connectivity

## References

- [Chainlink Documentation](https://docs.chain.link)
- [Chainlink OCR2 Protocol](https://research.chain.link/ocr.pdf)
- [Polymarket Documentation](https://docs.polymarket.com)
- [Tokio Runtime](https://tokio.rs)

---

**Last Updated:** 2026-02-18
**Version:** 0.1.0 (Scaffold Release)
**Status:** In Development
