# OracleProxy - Chainlink Round Trigger Detector

A Rust service that tracks BTC spot prices from major exchanges, monitors the last committed Chainlink on-chain price, and detects when the 0.1% deviation threshold is crossed — signalling that a Chainlink OCR2 round is triggering. Exposes the combined data via WebSocket for Python trading bot integration.

**Core insight:** Chainlink's BTC/USD feed on Polygon has a ~15-60 second lag between a spot move and the new price being committed on-chain. By detecting when that 0.1% trigger threshold is crossed in real-time, the bot gets an actionable edge before Polymarket settles.

## Features

### Deviation Detection
- **Chainlink Baseline Tracking**: Reads current committed Chainlink price via Polymarket RTDS
- **Real-Time Deviation Calc**: Continuously measures `(market_price - chainlink_price) / chainlink_price`
- **Round Imminent Flag**: Emits `round_imminent: true` when deviation crosses ±0.1%
- **Baseline Age Tracking**: Surfaces how stale the Chainlink price is (staleness = opportunity window)

### Price Aggregation
- **3-Exchange Median**: Binance, Coinbase, Kraken — median is inherently outlier-resistant
- **No Explicit Outlier Filtering Needed**: With a median, a single bad feed gets outvoted automatically
- **Exchange Prices Included**: Raw per-exchange prices surfaced for bot inspection

### Technical Indicators
- **EMA**: 12, 26, 50 period exponential moving averages
- **RSI**: Relative Strength Index (14 period)
- **Momentum**: Rate of Change (ROC) at 10 and 20 periods
- **Volatility**: Standard Deviation and Bollinger Bands
- **MACD**: Moving Average Convergence Divergence with signal line

### WebSocket API
- **Push-Based**: Low-latency feed to Python bot
- **JSON Format**: Easy to parse, includes deviation fields on every message
- **Heartbeat**: Periodic connection health checks

### HTTP REST API
- **Health Checks**: `/health` endpoint including Chainlink baseline age
- **Price Queries**: `/api/v1/price/{symbol}`
- **Indicator Data**: `/api/v1/indicators`
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

## WebSocket Message Format

### Subscribe
```json
{"action": "subscribe", "channels": ["BTC/USD"]}
```

### Price Update Message
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

### Key Fields for Trading Logic

| Field | Description |
|---|---|
| `chainlink_price` | Last committed Chainlink on-chain price |
| `chainlink_age_secs` | How long since Chainlink last updated |
| `deviation_pct` | % difference between market and Chainlink price |
| `round_imminent` | `true` when `abs(deviation_pct) >= 0.10` |

## Python Bot Integration

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
                          f"market={d['market_price']:.2f}, "
                          f"chainlink={d['chainlink_price']:.2f}, "
                          f"deviation={d['deviation_pct']:.3f}%")
                    # Your Polymarket position logic here

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
