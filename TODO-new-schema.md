# TODO-new-schema.md

## Branch
- Created from `feat/clob-writer`:
  - `feat/ws-tick-schema`

## Goal
Standardize live WS payloads for paper/live execution and faster backtest/replay.

## Subscription model
Client should specify:
- assets (e.g. `BTC`)
- timeframes (e.g. `5m`, `15m`)
- optional field sets

Example subscribe request:
```json
{
  "type": "subscribe",
  "assets": ["BTC"],
  "timeframes": ["5m", "15m"],
  "fields": ["oracle", "signals", "conditions.clob_l1", "conditions.clob_l2_top5"]
}
```

## Frame schema plan (v1)
Use one canonical frame type per emit:
- `type`: `market_frame`
- `schema_version`
- `seq`
- `ts_event`
- `ts_emit`
- `asset`
- `subscribed_timeframes`
- `oracle` object
- `signals` array (plural; may be empty)
- `conditions` array (per `condition_id`/timeframe with token IDs + L1/L2)

### Why `signals` plural
Multiple events can co-occur around the same time (`pre_trigger_alert`, `bb_breakout`, `round_triggered`), and array shape avoids per-event schema branching.

## Conditions array
Each `conditions[]` item should contain:
- `condition_id`
- `timeframe`
- `close_time`
- `token_yes_id`, `token_no_id`
- `clob_l1` (`yes`/`no` side best bid/ask/mid/spread)
- `clob_l2_top5` (`yes`/`no` side arrays of `[price,size]` for bids/asks)

## Example frame (summary)
See chat draft: includes
- oracle section
- signals array with `meta` fields (deviation, age, bb/rsi/momentum/stdev)
- conditions array for 5m and 15m condition snapshots
- optional writer metrics (queue/depth/drops)

## Performance goals
- improve granularity (current sample interval moved to `100ms`)
- reduce schema ambiguity for execution clients
- simplify replay/backtesting by using one stable frame shape

## Implementation tasks
1. Add server-side subscription filter by asset/timeframe.
2. Emit canonical `market_frame` with fixed shape.
3. Fill `signals[]` from event burst in aggregation loop.
4. Build `conditions[]` from current CLOB state for subscribed conditions.
5. Include top-5 levels compactly (`[[price,size], ...]`).
6. Add `schema_version` and monotonic `seq`.
7. Add optional writer metrics in frame for observability.
8. Keep compatibility mode for current payload during migration.
9. Add golden fixture tests for frame schema + parser compatibility.

## Protobuf note (decision pending)
Given local Python execution client, protobuf gains may be moderate. JSON likely sufficient initially if payload is compact and schema fixed.

Potential protobuf benefits:
- smaller payload size
- faster parse/validation
- strict schema evolution

Potential costs:
- additional tooling/codegen in Rust + Python
- migration complexity and dual-mode support
- less human-readable debugging

Recommended path:
- implement stable JSON v1 first
- benchmark CPU/latency
- add protobuf as optional WS subprotocol if JSON parse cost becomes material
