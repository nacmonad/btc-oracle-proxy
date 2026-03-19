# Execution Signer API (proposed)

Goal: allow `polymarket-researcher/execution` to offload signing/posting to Rust service over localhost.

## Base
- Host: `127.0.0.1`
- Transport: HTTP (or UDS later)
- Auth: local-only bind + optional shared token header

## Endpoints

### `GET /execution/health`
Response:
```json
{"ok": true, "service": "oracle-proxy-execution", "version": "0.1"}
```

### `POST /execution/order`
Create/sign/post an order.

Request:
```json
{
  "client_order_id": "uuid-or-run-id",
  "token_id": "...",
  "side": "BUY|SELL",
  "price": 0.62,
  "size": 1.0,
  "tif": "GTD|GTC",
  "expiration_ts": 1760000000,
  "strategy_name": "S1",
  "strategy_version": "0.1",
  "paper": false
}
```

Response:
```json
{
  "ok": true,
  "order_id": "...",
  "status": "accepted|rejected",
  "reason": "",
  "submit_to_ack_ms": 118.4
}
```

### `POST /execution/cancel`
Request:
```json
{"order_id":"..."}
```
Response:
```json
{"ok": true, "cancelled": true}
```

### `GET /execution/order_status?order_id=...`
Response:
```json
{
  "ok": true,
  "order_id": "...",
  "status": "open|partial|filled|cancelled|rejected",
  "filled_size": 0.0,
  "remaining_size": 1.0,
  "avg_fill_price": null
}
```

## Notes
- Keep response schema stable for Python executor.
- Include deterministic error codes for retries (`auth_failed`, `rate_limited`, `network_error`, `invalid_order`).
- Prefer Rust side to emit exact `submit->ack` timings.
