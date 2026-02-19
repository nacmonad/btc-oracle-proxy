#!/usr/bin/env python3
"""
Oracle-proxy WebSocket smoke test / live monitor.

Usage:
    python tests/test_websocket.py                    # default ws://127.0.0.1:8080/ws
    python tests/test_websocket.py ws://host:port/ws  # custom addr
    python tests/test_websocket.py --ticks-only       # suppress noisy tick events

Requirements:
    pip install websockets
"""

import asyncio
import json
import sys
import time
from datetime import datetime, timezone

try:
    import websockets
except ImportError:
    print("Missing dependency:  pip install websockets")
    sys.exit(1)


# ── ANSI colours ──────────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RED    = "\033[31m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
WHITE  = "\033[97m"
BG_RED = "\033[41m"

def c(color: str, text: str) -> str:
    return f"{color}{text}{RESET}"

def ts_now() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]


# ── Formatters per event type ─────────────────────────────────────────────────

def fmt_tick(data: dict) -> str | None:
    mp  = data.get("market_price", 0)
    cl  = data.get("chainlink_price")
    dev = data.get("deviation_pct")
    age = data.get("chainlink_age_secs")
    ri  = data.get("round_imminent", False)

    dev_str = ""
    if dev is not None:
        color = RED if abs(dev) >= 0.10 else (YELLOW if abs(dev) >= 0.07 else GREEN)
        arrow = "↑" if dev > 0 else "↓"
        dev_str = c(color, f"{arrow}{dev:+.3f}%")
        if ri:
            dev_str += c(BG_RED + BOLD, " ⚡ROUND IMMINENT")

    cl_str  = c(CYAN, f"${cl:,.2f}") if cl else c(DIM, "awaiting")
    age_str = c(DIM, f"({age}s ago)") if age is not None else ""

    return (
        f"{c(DIM, ts_now())}  "
        f"tick  "
        f"market={c(WHITE + BOLD, f'${mp:,.2f}')}  "
        f"chainlink={cl_str} {age_str}  "
        f"dev={dev_str}"
    )


def fmt_pre_trigger(data: dict) -> str:
    direction = data.get("direction", "?")
    dev       = data.get("deviation_pct", 0)
    signals   = data.get("signals", [])
    mp        = data.get("market_price", 0)
    cl        = data.get("chainlink_price", 0)
    age       = data.get("chainlink_age_secs", 0)
    bb_w      = data.get("bb_width_pct")
    rsi       = data.get("rsi_14")
    roc       = data.get("momentum_10")

    dir_color = GREEN if direction == "UP" else RED
    extra = []
    if bb_w is not None: extra.append(f"bb_width={bb_w:.3f}%")
    if rsi  is not None: extra.append(f"rsi={rsi:.1f}")
    if roc  is not None: extra.append(f"roc10={roc:+.3f}%")

    return (
        f"\n{c(YELLOW + BOLD, '━' * 60)}\n"
        f"{c(YELLOW + BOLD, f'🎯  PRE-TRIGGER ALERT  {direction}')}\n"
        f"  signals   : {c(YELLOW, ', '.join(signals))}\n"
        f"  deviation : {c(dir_color + BOLD, f'{dev:+.3f}%')}\n"
        f"  market    : {c(WHITE, f'${mp:,.2f}')}   "
        f"chainlink : {c(CYAN, f'${cl:,.2f}')}  ({age}s ago)\n"
        f"  {c(DIM, '  '.join(extra))}\n"
        f"{c(YELLOW + BOLD, '━' * 60)}\n"
    )


def fmt_round_triggered(data: dict) -> str:
    direction = data.get("direction", "?")
    dev       = data.get("deviation_pct", 0)
    mp        = data.get("market_price", 0)
    cl        = data.get("chainlink_price", 0)
    age       = data.get("chainlink_age_secs", 0)
    exch      = data.get("exchange_prices", {})
    exch_str  = "  ".join(
        f"{k}={c(DIM, f'${v:,.2f}')}" for k, v in sorted(exch.items())
    )
    dir_color = GREEN if direction == "UP" else RED

    return (
        f"\n{c(BG_RED + BOLD, '━' * 60)}\n"
        f"{c(BG_RED + BOLD, f'⚡  CHAINLINK ROUND TRIGGERED  {direction}')}\n"
        f"  deviation : {c(dir_color + BOLD, f'{dev:+.3f}%')}\n"
        f"  market    : {c(WHITE + BOLD, f'${mp:,.2f}')}   "
        f"chainlink : {c(CYAN, f'${cl:,.2f}')}  ({age}s ago)\n"
        f"  {exch_str}\n"
        f"{c(BG_RED + BOLD, '━' * 60)}\n"
    )


def fmt_round_settled(data: dict) -> str:
    prev     = data.get("prev_price", 0)
    new      = data.get("new_price", 0)
    delta    = data.get("price_delta", 0)
    delta_p  = data.get("delta_pct", 0)
    duration = data.get("round_duration_secs")
    color    = GREEN if delta > 0 else RED

    dur_str = f"  window={duration}s" if duration else ""

    return (
        f"\n{c(GREEN + BOLD, '━' * 60)}\n"
        f"{c(GREEN + BOLD, '🔗  CHAINLINK ROUND SETTLED')}\n"
        f"  {c(CYAN, f'${prev:,.2f}')} → {c(WHITE + BOLD, f'${new:,.2f}')}  "
        f"delta={c(color, f'{delta_p:+.3f}%')}{dur_str}\n"
        f"{c(GREEN + BOLD, '━' * 60)}\n"
    )


def fmt_deviation_approach(data: dict) -> str:
    direction = data.get("direction", "?")
    dev       = data.get("deviation_pct", 0)
    mp        = data.get("market_price", 0)
    cl        = data.get("chainlink_price", 0)
    age       = data.get("chainlink_age_secs", 0)
    dir_color = GREEN if direction == "UP" else RED

    return (
        f"{c(DIM, ts_now())}  "
        f"{c(YELLOW + BOLD, '〰  DEVIATION APPROACH')}  "
        f"{c(dir_color, direction)}  "
        f"dev={c(YELLOW + BOLD, f'{dev:+.3f}%')}  "
        f"market={c(WHITE, f'${mp:,.2f}')}  "
        f"chainlink={c(CYAN, f'${cl:,.2f}')} ({age}s ago)"
    )


def fmt_bb_breakout(data: dict) -> str:
    direction = data.get("direction", "?")
    mp        = data.get("market_price", 0)
    bb_w      = data.get("bb_width_pct", 0)
    expanding = data.get("bb_expanding", False)
    dev       = data.get("deviation_pct")
    dev_str   = f"{dev:+.3f}%" if dev is not None else "n/a"
    dir_color = GREEN if direction == "UP" else RED
    exp_str   = c(GREEN, "expanding") if expanding else c(DIM, "flat")

    return (
        f"{c(DIM, ts_now())}  "
        f"{c(CYAN + BOLD, '📈  BB BREAKOUT')}  "
        f"{c(dir_color, direction)}  "
        f"market={c(WHITE, f'${mp:,.2f}')}  "
        f"width={bb_w:.3f}%  bands={exp_str}  "
        f"dev={c(YELLOW, dev_str)}"
    )


def fmt_exchange_status(ev: dict) -> str:
    exchange = ev.get("exchange", "?")
    status   = ev.get("status", "?")
    error    = ev.get("error")
    color    = GREEN if status == "connected" else RED
    err_str  = f"  {c(RED, error)}" if error else ""
    return (
        f"{c(DIM, ts_now())}  "
        f"exchange_status  "
        f"{c(WHITE + BOLD, exchange)} → {c(color, status)}{err_str}"
    )


# ── Main loop ─────────────────────────────────────────────────────────────────

async def run(uri: str, ticks_only: bool, show_ticks: bool) -> None:
    tick_count   = 0
    event_counts: dict[str, int] = {}
    connect_time = time.monotonic()

    print(c(BOLD, f"\nConnecting to {uri} …"))

    async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
        print(c(GREEN + BOLD, "✓ Connected\n"))

        # Subscribe
        await ws.send(json.dumps({"type": "subscribe", "channels": ["BTC/USD"]}))

        async for raw in ws:
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                print(c(RED, f"[bad JSON] {raw[:120]}"))
                continue

            event_type = ev.get("type", "unknown")
            data       = ev.get("data", ev)  # some events embed data at top level
            event_counts[event_type] = event_counts.get(event_type, 0) + 1

            if event_type == "tick":
                tick_count += 1
                if show_ticks:
                    line = fmt_tick(data)
                    if line:
                        print(line)
                elif tick_count % 60 == 1:
                    # Print a summary line every ~30s instead
                    mp  = data.get("market_price", 0)
                    dev = data.get("deviation_pct")
                    age = data.get("chainlink_age_secs")
                    uptime = int(time.monotonic() - connect_time)
                    dev_s = f"{dev:+.3f}%" if dev is not None else "n/a"
                    print(
                        c(DIM, ts_now()) +
                        c(DIM, f"  [tick #{tick_count}  uptime={uptime}s]  ") +
                        f"market={c(WHITE, f'${mp:,.2f}')}  dev={c(CYAN, dev_s)}  age={age}s"
                    )

            elif event_type == "pre_trigger_alert":
                print(fmt_pre_trigger(data))

            elif event_type == "round_triggered":
                print(fmt_round_triggered(data))

            elif event_type == "round_settled":
                print(fmt_round_settled(data))

            elif event_type == "deviation_approach":
                print(fmt_deviation_approach(data))

            elif event_type == "bb_breakout":
                print(fmt_bb_breakout(data))

            elif event_type == "exchange_status":
                print(fmt_exchange_status(ev))

            else:
                print(c(DIM, f"[{event_type}]  {json.dumps(data)[:120]}"))


async def main() -> None:
    args       = sys.argv[1:]
    uri        = "ws://127.0.0.1:8080/ws"
    ticks_only = "--ticks-only" in args
    show_ticks = "--no-ticks" not in args and not ticks_only

    for arg in args:
        if arg.startswith("ws://") or arg.startswith("wss://"):
            uri = arg

    if ticks_only:
        show_ticks = True

    retry_delay = 2
    while True:
        try:
            await run(uri, ticks_only, show_ticks)
        except (
            websockets.exceptions.ConnectionClosedError,
            websockets.exceptions.ConnectionClosedOK,
        ):
            print(c(YELLOW, f"\nConnection closed — reconnecting in {retry_delay}s…"))
        except OSError as e:
            print(c(RED, f"\nConnection failed ({e}) — retrying in {retry_delay}s…"))
        except KeyboardInterrupt:
            print(c(DIM, "\nBye."))
            break

        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, 30)


if __name__ == "__main__":
    asyncio.run(main())
