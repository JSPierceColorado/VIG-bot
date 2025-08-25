#!/usr/bin/env python3
"""
Perpetual VIG RSI bot (Alpaca, 4h)
- Every minute: fetch CLOSED 4h bars for VIG
- If RSI(14) <= 30 on a NEWLY CLOSED bar: market BUY using 100% buying power
- No selling (buy & hold)
- Logs to stdout (Railway-friendly)
"""

import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from alpaca_trade_api.rest import REST, TimeFrame  # using alpaca_trade_api (your working stack)

# ===== Config (env) =====
SYMBOL           = os.getenv("SYMBOL", "VIG")
RSI_LEN          = int(os.getenv("RSI_LEN", "14"))
RSI_BUY_THRESH   = float(os.getenv("RSI_BUY_THRESH", "30"))
CHECK_EVERY_SEC  = int(os.getenv("CHECK_EVERY_SEC", "60"))
MIN_NOTIONAL_USD = float(os.getenv("MIN_NOTIONAL_USD", "10.00"))
DATA_FEED        = os.getenv("DATA_FEED", "iex").lower()   # 'iex' (default) or 'sip'

# Alpaca creds (same env naming you already use)
ALPACA_API_KEY    = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
APCA_API_BASE_URL = os.getenv("APCA_API_BASE_URL", "https://api.alpaca.markets")  # keep your default

if not (ALPACA_API_KEY and ALPACA_SECRET_KEY):
    raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY (or APCA_* equivalents).")

api = REST(key_id=ALPACA_API_KEY, secret_key=ALPACA_SECRET_KEY, base_url=APCA_API_BASE_URL)


# ===== Utils =====
def log(msg: str):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"[vig-rsi4h] {now} | {msg}", flush=True)

def rsi(closes: List[float], length: int = 14) -> float:
    """Wilder RSI on closed bars (closes oldest->newest)."""
    if len(closes) < length + 1:
        return float("nan")
    gains = losses = 0.0
    for i in range(1, length + 1):
        d = closes[i] - closes[i-1]
        gains  += max(d, 0.0)
        losses += max(-d, 0.0)
    avg_gain = gains / length
    avg_loss = losses / length
    for i in range(length + 1, len(closes)):
        d = closes[i] - closes[i-1]
        gain = max(d, 0.0)
        loss = max(-d, 0.0)
        avg_gain = (avg_gain * (length - 1) + gain) / length
        avg_loss = (avg_loss * (length - 1) + loss) / length
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


# ===== Data helpers =====
def fetch_closed_4h(api: REST, symbol: str, limit: int = 200):
    """
    Returns (closes, last_bar_start_utc) for CLOSED 4h bars oldest->newest.
    We drop a potentially forming last bar by checking its start time.
    """
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=limit * 4 + 24)

    # get_bars returns BarsV2 with .df (MultiIndex: symbol,timestamp)
    bars = api.get_bars(
        symbol,
        "4Hour",  # string form is safe across versions
        start=start.isoformat(),
        end=now.isoformat(),
        adjustment="raw",
        feed=DATA_FEED,
        limit=limit + 5,
    )

    df = getattr(bars, "df", None)
    if df is None or df.empty:
        return [], None

    # if MultiIndex, slice symbol; else assume single-index
    try:
        sym_df = df.xs(symbol, level=0)
    except Exception:
        sym_df = df

    # Ensure chronological order
    sym_df = sym_df.sort_index()

    # Drop a potentially forming last bar:
    # Alpaca timestamps are bar START times; a 4h bar is "closed"
    # only if start <= now - 4h.
    if not sym_df.empty and sym_df.index[-1].to_pydatetime().replace(tzinfo=timezone.utc) > (now - timedelta(hours=4)):
        sym_df = sym_df.iloc[:-1]

    if sym_df.empty:
        return [], None

    closes = sym_df["close"].astype(float).tolist()
    last_start = sym_df.index[-1].to_pydatetime().replace(tzinfo=timezone.utc)
    return closes, last_start


def get_buying_power_usd(api: REST) -> float:
    acct = api.get_account()
    try:
        return float(acct.buying_power)
    except Exception:
        return float(acct.cash)


def buy_all_buying_power(api: REST, symbol: str, notional_usd: float):
    client_order_id = f"buy-{symbol}-{int(time.time()*1000)}"
    order = api.submit_order(
        symbol=symbol,
        side="buy",
        type="market",
        time_in_force="day",
        notional=round(notional_usd, 2),
        client_order_id=client_order_id,
    )
    oid = getattr(order, "id", "") or getattr(order, "client_order_id", "")
    status = getattr(order, "status", "submitted")
    log(f"{symbol} | BUY ${notional_usd:.2f} submitted (order {oid}, status {status})")


# ===== Main loop =====
def main():
    log(f"Starting VIG 4h RSI bot | base={APCA_API_BASE_URL} | feed={DATA_FEED}")
    last_seen_bar: Optional[datetime] = None

    while True:
        try:
            closes, last_bar_start = fetch_closed_4h(api, SYMBOL, limit=max(200, RSI_LEN + 50))
            if not closes or last_bar_start is None:
                log(f"{SYMBOL} | No closed 4h bars yet (market closed or data delay).")
            else:
                r = rsi(closes, RSI_LEN)
                bp = get_buying_power_usd(api)
                px = closes[-1]
                is_new_bar = (last_seen_bar is None) or (last_bar_start != last_seen_bar)
                log(f"{SYMBOL} | 4h RSI{RSI_LEN}={r:.2f} | price={px:.4f} | buying_power=${bp:.2f} | bar_start={last_bar_start.isoformat()} | {'NEW' if is_new_bar else 'same'}")

                if is_new_bar and r <= RSI_BUY_THRESH and bp >= MIN_NOTIONAL_USD:
                    buy_all_buying_power(api, SYMBOL, bp)
                    last_seen_bar = last_bar_start
                elif is_new_bar:
                    # update even if no buy, so we don't spam "NEW"
                    last_seen_bar = last_bar_start

        except Exception as e:
            log(f"Error: {type(e).__name__}: {e}")

        time.sleep(CHECK_EVERY_SEC)


if __name__ == "__main__":
    main()
