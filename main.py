#!/usr/bin/env python3
"""
Perpetual VIG RSI bot (Alpaca, 4-hour timeframe)

- Fetch CLOSED 4h bars for VIG
- On each NEWLY CLOSED 4h bar, compute RSI(14)
- If RSI <= 30: market BUY using 100% of current buying power
- No selling (buy & hold)
- Plain logs (Railway-friendly)
"""

import os
import math
import time
import signal
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional

# ---- Alpaca SDK ----
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment, DataFeed


# =========================
# Config (env or defaults)
# =========================
SYMBOL                 = os.getenv("SYMBOL", "VIG")
RSI_LEN                = int(os.getenv("RSI_LEN", "14"))
RSI_BUY_THRESH         = float(os.getenv("RSI_BUY_THRESH", "30"))
BARS_LIMIT             = int(os.getenv("BARS_LIMIT", "400"))          # enough history for RSI
CHECK_EVERY_SEC        = int(os.getenv("CHECK_EVERY_SEC", "60"))      # poll cadence
BAR_ALIGN_OFFSET_SEC   = int(os.getenv("BAR_ALIGN_OFFSET_SEC", "5"))  # cosmetic; we still gate by bar timestamp
MIN_NOTIONAL_USD       = Decimal(os.getenv("MIN_NOTIONAL_USD", "10")) # skip dust buys
DATA_FEED              = os.getenv("DATA_FEED", "IEX").upper()        # IEX (free) or SIP (paid)
LOG_PREFIX             = os.getenv("LOG_PREFIX", "[vig-rsi4h-bot]")

ALPACA_KEY   = os.getenv("ALPACA_API_KEY_ID") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET= os.getenv("ALPACA_API_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_PAPER = os.getenv("ALPACA_PAPER", "1").lower() in ("1", "true", "yes")

if not (ALPACA_KEY and ALPACA_SECRET):
    raise RuntimeError("Missing ALPACA_API_KEY_ID / ALPACA_API_SECRET_KEY env vars.")


# =========================
# Clients
# =========================
trading = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=ALPACA_PAPER)
data    = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)  # same creds fine


# =========================
# Logging & signals
# =========================
_shutting_down = False

def log(msg: str):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"{LOG_PREFIX} {now} | {msg}", flush=True)

def _handle(sig, _):
    global _shutting_down
    _shutting_down = True
    log(f"Received {sig}. Shutting down gracefully.")
signal.signal(signal.SIGTERM, _handle)
signal.signal(signal.SIGINT, _handle)


# =========================
# Indicators
# =========================
def rsi(closes: List[float], length: int = 14) -> float:
    """Wilder RSI; closes must be oldest→newest CLOSED bars."""
    if len(closes) < length + 1:
        return float("nan")

    gains = losses = 0.0
    for i in range(1, length + 1):
        d = closes[i] - closes[i - 1]
        gains  += max(d, 0.0)
        losses += max(-d, 0.0)
    avg_gain = gains / length
    avg_loss = losses / length

    for i in range(length + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        gain = max(d, 0.0)
        loss = max(-d, 0.0)
        avg_gain = (avg_gain * (length - 1) + gain) / length
        avg_loss = (avg_loss * (length - 1) + loss) / length

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


# =========================
# Data helpers (4-hour bars)
# =========================
TF_4H = TimeFrame(4, TimeFrameUnit.Hour)

def fetch_closed_4h_closes(symbol: str, limit: int) -> Tuple[List[float], Optional[datetime]]:
    """
    Fetch CLOSED 4h bars (oldest→newest). Returns (closes, last_bar_end_time_utc).
    last_bar_end_time_utc is the timestamp of the most recent CLOSED 4h bar (DataFrame index).
    """
    now  = datetime.now(timezone.utc)
    start = now - timedelta(hours=limit * 4 + 24)  # cushion

    req = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TF_4H,
        start=start,
        end=now,
        adjustment=Adjustment.RAW,
        feed=DataFeed.SIP if DATA_FEED == "SIP" else DataFeed.IEX
    )
    bars = data.get_stock_bars(req)
    df = bars.df
    if df is None or df.empty:
        return [], None

    try:
        sym_df = df.xs(symbol, level=0)
    except Exception:
        sym_df = df

    # sym_df index is pandas.DatetimeIndex (tz-aware UTC from Alpaca)
    closes = sym_df["close"].astype(float).tolist()
    last_ts = sym_df.index[-1].to_pydatetime()
    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=timezone.utc)

    return (closes[-limit:] if len(closes) > limit else closes, last_ts)


# =========================
# Trading helpers
# =========================
def get_buying_power() -> Decimal:
    acct = trading.get_account()
    return Decimal(acct.buying_power)

def buy_with_notional(symbol: str, notional: Decimal):
    """Place a market BUY using USD notional (TIF=DAY)."""
    req = MarketOrderRequest(
        symbol=symbol,
        notional=float(notional),
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY
    )
    resp = trading.submit_order(req)
    oid = getattr(resp, "id", None) or getattr(resp, "client_order_id", None)
    log(f"{symbol} | BUY market notional=${notional} (order_id={oid})")


# =========================
# Main loop
# =========================
def main():
    log(f"Starting VIG 4h RSI bot: RSI{RSI_LEN} ≤ {RSI_BUY_THRESH} → BUY 100% buying power | feed={DATA_FEED} | paper={ALPACA_PAPER}")
    last_seen_bar: Optional[datetime] = None

    # small delay so we’re not racing bar close; not critical since we gate by timestamp
    time.sleep(BAR_ALIGN_OFFSET_SEC)

    while not _shutting_down:
        try:
            closes, last_bar = fetch_closed_4h_closes(SYMBOL, BARS_LIMIT)
            if not closes or last_bar is None:
                log(f"{SYMBOL} | No bars returned (market closed or data delay).")
            else:
                same_bar = (last_seen_bar is not None and last_bar == last_seen_bar)
                r = rsi(closes, RSI_LEN)
                bp = get_buying_power()
                px = closes[-1]
                log(f"{SYMBOL} | 4h RSI{RSI_LEN}={r:.2f} | price={px:.4f} | buying_power=${bp} | bar_close={last_bar.isoformat()} | {'same 4h bar' if same_bar else 'new 4h bar'}")

                # Only act ONCE per newly closed 4h bar
                if not same_bar and r <= RSI_BUY_THRESH and bp >= MIN_NOTIONAL_USD:
                    buy_with_notional(SYMBOL, bp.quantize(Decimal('0.01')))
                    last_seen_bar = last_bar
                elif not same_bar:
                    # update the gate even if no buy (so we don't re-log as "new bar" next loop)
                    last_seen_bar = last_bar

        except Exception as e:
            log(f"Error: {e}")

        # Poll cadence; we’ll fire only when a new 4h bar appears
        time.sleep(CHECK_EVERY_SEC)


if __name__ == "__main__":
    main()
