#!/usr/bin/env python3
"""
VIG RSI + Dual-TF Trend bot (Alpaca)
- Aligns to 15-minute bar closes in America/New_York (e.g., :00:05, :15:05, :30:05, :45:05)
- Indicators:
    * RSI(14) on 15m bars
    * SMA(60) on 15m bars                <-- "short MA" (kept)
    * SMA(60) on 1h bars                 <-- replaces former 240x15m ("long MA")
- Buy 20% of buying power on each NEW closed 15m bar if:
    RSI(14,15m) <= threshold AND SMA60(15m) < SMA60(1h)

No selling (accumulate). Logs to stdout (Railway-friendly).
"""

import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo
import math

from alpaca_trade_api.rest import REST  # using alpaca_trade_api (your working stack)

# ===== Config (env) =====
SYMBOL            = os.getenv("SYMBOL", "VIG")
RSI_LEN           = int(os.getenv("RSI_LEN", "14"))
RSI_BUY_THRESH    = float(os.getenv("RSI_BUY_THRESH", "30"))
SMA_SHORT_LEN_15  = int(os.getenv("SMA_SHORT_LEN_15", "60"))   # 60 x 15m
SMA_LONG_LEN_1H   = int(os.getenv("SMA_LONG_LEN_1H", "60"))    # 60 x 1h
DATA_FEED         = os.getenv("DATA_FEED", "iex").lower()       # 'iex' (default) or 'sip'
BUY_FRACTION      = float(os.getenv("BUY_FRACTION", "0.20"))    # 20% of buying power per buy
ALIGN_TZ          = os.getenv("ALIGN_TZ", "America/New_York")   # align to exchange time
RUN_DELAY_SEC     = int(os.getenv("RUN_DELAY_SEC", "5"))        # run N seconds after the 15m close

# History knobs (generous so warmup is met even on thin symbols / holidays)
HISTORY_DAYS_15M  = int(os.getenv("HISTORY_DAYS_15M", "60"))
HISTORY_DAYS_1H   = int(os.getenv("HISTORY_DAYS_1H",  "180"))
BAR_LIMIT_MAX     = int(os.getenv("BAR_LIMIT_MAX", "10000"))

# Alpaca creds
ALPACA_API_KEY    = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
APCA_API_BASE_URL = os.getenv("APCA_API_BASE_URL", "https://api.alpaca.markets")

if not (ALPACA_API_KEY and ALPACA_SECRET_KEY):
    raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY (or APCA_* equivalents).")

api = REST(key_id=ALPACA_API_KEY, secret_key=ALPACA_SECRET_KEY, base_url=APCA_API_BASE_URL)

# ===== Utils =====
def log(msg: str):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"[vig-rsi15m] {now} | {msg}", flush=True)

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

def sma(closes: List[float], length: int) -> float:
    if len(closes) < length:
        return float("nan")
    return sum(closes[-length:]) / float(length)

# ===== Data helpers =====
def fetch_closed_bars(api: REST, symbol: str, timeframe_str: str, bar_minutes: int, history_days: int, limit_max: int):
    """
    Returns (closes, last_bar_start_utc, num_bars) for CLOSED bars oldest->newest.
    Drops a potentially forming last bar (timestamps are bar START times in UTC).
    """
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=history_days)

    bars = api.get_bars(
        symbol,
        timeframe_str,
        start=start.isoformat(),
        end=now.isoformat(),
        adjustment="raw",
        feed=DATA_FEED,
        limit=limit_max,
    )

    df = getattr(bars, "df", None)
    if df is None or df.empty:
        return [], None, 0

    # If MultiIndex, slice by symbol; else assume single-index
    try:
        sym_df = df.xs(symbol, level=0)
    except Exception:
        sym_df = df

    sym_df = sym_df.sort_index()

    # Drop forming bar
    cutoff = now - timedelta(minutes=bar_minutes)
    if not sym_df.empty and sym_df.index[-1].to_pydatetime().replace(tzinfo=timezone.utc) > cutoff:
        sym_df = sym_df.iloc[:-1]

    if sym_df.empty:
        return [], None, 0

    closes = sym_df["close"].astype(float).tolist()
    last_start = sym_df.index[-1].to_pydatetime().replace(tzinfo=timezone.utc)
    return closes, last_start, len(closes)

def get_buying_power_usd(api: REST) -> float:
    acct = api.get_account()
    try:
        return float(acct.buying_power)
    except Exception:
        return float(acct.cash)

def submit_notional_buy(api: REST, symbol: str, notional_usd: float):
    import time as _t
    client_order_id = f"buy-{symbol}-{int(_t.time()*1000)}"
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

# ===== 15m alignment helpers =====
def next_quarter_hour(now_ny: datetime) -> datetime:
    """Given now in NY tz, return the next :00/:15/:30/:45 in NY tz."""
    minute = now_ny.minute
    next_min = ((minute // 15) + 1) * 15
    if next_min >= 60:
        base = now_ny.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        base = now_ny.replace(minute=next_min, second=0, microsecond=0)
    return base

def seconds_until_next_run(delay_sec: int) -> float:
    """Seconds from now (UTC) to next NY 15m boundary + delay."""
    ny = ZoneInfo(ALIGN_TZ)
    now_utc = datetime.now(timezone.utc)
    now_ny = now_utc.astimezone(ny)
    target_ny = next_quarter_hour(now_ny) + timedelta(seconds=delay_sec)
    target_utc = target_ny.astimezone(timezone.utc)
    return max(0.0, (target_utc - now_utc).total_seconds())

# ===== Main loop =====
def main():
    log(
        f"Starting {SYMBOL} 15m RSI+DualTF bot | base={APCA_API_BASE_URL} | feed={DATA_FEED} | "
        f"rsi_len={RSI_LEN} | sma60_15m={SMA_SHORT_LEN_15} | sma60_1h={SMA_LONG_LEN_1H} | "
        f"buy_fraction={BUY_FRACTION:.2f} | align_tz={ALIGN_TZ} | delay={RUN_DELAY_SEC}s"
    )
    last_seen_15m_bar: Optional[datetime] = None

    while True:
        try:
            # ---- Fetch 15m data (RSI + short SMA) ----
            closes_15, last_15, n15 = fetch_closed_bars(
                api, SYMBOL, "15Min", 15, HISTORY_DAYS_15M, BAR_LIMIT_MAX
            )

            # ---- Fetch 1h data (long SMA) ----
            closes_1h, last_1h, n1h = fetch_closed_bars(
                api, SYMBOL, "1Hour", 60, HISTORY_DAYS_1H, BAR_LIMIT_MAX
            )

            if not closes_15 or last_15 is None:
                log(f"{SYMBOL} | No closed 15m bars (market closed or data delay).")
            else:
                # Warmup checks
                r  = rsi(closes_15, RSI_LEN) if len(closes_15) >= RSI_LEN + 1 else float("nan")
                s15 = sma(closes_15, SMA_SHORT_LEN_15)
                s1h = sma(closes_1h, SMA_LONG_LEN_1H) if closes_1h else float("nan")

                bp  = get_buying_power_usd(api)
                px  = closes_15[-1]
                is_new_bar = (last_seen_15m_bar is None) or (last_15 != last_seen_15m_bar)

                # Log status (include counts so you can spot data sufficiency)
                log(
                    f"{SYMBOL} | 15m n={n15}, 1h n={n1h} | RSI{RSI_LEN}={r:.2f} | "
                    f"SMA60(15m)={s15:.4f} | SMA60(1h)={s1h:.4f} | price={px:.4f} | "
                    f"buying_power=${bp:.2f} | 15m_bar_start={last_15.isoformat()} | {'NEW' if is_new_bar else 'same'}"
                )

                ready = (not math.isnan(r)) and (not math.isnan(s15)) and (not math.isnan(s1h))
                should_buy = (
                    is_new_bar and ready and
                    (r <= RSI_BUY_THRESH) and
                    (s15 < s1h) and
                    (bp > 0.0)
                )

                if should_buy:
                    notional = bp * BUY_FRACTION
                    if notional > 0:
                        submit_notional_buy(api, SYMBOL, notional)
                    last_seen_15m_bar = last_15
                elif is_new_bar:
                    last_seen_15m_bar = last_15

        except Exception as e:
            log(f"Error: {type(e).__name__}: {e}")

        # Sleep until just after the next 15m close in NY time
        sleep_s = seconds_until_next_run(RUN_DELAY_SEC)
        if sleep_s < 0.5:
            sleep_s = 5.0
        time.sleep(sleep_s)

if __name__ == "__main__":
    main()
