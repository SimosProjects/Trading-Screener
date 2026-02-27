# strategies.py
from __future__ import annotations

import csv
import datetime as dt
import math
import os
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf
import ta

from utils import get_logger, iso_week_id, safe_float, safe_int, atomic_write
from config import (
    # market data
    DATA_PERIOD, DATA_INTERVAL,

    # files
    STOCK_POSITIONS_FILE, STOCK_TRADES_FILE,
    STOCK_FILLS_FILE,
    STOCK_MONTHLY_DIR,
    RETIREMENT_POSITIONS_FILE,
    CSP_LEDGER_FILE, CSP_POSITIONS_FILE, CC_POSITIONS_FILE,

    # accounts / sizing
    INDIVIDUAL, IRA, ROTH,
    ACCOUNT_SIZES,
    INDIVIDUAL_STOCK_CAP,
    RETIREMENT_STOCK_CAPS,
    RETIREMENT_MAX_EQUITY_UTIL_PCT,
    RETIREMENT_BREAKEVEN_ONLY_DD_PCT,
    RETIREMENT_STOP_LOSS_PCT,
    RETIREMENT_POSITION_SIZE_PCT,
    RETIREMENT_MAX_STOCK_POSITIONS,
    RETIREMENT_STOCKS,

    # stock rules
    STOCK_REQUIRE_NEXTDAY_VALIDATION,
    STOCK_RISK_PCT_INDIVIDUAL,
    STOCK_MAX_POSITION_PCT_INDIVIDUAL,
    STOCK_TARGET_R_MULTIPLE,
    STOCK_BREAKEVEN_AFTER_R,
    STOCK_USE_BREAKEVEN_TRAIL,
    STOCK_STOP_ATR_PULLBACK,
    STOCK_STOP_ATR_BREAKOUT,

    # CSP rules
    CSP_POSITIONS_COLUMNS, CC_POSITIONS_COLUMNS,
    CSP_TARGET_DTE_MIN, CSP_TARGET_DTE_MAX,
    CSP_TAKE_PROFIT_PCT, CSP_TP_MAX_SPREAD_PCT,
    CSP_MAX_CASH_PER_TRADE,
    CSP_MIN_OI, CSP_MIN_VOLUME, CSP_MIN_BID, CSP_MIN_IV,
    CSP_STRIKE_MODE,
    CSP_STRIKE_BASE_NORMAL,
    CSP_MIN_PREMIUM_CONSERVATIVE, CSP_MIN_PREMIUM_BALANCED, CSP_MIN_PREMIUM_AGGRESSIVE,
    CSP_MIN_YIELD_CONSERVATIVE, CSP_MIN_YIELD_BALANCED, CSP_MIN_YIELD_AGGRESSIVE,
    CSP_MAX_AGGRESSIVE_TOTAL, CSP_MAX_AGGRESSIVE_PER_WEEK,
    CSP_MAX_POSITIONS_PER_SECTOR, CSP_TICKER_SECTOR,
    CSP_EARLY_ASSIGN_ITM_PCT, CSP_EARLY_ASSIGN_WARN_ONLY, CSP_EARLY_ASSIGN_MAX_DTE,

    # CC policy
    CC_TARGET_DTE_MIN, CC_TARGET_DTE_MAX,
    CC_ATR_MULT_NORMAL, CC_ATR_MULT_MILD, CC_ATR_MULT_DEEP, CC_ATR_MULT_SEVERE,
    CC_UNDERWATER_MILD_PCT, CC_UNDERWATER_DEEP_PCT,
    CC_STRIKE_FLOOR_BELOW_CURRENT_PCT,
    CC_MIN_BID,

    # slippage / fill model
    STOCK_SLIPPAGE_PER_SHARE,
    OPT_SELL_FILL_PCT,
    OPT_BUY_FILL_PCT,
    OPT_COMMISSION_PER_CONTRACT,
)

log = get_logger(__name__)

# Derived: max underlying price allowed for CSPs (e.g., $6,500 cap => $65/share for 1 contract)
CSP_MAX_SHARE_PRICE = float(CSP_MAX_CASH_PER_TRADE) / 100.0


# ============================================================
# Data / indicators
# ============================================================

# Module-level cache reference.  Set by the screener orchestrator before
# the run starts via set_data_cache().  When None, every call falls back
# to a direct yfinance download (safe but slow — same as before).
_cache = None


def set_data_cache(cache) -> None:
    """Inject the pre-warmed DataCache for this run."""
    global _cache
    _cache = cache


# ── Option chain cache (per-run) ────────────────────────────────────────────
# Keyed by "{ticker}-{expiry}" for full chains, "{ticker}" for expiry listings.
# Both are reset once per run via reset_chain_cache() so every run sees fresh
# quotes.  No TTL needed — the screener is single-process and short-lived.

_chain_cache: Dict[str, object] = {}   # key: "{ticker}-{expiry}"
_expiry_cache: Dict[str, tuple] = {}   # key: "{ticker}"


def reset_chain_cache() -> None:
    """Clear option chain caches.  Called once at screener startup."""
    global _chain_cache, _expiry_cache
    _chain_cache = {}
    _expiry_cache = {}


def download_ohlcv(ticker: str, period: str = DATA_PERIOD, interval: str = DATA_INTERVAL) -> pd.DataFrame:
    # Use the session cache when available to avoid redundant network calls.
    if _cache is not None and _cache.has(ticker):
        return _cache.ohlcv(ticker)
    try:
        df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    except Exception as e:
        log.warning("download_ohlcv failed for %s: %s", ticker, e)
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df.dropna(inplace=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]

    df["SMA_50"] = ta.trend.sma_indicator(close, window=50)
    df["SMA_200"] = ta.trend.sma_indicator(close, window=200)
    df["EMA_21"] = ta.trend.ema_indicator(close, window=21)
    df["EMA_50"] = ta.trend.ema_indicator(close, window=50)
    df["EMA_10"] = ta.trend.ema_indicator(close, window=10)

    df["ATR_14"] = ta.volatility.average_true_range(high=high, low=low, close=close, window=14)
    df["RSI_2"] = ta.momentum.rsi(close, window=2)
    df["RSI_14"] = ta.momentum.rsi(close, window=14)
    df["ADX_14"] = ta.trend.adx(high=high, low=low, close=close, window=14)

    df["VOL_SMA_10"] = volume.rolling(window=10).mean()
    # shift(1): compare today's close against the PRIOR 20-day high so the breakout
    # condition (close > HIGH_20) can actually be true on the signal bar.
    df["HIGH_20"] = close.shift(1).rolling(window=20).max()
    df["LOW_20"]  = close.shift(1).rolling(window=20).min()

    return df


# ============================================================
# Market regime (SPY/QQQ/VIX)
# ============================================================

def market_context_from_dfs(spy_df: pd.DataFrame, qqq_df: pd.DataFrame, vix_df: pd.DataFrame) -> Dict[str, float | bool]:
    spy_df = add_indicators(spy_df)
    qqq_df = add_indicators(qqq_df)
    if spy_df.empty or qqq_df.empty or vix_df is None or vix_df.empty:
        # fail-safe: return conservative "OFF" regime
        return {
            "spy_close": 0.0,
            "qqq_close": 0.0,
            "vix_close": 99.0,
            "spy_above_200": False,
            "spy_above_50": False,
            "spy_above_21": False,
            "qqq_above_50": False,
            "vix_below_18": False,
            "vix_below_25": False,
        }

    spy_last = spy_df.iloc[-1]
    qqq_last = qqq_df.iloc[-1]
    vix_close = float(vix_df["Close"].iloc[-1])

    spy_close = float(spy_last["Close"])
    qqq_close = float(qqq_last["Close"])

    return {
        "spy_close": spy_close,
        "qqq_close": qqq_close,
        "vix_close": vix_close,
        "spy_above_200": bool(spy_close > float(spy_last["SMA_200"])),
        "spy_above_50": bool(spy_close > float(spy_last["SMA_50"])),
        "spy_above_21": bool(spy_close > float(spy_last["EMA_21"])),
        "qqq_above_50": bool(qqq_close > float(qqq_last["SMA_50"])),
        "vix_below_18": bool(vix_close < 18),
        "vix_below_25": bool(vix_close < 25),
    }


def market_context(today: dt.date) -> Dict[str, float | bool]:
    spy_df = download_ohlcv("SPY")
    qqq_df = download_ohlcv("QQQ")
    vix_df = download_ohlcv("^VIX")
    return market_context_from_dfs(spy_df, qqq_df, vix_df)

def trading_allowed(mkt: Dict) -> bool:
    return bool(
        mkt.get("spy_above_200")
        and mkt.get("spy_above_50")
        and mkt.get("spy_above_21")
        and mkt.get("qqq_above_50")
        and mkt.get("vix_below_25")
    )


# ============================================================
# Stock entry logic
# ============================================================

def is_eligible(stock_row: pd.Series) -> bool:
    """Healthy-trend filter used for stock entries and CSP scan."""
    try:
        close = float(stock_row["Close"])
        sma50 = float(stock_row["SMA_50"])
        ema21 = float(stock_row["EMA_21"])
        adx = float(stock_row["ADX_14"])
    except Exception as e:
        log.debug("is_eligible: indicator field missing or non-numeric: %s", e)
        return False
    return bool(close > sma50 and ema21 > sma50 and adx > 20)


def is_csp_eligible(stock_row: pd.Series, *, allow_below_200: bool = False) -> bool:
    """Eligibility filter for CSP scanning.

    Default (allow_below_200=False) is conservative:
      - Require Close > SMA200 (structural uptrend)
      - Avoid ultra-low ADX (often choppy / directionless)

    Risk-off variant (allow_below_200=True) is *defensive-only* oriented:
      - Allow names below SMA200, but require Close > SMA50 (avoid true waterfalls)
      - Looser ADX floor
    """
    try:
        close = float(stock_row["Close"])
        sma50 = float(stock_row.get("SMA_50", 0) or 0)
        sma200 = float(stock_row.get("SMA_200", 0) or 0)
        adx = float(stock_row.get("ADX_14", 0) or 0)
    except Exception as e:
        log.debug("is_csp_eligible: indicator field missing or non-numeric: %s", e)
        return False

    if sma50 <= 0:
        return False

    if not allow_below_200:
        if sma200 <= 0:
            return False
        if close < sma200:
            return False
        if adx and adx < 15:
            return False
        return True

    # risk-off: keep it very restrained
    if close < sma50:
        return False
    if adx and adx < 10:
        return False
    return True


def pullback_signal(stock_row: pd.Series) -> bool:
    # Very short-term oversold + close near EMA21
    try:
        rsi2 = float(stock_row["RSI_2"])
        ema21 = float(stock_row["EMA_21"])
        close = float(stock_row["Close"])
    except Exception as e:
        log.debug("pullback_signal: indicator field missing or non-numeric: %s", e)
        return False
    rsi_ok = rsi2 < 5
    near_ema = abs(close - ema21) / max(ema21, 1e-9) < 0.005
    return bool(rsi_ok and near_ema)


def breakout_signal(stock_row: pd.Series) -> bool:
    # 20D breakout + volume expansion
    try:
        close = float(stock_row["Close"])
        high20 = float(stock_row["HIGH_20"])
        vol = float(stock_row["Volume"])
        vol_sma = float(stock_row["VOL_SMA_10"])
    except Exception as e:
        log.debug("breakout_signal: indicator field missing or non-numeric: %s", e)
        return False
    return bool(close > high20 and vol > 1.5 * vol_sma)


def nextday_valid_for_entry(signal: str, last: pd.Series) -> bool:
    """Heuristic to prefer signals that are still tradable the next day (EOD run)."""
    if not STOCK_REQUIRE_NEXTDAY_VALIDATION:
        return True

    try:
        close = float(last["Close"])
        ema21 = float(last["EMA_21"])
        atr = float(last.get("ATR_14", 0) or 0)
        high20 = float(last["HIGH_20"])
        vol = float(last["Volume"])
        vol_sma = float(last.get("VOL_SMA_10", 0) or 0)
    except Exception as e:
        log.debug("nextday_valid_for_entry: indicator field missing or non-numeric: %s", e)
        return False

    # BREAKOUT: avoid blow-off; require real vol
    if atr > 0 and close > high20 + atr:
        return False
    if vol_sma > 0 and vol < 1.3 * vol_sma:
        return False
    return True


# ============================================================
# Retirement holdings inventory (long holds)
# ============================================================

RETIREMENT_FIELDS = [
    "account", "ticker", "shares", "entry_price", "entry_date",
    "current_price", "pct_change", "breakeven_target", "flag_breakeven_only", "notes",
]


def ensure_retirement_file() -> None:
    if os.path.isfile(RETIREMENT_POSITIONS_FILE):
        return
    with open(RETIREMENT_POSITIONS_FILE, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RETIREMENT_FIELDS)
        w.writeheader()


def load_retirement_positions() -> List[dict]:
    ensure_retirement_file()
    with open(RETIREMENT_POSITIONS_FILE, "r", newline="") as f:
        return list(csv.DictReader(f))


def write_retirement_positions(rows: List[dict]) -> None:
    ensure_retirement_file()
    def _write(f):
        w = csv.DictWriter(f, fieldnames=RETIREMENT_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in RETIREMENT_FIELDS})
    atomic_write(RETIREMENT_POSITIONS_FILE, _write)


def retirement_flag_breakeven_only(entry_price: float, current_price: float) -> bool:
    if entry_price <= 0:
        return False
    dd = (current_price - entry_price) / entry_price
    return bool(dd <= -float(RETIREMENT_BREAKEVEN_ONLY_DD_PCT))


def update_retirement_marks() -> Tuple[Dict[str, dict], List[str]]:
    """Update current_price/pct_change + breakeven-only flags. Returns (by_key, flagged_tickers)."""
    rows = load_retirement_positions()
    if not rows:
        return {}, []

    tickers = sorted({(r.get("ticker") or "").strip().upper() for r in rows if (r.get("ticker") or "").strip()})
    last_close: Dict[str, float] = {}

    for tkr in tickers:
        try:
            df = download_ohlcv(tkr, period="7d", interval="1d")
            if not df.empty and "Close" in df.columns:
                last_close[tkr] = float(df["Close"].dropna().iloc[-1])
        except Exception as e:
            log.warning("retirement mark price fetch failed for %s: %s", tkr, e)
            continue

    flagged = set()
    by_key: Dict[str, dict] = {}

    for r in rows:
        acct = (r.get("account") or "").strip().upper()
        tkr = (r.get("ticker") or "").strip().upper()
        if not acct or not tkr:
            continue

        try:
            entry = float(r.get("entry_price") or 0.0)
        except Exception as e:
            log.warning("update_retirement_marks: bad entry_price for %s: %s", tkr, e)
            entry = 0.0

        px = last_close.get(tkr)
        if px is None:
            continue

        r["current_price"] = f"{px:.2f}"
        if entry > 0:
            pct = (px - entry) / entry
            r["pct_change"] = f"{pct*100:.2f}"
            be_only = retirement_flag_breakeven_only(entry, px)
            r["flag_breakeven_only"] = "1" if be_only else "0"
            r["breakeven_target"] = f"{entry:.2f}" if be_only else ""
            if be_only:
                flagged.add(tkr)

        by_key[f"{acct}:{tkr}"] = r

    write_retirement_positions(rows)
    return by_key, sorted(flagged)


def close_retirement_stops(today: dt.date) -> Dict[str, List[str]]:
    """Close retirement positions that have hit the hard stop loss.

    Runs daily after update_retirement_marks() so current_price is fresh.
    A position is stopped out when:
        current_price <= entry_price * (1 - RETIREMENT_STOP_LOSS_PCT)

    Closed positions are written to stock_trades.csv (close_type=STOP) and
    removed from retirement_positions.csv.  Monthly rebuild is left to the
    caller (screener.py already calls rebuild_stock_monthly_from_trades).

    Returns {"stopped": ["AAPL @152.00 (-21.3%)", ...]}
    """
    if not RETIREMENT_STOP_LOSS_PCT or float(RETIREMENT_STOP_LOSS_PCT) <= 0:
        return {"stopped": []}

    rows = load_retirement_positions()
    if not rows:
        return {"stopped": []}

    stopped: List[str] = []
    surviving: List[dict] = []
    changed = False

    for r in rows:
        acct = (r.get("account") or "").strip().upper()
        tkr  = (r.get("ticker") or "").strip().upper()

        if acct not in (IRA, ROTH):
            # Stop logic only applies to retirement accounts.
            surviving.append(r)
            continue

        try:
            entry = float(r.get("entry_price") or 0.0)
            cur   = float(r.get("current_price") or 0.0)
            sh    = int(float(r.get("shares") or 0))
        except Exception as e:
            log.warning("close_retirement_stops: bad numeric field for %s %s: %s", acct, tkr, e)
            surviving.append(r)
            continue

        if entry <= 0 or cur <= 0 or sh <= 0:
            surviving.append(r)
            continue

        stop_level = entry * (1.0 - float(RETIREMENT_STOP_LOSS_PCT))
        if cur > stop_level:
            # Not stopped — keep position.
            surviving.append(r)
            continue

        # Position has breached the hard stop — close it.
        pnl_abs = (cur - entry) * sh
        pnl_pct = (cur - entry) / entry

        log.warning(
            "Retirement stop triggered: %s %s %d sh — entry %.2f, now %.2f "
            "(%.1f%%), stop %.2f",
            acct, tkr, sh, entry, cur, pnl_pct * 100, stop_level,
        )

        entry_date = (r.get("entry_date") or "")

        append_stock_trade({
            "id":          f"{acct}-{tkr}-{today.isoformat()}-STOP",
            "account":     acct,
            "ticker":      tkr,
            "entry_date":  entry_date,
            "entry_price": f"{entry:.2f}",
            "shares":      str(sh),
            "exit_date":   today.isoformat(),
            "exit_price":  f"{cur:.2f}",
            "reason":      "STOP",
            "close_type":  "STOP",
            "pnl_abs":     f"{pnl_abs:.2f}",
            "pnl_pct":     f"{pnl_pct*100:.2f}",
        })

        append_stock_fill({
            "date":    today.isoformat(),
            "account": acct,
            "ticker":  tkr,
            "action":  "CLOSE",
            "price":   f"{cur:.2f}",
            "shares":  str(sh),
            "reason":  f"RETIREMENT_STOP ({float(RETIREMENT_STOP_LOSS_PCT)*100:.0f}%)",
        })

        stopped.append(f"{tkr} @{cur:.2f} ({pnl_pct*100:+.1f}%)")
        changed = True

    if changed:
        write_retirement_positions(surviving)

    return {"stopped": stopped}


def retirement_market_value_by_account(ret_by_key: Dict[str, dict]) -> Dict[str, float]:
    mv = {INDIVIDUAL: 0.0, IRA: 0.0, ROTH: 0.0}
    for _, r in ret_by_key.items():
        acct = (r.get("account") or "").strip().upper()
        if acct not in mv:
            continue
        try:
            sh = float(r.get("shares") or 0.0)
            px = float(r.get("current_price") or 0.0)
            mv[acct] += sh * px
        except Exception as e:
            log.warning("retirement_market_value_by_account: bad numeric field for %s/%s: %s",
                        acct, r.get("ticker", "?"), e)
            continue
    return mv


STOCK_POS_FIELDS = [
    "id",
    "account",
    "ticker",
    "signal",
    "plan_date",
    "entry_date",
    "entry_price",
    "shares",
    "adds",
    "last_add_date",
    "initial_entry_price",
    "initial_shares",
    "stop_price",
    "target_price",
    "risk_per_share",
    "r_multiple_target",
    "status",
    "exit_date",
    "exit_price",
    "exit_reason",
    "pnl_abs",
    "pnl_pct",
    "notes",
]
STOCK_TRADE_FIELDS = [
    "id",
    "account",
    "ticker",
    "entry_date",
    "entry_price",
    "shares",
    "exit_date",
    "exit_price",
    "reason",
    "close_type",
    "pnl_abs",
    "pnl_pct",
]

STOCK_FILL_FIELDS = [
    "date",
    "account",
    "ticker",
    "action",   # OPEN / ADD / CLOSE
    "price",
    "shares",
    "reason",
]



def ensure_stock_files() -> None:
    if not os.path.isfile(STOCK_POSITIONS_FILE):
        with open(STOCK_POSITIONS_FILE, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=STOCK_POS_FIELDS)
            w.writeheader()
    if not os.path.isfile(STOCK_TRADES_FILE):
        with open(STOCK_TRADES_FILE, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=STOCK_TRADE_FIELDS)
            w.writeheader()
    if not os.path.isfile(STOCK_FILLS_FILE):
        with open(STOCK_FILLS_FILE, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=STOCK_FILL_FIELDS)
            w.writeheader()


def load_stock_positions() -> List[dict]:
    ensure_stock_files()
    with open(STOCK_POSITIONS_FILE, "r", newline="") as f:
        return list(csv.DictReader(f))


def write_stock_positions(rows: List[dict]) -> None:
    ensure_stock_files()
    def _write(f):
        w = csv.DictWriter(f, fieldnames=STOCK_POS_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in STOCK_POS_FIELDS})
    atomic_write(STOCK_POSITIONS_FILE, _write)


def append_stock_trade(row: dict) -> None:
    ensure_stock_files()
    with open(STOCK_TRADES_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=STOCK_TRADE_FIELDS)
        w.writerow({k: row.get(k, "") for k in STOCK_TRADE_FIELDS})

def append_stock_fill(row: dict) -> None:
    ensure_stock_files()
    with open(STOCK_FILLS_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=STOCK_FILL_FIELDS)
        w.writerow({k: row.get(k, "") for k in STOCK_FILL_FIELDS})

def rebuild_stock_monthly_from_trades() -> None:
    """Rebuild per-account-group monthly CSVs from stock_trades.csv.

    Produces two files per month:
      stock_monthly/YYYY-MM-INDIVIDUAL.csv  — individual account swing trades
      stock_monthly/YYYY-MM-IRA.csv         — IRA + ROTH trades (each row tagged)

    Profit is recorded on exit_date so unrealized positions never appear here.
    Called-away CC lots appear with close_type=CC_CALLED_AWAY on the call date.
    """
    from config import IRA_ACCOUNTS
    ensure_stock_files()
    if not os.path.isfile(STOCK_TRADES_FILE):
        return
    with open(STOCK_TRADES_FILE, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return

    os.makedirs(STOCK_MONTHLY_DIR, exist_ok=True)

    out_fields = [
        "date", "account", "ticker", "shares",
        "entry_price", "exit_price", "close_type",
        "pnl_abs", "pnl_pct",
    ]

    # Bucket by (month, file_group) where file_group is INDIVIDUAL or IRA
    by_bucket: Dict[tuple, List[dict]] = {}
    for r in rows:
        d = (r.get("exit_date") or "").strip()
        if len(d) < 7:
            continue
        month = d[:7]
        acct  = (r.get("account") or INDIVIDUAL).strip().upper()
        group = "IRA" if acct in IRA_ACCOUNTS else "INDIVIDUAL"
        by_bucket.setdefault((month, group), []).append(r)

    for (month, group), mrows in sorted(by_bucket.items()):
        out_rows: List[dict] = []
        total = 0.0
        for r in sorted(mrows, key=lambda x: x.get("exit_date") or ""):
            try:
                pnl = float(r.get("pnl_abs") or 0.0)
            except Exception as e:
                log.warning("rebuild_stock_monthly: bad pnl_abs for %s %s: %s",
                            r.get("account", "?"), r.get("ticker", "?"), e)
                pnl = 0.0
            total += pnl
            out_rows.append({
                "date":        (r.get("exit_date") or ""),
                "account":     (r.get("account") or ""),
                "ticker":      (r.get("ticker") or ""),
                "shares":      (r.get("shares") or ""),
                "entry_price": (r.get("entry_price") or ""),
                "exit_price":  (r.get("exit_price") or ""),
                "close_type":  (r.get("close_type") or ""),
                "pnl_abs":     f"{pnl:.2f}",
                "pnl_pct":     (r.get("pnl_pct") or ""),
            })

        out_rows.append({
            "date": "", "account": "", "ticker": "TOTAL",
            "shares": "", "entry_price": "", "exit_price": "",
            "close_type": "", "pnl_abs": f"{total:.2f}", "pnl_pct": "",
        })

        path = os.path.join(STOCK_MONTHLY_DIR, f"{month}-{group}.csv")
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=out_fields)
            w.writeheader()
            for rr in out_rows:
                w.writerow(rr)


def _stock_position_id(account: str, ticker: str, entry_date: str) -> str:
    return f"{account}-{ticker}-{entry_date}"


def stock_market_value_by_account(stock_positions: List[dict], prices: Dict[str, float]) -> Dict[str, float]:
    mv = {INDIVIDUAL: 0.0, IRA: 0.0, ROTH: 0.0}
    for r in stock_positions:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        acct = (r.get("account") or "").strip().upper()
        tkr = (r.get("ticker") or "").strip().upper()
        if acct not in mv or not tkr:
            continue
        px = prices.get(tkr)
        if px is None:
            continue
        try:
            sh = float(r.get("shares") or 0.0)
            mv[acct] += sh * px
        except Exception as e:
            log.warning("stock_market_value_by_account: bad shares field for %s/%s: %s", acct, tkr, e)
            continue
    return mv


def plan_stock_trade(
    *,
    account: str,
    ticker: str,
    signal: str,
    last: pd.Series,
    mkt: Dict[str, float | bool],
    existing_open_tickers: set,
    acct_current_mv: float,
    retirement_breakeven_only: bool,
) -> Optional[dict]:
    """Build a paper trade plan (entry/stop/target/shares).

    INDIVIDUAL: swing trade — ATR stops, 2R target, risk-based sizing.
    IRA/ROTH:   buy-and-hold — pullback only, flat $10K sizing, wide
                catastrophic stop only, no take-profit target.
    """
    account = account.upper().strip()
    ticker  = ticker.upper().strip()
    signal  = signal.upper().strip()

    if not ticker or ticker in existing_open_tickers:
        return None

    try:
        close  = float(last["Close"])
        ema21  = float(last["EMA_21"])
        atr    = float(last.get("ATR_14", 0) or 0)
        high20 = float(last["HIGH_20"])
    except Exception as e:
        log.warning("plan_stock_trade: indicator field missing for %s: %s", ticker, e)
        return None

    if close <= 0:
        return None
    if signal not in ("PULLBACK", "BREAKOUT"):
        return None

    # ── Retirement buy-and-hold path ────────────────────────────────────────
    if account in (IRA, ROTH):
        # Only quality names and only on pullbacks — no breakout chasing.
        if signal != "PULLBACK":
            return None
        if ticker not in RETIREMENT_STOCKS:
            return None
        if retirement_breakeven_only:
            return None

        # Flat position sizing: 50% of the retirement stock slice (~$10K).
        slice_cap = float(RETIREMENT_STOCK_CAPS.get(account, 0))
        if slice_cap <= 0:
            return None

        pos_value = slice_cap * float(RETIREMENT_POSITION_SIZE_PCT)
        shares = int(pos_value / close)
        if shares < 1:
            return None

        # Remaining capacity check — don't exceed the slice.
        remaining = max(slice_cap - float(acct_current_mv or 0.0), 0.0)
        if remaining < pos_value * 0.5:
            # Less than half a position worth of room — skip.
            return None

        shares = min(shares, int(remaining / close))
        if shares < 1:
            return None

        # Wide catastrophic stop only — designed to survive normal corrections.
        stop   = close * (1.0 - float(RETIREMENT_STOP_LOSS_PCT))
        # No price target: hold indefinitely until stop or manual exit.
        target = 0.0

        return {
            "account":          account,
            "ticker":           ticker,
            "signal":           signal,
            "entry_price":      float(close),
            "stop_price":       float(stop),
            "target_price":     float(target),
            "shares":           int(shares),
            "risk_per_share":   float(close - stop),
            "r_multiple_target": 0.0,
            "notes": (
                f"RETIRE BUY-HOLD | pos_value=${pos_value:,.0f} "
                f"stop={RETIREMENT_STOP_LOSS_PCT*100:.0f}% below entry"
            ),
        }

    # ── INDIVIDUAL swing trade path ──────────────────────────────────────────
    if not nextday_valid_for_entry(signal, last):
        return None

    # --- Stop / target logic ---
    if signal == "PULLBACK":
        stop     = ema21 - (STOCK_STOP_ATR_PULLBACK * atr) if atr > 0 else ema21 * 0.97
        risk_ps  = max(close - stop, 0.01)
        target_r = close + STOCK_TARGET_R_MULTIPLE * risk_ps
        target   = max(high20, target_r)
    else:
        breakout_level = high20
        stop    = breakout_level - (STOCK_STOP_ATR_BREAKOUT * atr) if atr > 0 else breakout_level * 0.96
        risk_ps = max(close - stop, 0.01)
        target  = close + STOCK_TARGET_R_MULTIPLE * risk_ps

    # --- Account sizing ---
    acct_size = float(INDIVIDUAL_STOCK_CAP)
    if acct_size <= 0:
        return None

    max_pos_value = acct_size * float(STOCK_MAX_POSITION_PCT_INDIVIDUAL)
    risk_cap      = acct_size * float(STOCK_RISK_PCT_INDIVIDUAL)

    risk_shares      = int(risk_cap / risk_ps)
    value_cap_shares = int(max_pos_value / close)
    remaining_value  = max(acct_size - float(acct_current_mv or 0.0), 0.0)
    remaining_shares = int(remaining_value / close)

    if risk_shares < 1 or value_cap_shares < 1 or remaining_shares < 1:
        return None

    shares = min(risk_shares, value_cap_shares, remaining_shares)
    if shares < 1:
        return None

    return {
        "account":          account,
        "ticker":           ticker,
        "signal":           signal,
        "entry_price":      float(close),
        "stop_price":       float(stop),
        "target_price":     float(target),
        "shares":           int(shares),
        "risk_per_share":   float(risk_ps),
        "r_multiple_target": float(STOCK_TARGET_R_MULTIPLE),
        "notes": (
            f"{signal} plan | "
            f"risk_cap=${risk_cap:,.0f}, "
            f"max_pos=${max_pos_value:,.0f}"
        ),
    }


def execute_stock_plan(today: dt.date, plan: dict) -> str:
    """Paper 'execution': record OPEN stock position immediately (filled at close).

    Routing:
      IRA/ROTH  → retirement_positions.csv  (buy-and-hold; managed by
                   close_retirement_stops / update_retirement_marks)
      INDIVIDUAL → stock_positions.csv       (swing trades; managed by
                   update_and_close_stock_positions)
    """
    account    = (plan.get("account") or "").strip().upper()
    entry_date = today.isoformat()

    # ── Retirement buy-and-hold path ────────────────────────────────────────
    if account in (IRA, ROTH):
        ticker = (plan.get("ticker") or "").strip().upper()
        rows   = load_retirement_positions()

        # Idempotent: same account + ticker + entry_date = already recorded
        if any(
            (r.get("account") or "").strip().upper() == account
            and (r.get("ticker") or "").strip().upper() == ticker
            and (r.get("entry_date") or "") == entry_date
            for r in rows
        ):
            return f"{account}-{ticker}-{entry_date}"

        entry_px = float(plan["entry_price"])
        shares   = int(plan["shares"])

        rows.append({
            "account":             account,
            "ticker":              ticker,
            "shares":              str(shares),
            "entry_price":         f"{entry_px:.2f}",
            "entry_date":          entry_date,
            "current_price":       f"{entry_px:.2f}",
            "pct_change":          "0.00",
            "breakeven_target":    "",
            "flag_breakeven_only": "0",
            "notes":               plan.get("notes", "BUY-HOLD"),
        })
        write_retirement_positions(rows)
        log.info(
            "execute_stock_plan: %s %s %d sh @ %.2f → retirement_positions",
            account, ticker, shares, entry_px,
        )
        return f"{account}-{ticker}-{entry_date}"

    # ── INDIVIDUAL swing trade path ──────────────────────────────────────────
    ensure_stock_files()
    rows = load_stock_positions()

    pos_id = _stock_position_id(account, plan["ticker"], entry_date)

    # Idempotent
    if any((r.get("id") or "") == pos_id for r in rows):
        return pos_id

    rows.append({
        "id":               pos_id,
        "account":          account,
        "ticker":           plan["ticker"],
        "signal":           plan["signal"],
        "plan_date":        entry_date,
        "entry_date":       entry_date,
        "entry_price":      f"{float(plan['entry_price']):.2f}",
        "shares":           str(int(plan["shares"])),
        "stop_price":       f"{float(plan['stop_price']):.2f}",
        "target_price":     f"{float(plan['target_price']):.2f}",
        "risk_per_share":   f"{float(plan['risk_per_share']):.4f}",
        "r_multiple_target":f"{float(plan['r_multiple_target']):.2f}",
        "status":           "OPEN",
        "exit_date":        "",
        "exit_price":       "",
        "exit_reason":      "",
        "pnl_abs":          "",
        "pnl_pct":          "",
        "notes":            plan.get("notes", ""),
    })

    write_stock_positions(rows)
    return pos_id

def last_close_prices(tickers: List[str]) -> Dict[str, float]:
    """
    Fetch last available daily Close for each ticker.
    Returns { "AAPL": 195.12, ... }

    Uses the session cache when warmed; falls back to a yfinance batch call only
    for tickers the cache doesn't have.  Cache and fallback results are merged so
    no prices are lost when some tickers are missing from the cache.
    """
    tickers = sorted({(t or "").strip().upper() for t in tickers if (t or "").strip()})
    if not tickers:
        return {}

    prices: Dict[str, float] = {}

    if _cache is not None:
        prices = _cache.last_closes(tickers)
        missing = [t for t in tickers if t not in prices]
        if not missing:
            return prices
        # Only fetch what the cache missed; merge results below.
        tickers = missing

    try:
        df = yf.download(
            tickers=" ".join(tickers),
            period="7d",
            interval="1d",
            auto_adjust=False,
            progress=False,
            group_by="column",
        )
    except Exception as e:
        log.warning("last_close_prices batch fetch failed: %s", e)
        return prices  # return whatever the cache had rather than empty dict

    # Multi-ticker download => columns like ('Close','AAPL')
    if isinstance(df.columns, pd.MultiIndex):
        if ("Close" in df.columns.get_level_values(0)) and (len(df) > 0):
            close = df["Close"].dropna(how="all")
            if len(close) > 0:
                last = close.iloc[-1].to_dict()
                for k, v in last.items():
                    try:
                        prices[str(k).upper()] = float(v)
                    except Exception as e:
                        log.debug("last_close_prices: could not parse price for %s: %s", k, e)
                        pass
        return prices

    # Single ticker download => columns like 'Close'
    if "Close" in df.columns and len(df) > 0:
        try:
            v = float(df["Close"].dropna().iloc[-1])
            prices[tickers[0]] = v
        except Exception as e:
            log.warning("last_close_prices: could not parse single-ticker close for %s: %s",
                        tickers[0] if tickers else "?", e)

    return prices

def update_and_close_stock_positions(today: dt.date, mkt: Dict[str, float | bool]) -> Dict[str, List[str]]:
    """Update OPEN stock positions and close them if stop/target hit (paper, based on latest close)."""
    ensure_stock_files()
    rows = load_stock_positions()
    if not rows:
        return {"stops": [], "targets": []}

    open_rows = [r for r in rows if (r.get("status") or "").upper() == "OPEN"]
    if not open_rows:
        return {"stops": [], "targets": []}

    tickers = sorted({(r.get("ticker") or "").strip().upper() for r in open_rows if (r.get("ticker") or "").strip()})
    prices: Dict[str, float] = last_close_prices(tickers)

    stops: List[str] = []
    targets: List[str] = []

    changed = False
    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue

        tkr = (r.get("ticker") or "").strip().upper()
        px = prices.get(tkr)
        if px is None:
            continue

        try:
            entry = float(r.get("entry_price") or 0.0)
            stop = float(r.get("stop_price") or 0.0)
            target = float(r.get("target_price") or 0.0)
            sh = int(float(r.get("shares") or 0))
        except Exception as e:
            log.warning("update_and_close_stock_positions: bad numeric field for %s %s: %s",
                        r.get("account", "?"), tkr, e)
            continue

        if sh <= 0 or entry <= 0:
            continue

        # Optional: move stop to breakeven after +1R
        if STOCK_USE_BREAKEVEN_TRAIL:
            try:
                risk_ps = float(r.get("risk_per_share") or 0.0)
            except Exception as e:
                log.debug("update_and_close_stock_positions: bad risk_per_share for %s: %s", tkr, e)
                risk_ps = 0.0
            if risk_ps > 0 and (px - entry) >= (STOCK_BREAKEVEN_AFTER_R * risk_ps):
                new_stop = max(stop, entry)
                if new_stop != stop:
                    stop = new_stop
                    r["stop_price"] = f"{stop:.2f}"
                    changed = True

        exit_reason = None
        if stop > 0 and px <= stop:
            exit_reason = "STOP"
        elif target > 0 and px >= target:
            exit_reason = "TARGET"

        if not exit_reason:
            continue

        pnl_abs = (px - entry) * sh
        pnl_pct = (px - entry) / entry

        r["status"] = "CLOSED"
        r["exit_date"] = today.isoformat()
        r["exit_price"] = f"{px:.2f}"
        r["exit_reason"] = exit_reason
        r["pnl_abs"] = f"{pnl_abs:.2f}"
        r["pnl_pct"] = f"{pnl_pct*100:.2f}"
        changed = True

        append_stock_trade({
            "id": r.get("id", ""),
            "account": r.get("account", ""),
            "ticker": tkr,
            "entry_date": r.get("entry_date", ""),
            "entry_price": r.get("entry_price", ""),
            "shares": r.get("shares", ""),
            "exit_date": r.get("exit_date", ""),
            "exit_price": r.get("exit_price", ""),
            "reason": exit_reason,
            "close_type": exit_reason,
            "pnl_abs": r.get("pnl_abs", ""),
            "pnl_pct": r.get("pnl_pct", ""),
        })

        append_stock_fill({
            "date": today.isoformat(),
            "account": r.get("account", ""),
            "ticker": tkr,
            "action": "CLOSE",
            "price": f"{px:.2f}",
            "shares": str(int(sh)),
            "reason": exit_reason,
        })

        if exit_reason == "STOP":
            stops.append(f"{tkr} @{px:.2f}")
        else:
            targets.append(f"{tkr} @{px:.2f}")

    if changed:
        write_stock_positions(rows)

    return {"stops": stops, "targets": targets}


# ============================================================
# CSP planning / bookkeeping (paper)
# ============================================================



def ensure_positions_files() -> None:
    if not os.path.isfile(CSP_POSITIONS_FILE):
        with open(CSP_POSITIONS_FILE, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSP_POSITIONS_COLUMNS)
            w.writeheader()
    if not os.path.isfile(CC_POSITIONS_FILE):
        with open(CC_POSITIONS_FILE, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CC_POSITIONS_COLUMNS)
            w.writeheader()


def load_csv_rows(path: str) -> List[dict]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", newline="") as f:
        return list(csv.DictReader(f))


def write_csv_rows(path: str, rows: List[dict], fieldnames: List[str]) -> None:
    def _write(f):
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    atomic_write(path, _write)



def append_csp_ledger_row(row: dict) -> None:
    """Append a CSP OPEN entry to the simple CSP ledger.

    We store *total premium dollars* in 'premium' (not per-contract credit).
    If caller provides only credit_mid, we compute premium.
    """
    fieldnames = ["date","week_id","ticker","expiry","strike","contracts","premium","cash_reserved","tier"]
    file_exists = os.path.isfile(CSP_LEDGER_FILE)

    # normalize
    try:
        contracts = int(float(row.get("contracts") or 0)) or 1
    except Exception as e:
        log.warning("append_csp_ledger_row: bad contracts value for %s: %s",
                    row.get("ticker", "?"), e)
        contracts = 1
    if "premium" not in row or row.get("premium") in ("", None):
        try:
            credit_mid = float(row.get("credit_mid") or 0.0)
            row["premium"] = f"{credit_mid * 100.0 * contracts:.2f}"
        except Exception as e:
            log.warning("append_csp_ledger_row: could not compute premium for %s: %s",
                        row.get("ticker", "?"), e)
            row["premium"] = ""

    with open(CSP_LEDGER_FILE, mode="a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in fieldnames})



def csp_already_logged(ledger_rows: List[dict], week_id: str, ticker: str, expiry: str, strike: float) -> bool:
    for r in ledger_rows:
        try:
            if (r["week_id"] == week_id
                    and r["ticker"] == ticker
                    and r["expiry"] == expiry
                    # Tolerance of half a cent handles float->string->float round-trips
                    # (e.g. 50.0 stored as "50.0" parsed back as 50.000000001).
                    and abs(float(r["strike"]) - float(strike)) < 0.005):
                return True
        except Exception as e:
            log.debug("csp_already_logged: bad row data (week=%s ticker=%s): %s", week_id, ticker, e)
            continue
    return False


def _pick_expiry_in_dte_range(ticker_obj: yf.Ticker, dte_min: int, dte_max: int) -> Tuple[Optional[str], Optional[int]]:
    today = dt.date.today()
    ticker_sym = ticker_obj.ticker
    if ticker_sym not in _expiry_cache:
        _expiry_cache[ticker_sym] = ticker_obj.options
    expiries: List[Tuple[str, int]] = []
    for exp_str in _expiry_cache[ticker_sym]:
        try:
            exp_date = dt.date.fromisoformat(exp_str)
            dte = (exp_date - today).days
            expiries.append((exp_str, dte))
        except Exception as e:
            log.debug("_pick_expiry_in_dte_range: bad expiry string %r: %s", exp_str, e)
            continue
    for exp_str, dte in expiries:
        if dte_min <= dte <= dte_max:
            return exp_str, dte
    return None, None


def _suggest_put_strike(
    stock_last: pd.Series,
    atr_mult: float,
    *,
    risk_off: bool = False,
    min_otm_pct: float = 0.0,
    base_ma: str = "EMA_21",
) -> float:
    close = float(stock_last["Close"])
    atr14 = float(stock_last.get("ATR_14", 0) or 0)

    # Choose base level (slower MA when risk-off to avoid "chasing down")
    base = close
    base_ma_u = (base_ma or "").upper()
    if base_ma_u == "SMA_50":
        base = float(stock_last.get("SMA_50", close) or close)
    elif base_ma_u == "EMA_50":
        base = float(stock_last.get("EMA_50", close) or close)
    elif base_ma_u == "EMA_21":
        base = float(stock_last.get("EMA_21", close) or close)

    if CSP_STRIKE_MODE == "ema21_atr":
        raw = base - (atr_mult * atr14)
    else:
        raw = close * 0.92

    # Enforce minimum % OTM cushion
    if min_otm_pct and close > 0:
        otm_cap = close * (1.0 - float(min_otm_pct))
        raw = min(raw, otm_cap)

    return float(raw)


def _round_strike_to_chain(puts_df: pd.DataFrame, target_strike: float) -> float:
    strikes = sorted([float(s) for s in puts_df["strike"].tolist()])
    below = [s for s in strikes if s <= target_strike]
    if not below:
        return strikes[0]
    return below[-1]


def evaluate_csp_candidate(
    ticker: str,
    df: pd.DataFrame,
    atr_mult: float = 0.50,
    *,
    risk_off: bool = False,
    min_otm_pct: float = 0.0,
    base_ma: str = CSP_STRIKE_BASE_NORMAL,
) -> Optional[dict]:
    """Evaluate a single CSP candidate (enforces share price cap)."""
    if df is None or df.empty:
        return None

    stock_last = df.iloc[-1]
    try:
        close_px = float(stock_last.get("Close", 0) or 0)
    except Exception as e:
        log.debug("evaluate_csp_candidate: bad Close price for %s: %s", ticker, e)
        close_px = 0.0
    if close_px > CSP_MAX_SHARE_PRICE:
        return None

    try:
        t = yf.Ticker(ticker)
        exp_str, dte = _pick_expiry_in_dte_range(t, CSP_TARGET_DTE_MIN, CSP_TARGET_DTE_MAX)
        if not exp_str:
            return None

        chain_key = f"{ticker}-{exp_str}"
        if chain_key not in _chain_cache:
            _chain_cache[chain_key] = t.option_chain(exp_str)
        chain = _chain_cache[chain_key]
        puts = chain.puts.copy()
        if puts.empty:
            return None

        raw_strike = _suggest_put_strike(stock_last, atr_mult=atr_mult, risk_off=risk_off, min_otm_pct=min_otm_pct, base_ma=base_ma)
        strike = _round_strike_to_chain(puts, raw_strike)

        row = puts.loc[puts["strike"] == strike]
        if row.empty:
            return None
        row = row.iloc[0]

        bid = float(row.get("bid", 0) or 0)
        ask = float(row.get("ask", 0) or 0)
        _oi  = float(row.get("openInterest", 0) or 0)
        _vol = float(row.get("volume", 0) or 0)
        oi  = int(_oi)  if math.isfinite(_oi)  else 0
        vol = int(_vol) if math.isfinite(_vol) else 0
        iv  = float(row.get("impliedVolatility", 0) or 0)

        if bid < CSP_MIN_BID or ask <= 0 or ask < bid:
            return None
        if oi < CSP_MIN_OI or vol < CSP_MIN_VOLUME:
            return None
        if CSP_MIN_IV and iv < CSP_MIN_IV:
            return None

        mid = (bid + ask) / 2.0

        cash_required_per_contract = strike * 100.0
        contracts = int(CSP_MAX_CASH_PER_TRADE // cash_required_per_contract)
        if contracts < 1:
            return None

        est_premium = mid * 100.0 * contracts
        cash_reserved = cash_required_per_contract * contracts
        yield_pct = est_premium / cash_reserved if cash_reserved > 0 else 0.0

        return {
            "ticker": ticker,
            "expiry": exp_str,
            "dte": int(dte or 0),
            "strike": float(strike),
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "iv": iv,
            "contracts": int(contracts),
            "cash_reserved": float(cash_reserved),
            "est_premium": float(est_premium),
            "yield_pct": float(yield_pct),
            "atr_mult": float(atr_mult),
            "reason": f"Strike≈{base_ma}-{atr_mult:.2f}*ATR, minOTM={min_otm_pct:.0%} (raw {raw_strike:.2f})",
        }
    except Exception as e:
        log.warning("evaluate_csp_candidate failed for %s: %s", ticker, e)
        return None


def csp_regime(vix_close: float) -> str:
    if vix_close < 18:
        return "LOW_IV"
    if vix_close <= 25:
        return "NORMAL"
    return "HIGH_IV"


def classify_csp_tier(idea: dict) -> str:
    prem = float(idea["est_premium"])
    y = float(idea["yield_pct"])
    if prem >= CSP_MIN_PREMIUM_AGGRESSIVE and y >= CSP_MIN_YIELD_AGGRESSIVE:
        return "AGGRESSIVE"
    if prem >= CSP_MIN_PREMIUM_BALANCED and y >= CSP_MIN_YIELD_BALANCED:
        return "BALANCED"
    if prem >= CSP_MIN_PREMIUM_CONSERVATIVE and y >= CSP_MIN_YIELD_CONSERVATIVE:
        return "CONSERVATIVE"
    return "REJECT"


def score_csp_idea(idea: dict) -> float:
    prem = float(idea["est_premium"])
    y = float(idea["yield_pct"])
    iv = float(idea["iv"])
    dte = float(idea["dte"])
    s = 0.0
    s += min(prem / 250.0, 2.0)
    s += min(y / 0.04, 2.0)
    s += min(iv / 0.45, 1.5)
    s += 0.5 if 30 <= dte <= 40 else 0.0
    return float(s)


def allowed_tiers_for_regime(reg: str) -> set:
    if reg == "LOW_IV":
        return {"CONSERVATIVE", "BALANCED"}
    return {"CONSERVATIVE", "BALANCED", "AGGRESSIVE"}


def get_ticker_sector(ticker: str) -> str:
    """Return the sector label for a ticker, or 'OTHER' if not mapped."""
    return CSP_TICKER_SECTOR.get((ticker or "").strip().upper(), "OTHER")


def plan_weekly_csp_orders(
    csp_candidates: List[dict],
    *,
    today: dt.date,
    vix_close: float,
    total_remaining_cap: float,
    week_remaining_cap: float,
    aggressive_total: int,
    aggressive_week: int,
    open_sector_counts: Dict[str, int] = {},
) -> Dict[str, object]:
    week_id = iso_week_id(today)
    reg = csp_regime(vix_close)
    allowed = allowed_tiers_for_regime(reg)

    enriched: List[dict] = []
    for idea in csp_candidates:
        tier = classify_csp_tier(idea)
        if tier == "REJECT" or tier not in allowed:
            continue
        idea2 = dict(idea)
        idea2["tier"] = tier
        idea2["score"] = score_csp_idea(idea2)
        enriched.append(idea2)

    enriched.sort(key=lambda x: x["score"], reverse=True)

    selected: List[dict] = []
    used = set()

    total_remaining = float(total_remaining_cap)
    week_remaining = float(week_remaining_cap)

    # Start sector counts from existing open positions; grow as we select within this run.
    sector_counts: Dict[str, int] = dict(open_sector_counts)

    for idea in enriched:
        tkr = idea["ticker"]
        if tkr in used:
            continue

        cash = float(idea["cash_reserved"])
        if cash <= 0:
            continue

        if cash > week_remaining or cash > total_remaining:
            continue

        if idea["tier"] == "AGGRESSIVE":
            if aggressive_total >= CSP_MAX_AGGRESSIVE_TOTAL:
                continue
            if aggressive_week >= CSP_MAX_AGGRESSIVE_PER_WEEK:
                continue

        # Sector concentration check — applied globally across accounts.
        sector = get_ticker_sector(tkr)
        if sector != "OTHER" and sector_counts.get(sector, 0) >= CSP_MAX_POSITIONS_PER_SECTOR:
            log.debug(
                "CSP %s skipped: sector %s already at limit (%d)",
                tkr, sector, CSP_MAX_POSITIONS_PER_SECTOR,
            )
            continue

        selected.append(idea)
        used.add(tkr)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1

        week_remaining -= cash
        total_remaining -= cash

        if idea["tier"] == "AGGRESSIVE":
            aggressive_total += 1
            aggressive_week += 1

        if week_remaining < (CSP_MAX_CASH_PER_TRADE * 0.8):
            break

    return {
        "week_id": week_id,
        "regime": reg,
        "vix_close": float(vix_close),
        "selected": selected,
        "week_remaining_after": max(week_remaining, 0.0),
        "total_remaining_after": max(total_remaining, 0.0),
    }


def load_open_csp_tickers(today: Optional[dt.date] = None) -> set:
    """Return tickers that currently have an OPEN CSP in CSP_POSITIONS_FILE.

    If 'today' is provided, CSPs whose expiry is before 'today' are ignored (defensive in case
    an expiry hasn't been processed/closed yet).
    """
    ensure_positions_files()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    out = set()
    for r in rows:
        if (r.get("status") or "").strip().upper() != "OPEN":
            continue
        tkr = (r.get("ticker") or "").strip().upper()
        if not tkr:
            continue
        if today:
            exp_str = (r.get("expiry") or "").strip()
            try:
                if exp_str and dt.date.fromisoformat(exp_str) < today:
                    continue
            except Exception as e:
                log.debug("load_open_csp_tickers: bad expiry %r for %s: %s", exp_str, tkr, e)
        out.add(tkr)
    return out


def make_csp_position_id(ticker: str, expiry: str, strike: float, open_date: str) -> str:
    return f"{ticker}-{expiry}-{float(strike):.2f}-{open_date}"


def add_csp_position_from_selected(today: str, week_id: str, idea: dict) -> Tuple[str, bool]:
    """Add a CSP position row to CSP_POSITIONS_FILE.

    Returns (pos_id, created). If there's already an OPEN CSP for the ticker, this will NOT
    create a new position (regardless of expiry/strike).
    """
    ensure_positions_files()
    rows = load_csv_rows(CSP_POSITIONS_FILE)

    tkr = (idea.get("ticker") or "").strip().upper()
    if tkr:
        for r in rows:
            if (r.get("status") or "").strip().upper() != "OPEN":
                continue
            if (r.get("ticker") or "").strip().upper() == tkr:
                # Existing open CSP blocks new ones for same ticker.
                existing_id = (r.get("id") or "").strip()
                return (existing_id or make_csp_position_id(tkr, r.get("expiry") or "", float(r.get("strike") or 0.0), r.get("open_date") or today), False)

    pos_id = make_csp_position_id(tkr, idea["expiry"], idea["strike"], today)

    if any((r.get("id") or "") == pos_id for r in rows):
        return (pos_id, False)

    rows.append({
        "id": pos_id,
        "account": (idea.get("account") or INDIVIDUAL).strip().upper(),
        "open_date": today,
        "week_id": week_id,
        "ticker": tkr,
        "expiry": idea["expiry"],
        "dte_open": str(int(idea["dte"])),
        "strike": f"{float(idea['strike']):.2f}",
        "contracts": str(int(idea["contracts"])),
        "cash_reserved": f"{float(idea['cash_reserved']):.2f}",
        "premium": f"{float(idea['est_premium']):.2f}",
        "tier": idea.get("tier", ""),
        "status": "OPEN",
        "close_date": "",
        "close_type": "",
        "underlying_close_at_expiry": "",
        "shares_if_assigned": "",
        "assignment_cost_basis": "",
        "notes": "",
    })

    write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)
    return (pos_id, True)


def process_csp_take_profits(today: dt.date) -> Dict[str, List[str]]:
    """
    Close OPEN CSPs that have decayed to <= 50% of original premium (configurable).

    Fetches the current put bid/ask from the live option chain.  If the quote
    is stale, inverted, or the spread is too wide relative to mid, we skip
    rather than close blind — better to hold a position a day longer than to
    record a fictional fill.

    Returns {"closed": [list of summary strings]} for the Discord alert.
    """
    ensure_positions_files()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    if not rows:
        return {"closed": []}

    closed: List[str] = []
    changed = False

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue

        # Skip if already past expiry — let process_csp_expirations handle it.
        exp_str = (r.get("expiry") or "").strip()
        try:
            if exp_str and dt.date.fromisoformat(exp_str) <= today:
                continue
        except Exception as e:
            log.warning("process_csp_take_profits: bad expiry %r for %s: %s",
                        exp_str, r.get("ticker", "?"), e)
            continue

        ticker    = (r.get("ticker") or "").strip().upper()
        strike    = safe_float(r.get("strike"), 0.0)
        contracts = safe_int(r.get("contracts"), 0)

        try:
            orig_premium = safe_float(r.get("premium"), 0.0)
        except Exception as e:
            log.warning("process_csp_take_profits: bad premium field for %s %s: %s",
                        r.get("ticker", "?"), exp_str, e)
            orig_premium = 0.0

        if orig_premium <= 0 or strike <= 0 or contracts < 1 or not ticker:
            continue

        # Target: current total value of the position must be <= take-profit threshold.
        # orig_premium is total dollars; threshold is per-contract mid * 100 * contracts.
        tp_threshold = orig_premium * float(CSP_TAKE_PROFIT_PCT)

        try:
            t = yf.Ticker(ticker)
            chain_key = f"{ticker}-{exp_str}"
            if chain_key not in _chain_cache:
                _chain_cache[chain_key] = t.option_chain(exp_str)
            chain = _chain_cache[chain_key]
            puts = chain.puts
            if puts is None or puts.empty:
                log.info("CSP TP %s %s: no put chain available; skipping", ticker, exp_str)
                continue

            row = puts.loc[puts["strike"] == strike]
            if row.empty:
                log.info("CSP TP %s %s: strike %.2f not found in chain; skipping", ticker, exp_str, strike)
                continue
            row = row.iloc[0]

            bid = safe_float(row.get("bid"), 0.0)
            ask = safe_float(row.get("ask"), 0.0)

            # Basic sanity: need a real two-sided market.
            if bid <= 0 or ask <= 0 or ask < bid:
                log.info("CSP TP %s %s: inverted or zero quote (bid=%.2f ask=%.2f); skipping",
                         ticker, exp_str, bid, ask)
                continue

            mid = (bid + ask) / 2.0

            # Spread filter: if the market is too wide the mid is meaningless.
            spread_pct = (ask - bid) / mid
            if spread_pct > float(CSP_TP_MAX_SPREAD_PCT):
                log.info("CSP TP %s %s: spread %.0f%% too wide; skipping", ticker, exp_str, spread_pct * 100)
                continue

            current_value = mid * 100.0 * contracts

        except Exception as e:
            log.warning("CSP TP price fetch failed for %s %s: %s", ticker, exp_str, e)
            continue

        if current_value > tp_threshold:
            continue  # not enough decay yet

        # Close it.
        profit = orig_premium - current_value
        r["status"]     = "CLOSED_TP"
        r["close_date"] = today.isoformat()
        r["close_type"] = "CLOSED_TAKE_PROFIT"
        r["notes"]      = (
            f"TP at {float(CSP_TAKE_PROFIT_PCT)*100:.0f}%: "
            f"orig ${orig_premium:.0f} → current ${current_value:.0f} | profit ${profit:.0f}"
        )
        changed = True
        closed.append({
            "summary":    f"{ticker} {exp_str} {strike:.0f}P (${profit:.0f} profit)",
            "ticker":     ticker,
            "expiry":     exp_str,
            "strike":     strike,
            "contracts":  contracts,
            "ref_id":     (r.get("id") or ""),
            "buyback":    float(current_value),   # dollars paid to close
            "profit":     float(profit),
        })
        log.info("CSP TP closed: %s %s %.0fP — orig $%.0f current $%.0f profit $%.0f",
                 ticker, exp_str, strike, orig_premium, current_value, profit)

    if changed:
        write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)

    return {"closed": closed}


def process_csp_expirations(today: dt.date) -> Dict[str, List[str]]:
    ensure_positions_files()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    if not rows:
        return {"expired": [], "assigned": []}

    expired, assigned = [], []

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue

        exp_str = (r.get("expiry") or "").strip()
        try:
            exp = dt.date.fromisoformat(exp_str)
        except Exception as e:
            log.warning("process_csp_expirations: bad expiry %r for %s: %s",
                        exp_str, r.get("ticker", "?"), e)
            continue

        if exp > today:
            continue

        ticker = (r.get("ticker") or "").strip().upper()
        strike = float(r.get("strike") or 0.0)
        contracts = int(float(r.get("contracts") or 0.0))
        shares = contracts * 100

        underlying_close = None
        try:
            # Try the session cache first (covers ~1 year of daily data).
            # Fall back to a targeted network fetch only when the expiry date
            # predates the cache window (e.g. a long-dated CSP opened months ago).
            cached_df = download_ohlcv(ticker)  # cache-aware, returns copy
            if not cached_df.empty:
                cached_df.index = pd.to_datetime(cached_df.index)
                exp_ts = pd.Timestamp(exp)
                exact = cached_df[cached_df.index.normalize() == exp_ts]
                if not exact.empty:
                    underlying_close = float(exact["Close"].iloc[-1])
                else:
                    prior = cached_df[cached_df.index.normalize() < exp_ts]
                    if not prior.empty and prior.index[-1].date() >= (exp - dt.timedelta(days=5)):
                        # Cache has data close enough to expiry — use it.
                        underlying_close = float(prior["Close"].iloc[-1])

            if underlying_close is None:
                # Cache miss or expiry too old — targeted fetch around expiry date.
                start = (exp - dt.timedelta(days=7)).isoformat()
                end   = (exp + dt.timedelta(days=2)).isoformat()
                df = yf.download(ticker, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
                df.dropna(inplace=True)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                if not df.empty:
                    df.index = pd.to_datetime(df.index)
                    exp_ts = pd.Timestamp(exp)
                    exact = df[df.index.normalize() == exp_ts]
                    if not exact.empty:
                        underlying_close = float(exact["Close"].iloc[-1])
                    else:
                        prior = df[df.index.normalize() < exp_ts]
                        if not prior.empty:
                            underlying_close = float(prior["Close"].iloc[-1])
        except Exception as e:
            log.warning("CSP expiry price fetch failed for %s exp %s: %s", ticker, exp_str, e)
            underlying_close = None

        if underlying_close is None:
            log.warning("CSP expiry: could not determine close for %s %s — skipping", ticker, exp_str)
            continue

        r["underlying_close_at_expiry"] = f"{underlying_close:.2f}"
        r["close_date"] = exp.isoformat()

        if underlying_close >= strike:
            r["status"] = "EXPIRED"
            r["close_type"] = "EXPIRED_OTM"
            expired.append(f"{ticker} {exp_str} {strike:.0f}P")
        else:
            r["status"] = "ASSIGNED"
            r["close_type"] = "ASSIGNED_ITM"
            r["shares_if_assigned"] = str(shares)
            est_prem = float(r.get("premium") or r.get("est_premium") or 0.0)
            r["assignment_cost_basis"] = f"{(strike*shares - est_prem):.2f}"
            assigned.append(f"{ticker} {exp_str} {strike:.0f}P -> {shares} sh")

    write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)
    return {"expired": expired, "assigned": assigned}


def scan_early_assignments(today: dt.date) -> Dict[str, List[str]]:
    """Detect OPEN CSPs that are deeply ITM and likely already assigned.

    American-style equity options can be exercised any business day.  Waiting
    until the scheduled expiry date means the screener is blind to the assignment
    for potentially weeks — no lot is created, no CC income is started, and the
    position still appears as a live CSP eating into wheel capital.

    Trigger: current_price <= strike * (1 - CSP_EARLY_ASSIGN_ITM_PCT)

    Behaviour is controlled by CSP_EARLY_ASSIGN_WARN_ONLY:
      False (default) — mark status ASSIGNED immediately, same fields as
                        process_csp_expirations; create_lots_from_new_assignments
                        will pick it up on the next call in the same run.
      True            — log a warning and return the list for Discord alerting;
                        no state change.

    Returns {"warned": [...], "assigned": [...]} so the caller can surface
    both modes in the Discord alert.
    """
    ensure_positions_files()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    if not rows:
        return {"warned": [], "assigned": []}

    threshold = float(CSP_EARLY_ASSIGN_ITM_PCT)
    warned:   List[str] = []
    assigned: List[str] = []
    changed = False

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue

        exp_str = (r.get("expiry") or "").strip()
        try:
            exp = dt.date.fromisoformat(exp_str)
        except Exception as e:
            log.debug("scan_early_assignments: bad expiry %r: %s", exp_str, e)
            continue

        # Only scan positions that haven't expired yet — expirations are handled
        # by process_csp_expirations which runs before this in the same step.
        if exp <= today:
            continue

        # DTE gate: only trigger within the final days of the contract.
        # A high-beta stock with 10+ DTE can still recover; one with 3 DTE cannot.
        dte = (exp - today).days
        if dte > CSP_EARLY_ASSIGN_MAX_DTE:
            continue

        ticker    = (r.get("ticker") or "").strip().upper()
        strike    = safe_float(r.get("strike"), 0.0)
        contracts = safe_int(r.get("contracts"), 0)
        if not ticker or strike <= 0 or contracts < 1:
            continue

        try:
            df = download_ohlcv(ticker)   # cache-first, no extra network call
            if df is None or df.empty:
                continue
            current_price = float(df["Close"].iloc[-1])
        except Exception as e:
            log.warning("scan_early_assignments: price fetch failed for %s: %s", ticker, e)
            continue

        # Is the stock deeply enough ITM to trigger?
        itm_threshold_price = strike * (1.0 - threshold)
        if current_price > itm_threshold_price:
            continue   # OTM or only mildly ITM — normal, leave it alone

        pct_itm = (strike - current_price) / strike * 100.0
        label   = f"{ticker} {exp_str} {strike:.0f}P ({pct_itm:.1f}% ITM, {dte}d left, current {current_price:.2f})"

        if CSP_EARLY_ASSIGN_WARN_ONLY:
            log.warning("Early assignment candidate: %s", label)
            warned.append(label)
            continue

        # Auto-mark as ASSIGNED — identical fields to process_csp_expirations.
        shares    = contracts * 100
        est_prem  = safe_float(r.get("premium"), 0.0)

        r["status"]                      = "ASSIGNED"
        r["close_type"]                  = "ASSIGNED_EARLY"
        r["close_date"]                  = today.isoformat()
        r["underlying_close_at_expiry"]  = f"{current_price:.2f}"
        r["shares_if_assigned"]          = str(shares)
        r["assignment_cost_basis"]       = f"{(strike * shares - est_prem):.2f}"

        assigned.append(label)
        changed = True
        log.info("Early assignment auto-marked: %s", label)

    if changed:
        write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)

    return {"warned": warned, "assigned": assigned}


# ============================================================
# CC planning from assigned CSPs (classic wheel)
# ============================================================

def decide_cc_strike(
    current_price: float,
    net_cost_basis_per_share: float,
    atr: float,
) -> Tuple[str, float, str]:
    """
    ATR-scaled CC strike policy.

    Returns (decision, target_strike, reason).  Decision is always "SELL_CC".
    Caller rounds to chain and applies the hard floor.

    Strike = current_price + (atr_mult × ATR_14).  Using ATR instead of a
    fixed % means a volatile stock gets proportionally more room to recover
    than a stable one — a $1.50 ATR on a $40 stock gives different clearance
    than a $0.50 ATR on the same price.

    Multiplier tiers (higher = further OTM = more recovery room):
      ≥  0% vs basis : NORMAL (1.0×) — at/above basis, standard income mode
       0–10% down    : MILD   (1.5×) — give room for early recovery bounce
      10–25% down    : DEEP   (2.0×) — protect a meaningful rally
       > 25% down    : SEVERE (2.5×) — distress; never cap a recovery, collect what we can

    If ATR is unavailable (zero), falls back to a 2% OTM target so we always
    produce something rather than silently skipping.
    """
    if atr <= 0:
        # ATR unavailable — use a conservative fixed OTM rather than skip.
        target = current_price * 1.02
        return "SELL_CC", target, "ATR unavailable; 2% OTM fallback"

    if net_cost_basis_per_share <= 0:
        target = current_price + CC_ATR_MULT_NORMAL * atr
        return "SELL_CC", target, f"no basis data; {CC_ATR_MULT_NORMAL}×ATR OTM"

    pct_vs_basis = (current_price - net_cost_basis_per_share) / net_cost_basis_per_share

    if pct_vs_basis >= 0.0:
        mult = CC_ATR_MULT_NORMAL
        tier = "NORMAL"
    elif pct_vs_basis >= -float(CC_UNDERWATER_MILD_PCT):
        mult = CC_ATR_MULT_MILD
        tier = "MILD"
    elif pct_vs_basis >= -float(CC_UNDERWATER_DEEP_PCT):
        mult = CC_ATR_MULT_DEEP
        tier = "DEEP"
    else:
        mult = CC_ATR_MULT_SEVERE
        tier = "SEVERE"

    target = current_price + mult * atr
    reason = (
        f"{tier} ({pct_vs_basis*100:+.1f}% vs basis {net_cost_basis_per_share:.2f}); "
        f"{mult}×ATR ({atr:.2f}) → target {target:.2f}"
    )
    return "SELL_CC", target, reason


def _round_call_strike_to_chain(calls_df: pd.DataFrame, target_strike: float) -> float:
    strikes = sorted([float(s) for s in calls_df["strike"].tolist()])
    above = [s for s in strikes if s >= target_strike]
    if not above:
        return strikes[-1]
    return above[0]


def plan_covered_calls(today: dt.date, assigned_rows: List[dict], open_cc_lot_ids: set) -> List[dict]:
    ideas: List[dict] = []
    for pos in assigned_rows:
        ticker = (pos.get("ticker") or "").strip().upper()
        if not ticker:
            continue

        # Per-lot guard: skip if this specific lot already has an open CC.
        # Falls back to ticker-level check for legacy rows without lot_id.
        lot_id = (pos.get("lot_id") or "").strip()
        if lot_id and lot_id in open_cc_lot_ids:
            continue

        try:
            shares = int(float(pos.get("shares_if_assigned") or 0))
        except Exception as e:
            log.warning("plan_covered_calls: bad shares_if_assigned for %s: %s", ticker, e)
            shares = 0
        contracts = shares // 100
        if contracts < 1:
            continue

        try:
            # net_cost_basis is the per-share effective cost after premiums collected.
            # Until Step 3 tracks cumulative premiums, assigned_strike is the best proxy.
            net_cost_basis_per_share = float(pos.get("net_cost_basis_per_share") or
                                             pos.get("strike") or 0.0)
        except Exception as e:
            log.warning("plan_covered_calls: bad net_cost_basis_per_share for %s: %s", ticker, e)
            net_cost_basis_per_share = 0.0

        try:
            df = add_indicators(download_ohlcv(ticker))
            if df.empty:
                continue
            last = df.iloc[-1]
            current_price = float(last["Close"])
            atr = float(last.get("ATR_14") or 0)
        except Exception as e:
            log.warning("plan_covered_calls: price fetch failed for %s: %s", ticker, e)
            continue

        _decision, raw_target, cc_reason = decide_cc_strike(current_price, net_cost_basis_per_share, atr)

        # Floor: never sell a CC whose target lands below current_price * (1 - floor%).
        # Anchored to current price — not original basis — because when the stock is
        # deeply underwater a basis-relative floor would be ITM and block every strike.
        # This simply ensures we always sell something genuinely OTM.
        if CC_STRIKE_FLOOR_BELOW_CURRENT_PCT > 0:
            floor = current_price * (1.0 - float(CC_STRIKE_FLOOR_BELOW_CURRENT_PCT))
            if raw_target < floor:
                # ATR is very small relative to price (e.g., a slow defensive stock
                # that gapped down hard). Skip rather than sell a near-ITM CC.
                log.info(
                    "CC %s: raw target %.2f below current-price floor %.2f; skipping",
                    ticker, raw_target, floor,
                )
                continue

        try:
            t = yf.Ticker(ticker)
            exp_str, _ = _pick_expiry_in_dte_range(t, CC_TARGET_DTE_MIN, CC_TARGET_DTE_MAX)
            if not exp_str:
                log.info("CC %s: no expiry in %d–%d DTE window", ticker, CC_TARGET_DTE_MIN, CC_TARGET_DTE_MAX)
                continue
            chain_key = f"{ticker}-{exp_str}"
            if chain_key not in _chain_cache:
                _chain_cache[chain_key] = t.option_chain(exp_str)
            chain = _chain_cache[chain_key]
            calls = chain.calls.copy()
            if calls.empty:
                continue

            strike = _round_call_strike_to_chain(calls, raw_target)

            # Re-apply floor after chain rounding — the nearest available strike
            # could slip below the floor even when the raw target was above it.
            if CC_STRIKE_FLOOR_BELOW_CURRENT_PCT > 0:
                floor = current_price * (1.0 - float(CC_STRIKE_FLOOR_BELOW_CURRENT_PCT))
                if strike < floor:
                    log.info(
                        "CC %s: rounded strike %.2f below floor %.2f; skipping",
                        ticker, strike, floor,
                    )
                    continue

            row = calls.loc[calls["strike"] == strike].iloc[0]
            bid = float(row.get("bid", 0) or 0)
            ask = float(row.get("ask", 0) or 0)
            if bid < CC_MIN_BID or ask < bid:
                log.info("CC %s: strike %.0f bid %.2f below min or inverted; skipping", ticker, strike, bid)
                continue
            mid = (bid + ask) / 2.0
            # Retail call sells fill between bid and mid, not at mid.
            fill_credit  = bid + (mid - bid) * OPT_SELL_FILL_PCT
            commission   = OPT_COMMISSION_PER_CONTRACT * contracts
            net_credit   = fill_credit - (commission / 100.0)  # per-share net

            ideas.append({
                "ticker":        ticker,
                "expiry":        exp_str,
                "strike":        float(strike),
                "contracts":     int(contracts),
                "credit_mid":    float(net_credit),   # net per-share after fill model + commission
                "reason":        f"{cc_reason} | basis {net_cost_basis_per_share:.2f}",
                # Inherit account and lot_id from the lot that generated this CC idea.
                "account":       (pos.get("account") or INDIVIDUAL).strip().upper(),
                "source_lot_id": lot_id,              # links CC back to exact lot
            })
        except Exception as e:
            log.warning("plan_covered_calls failed for %s: %s", ticker, e)
            continue

    return ideas


def load_open_cc_tickers() -> set:
    """Return the set of tickers that already have an open CC.
    Kept for backward-compat; prefer load_open_cc_lot_ids for new code."""
    ensure_positions_files()
    rows = load_csv_rows(CC_POSITIONS_FILE)
    return {(r.get("ticker") or "").strip().upper()
            for r in rows
            if (r.get("status") or "").upper() == "OPEN"}


def load_open_cc_lot_ids() -> set:
    """Return the set of source_lot_ids that already have an open CC.

    This is the per-lot guard used by plan_ccs_from_open_lots so that two
    lots for the same ticker (two separate CSP assignment cycles) are handled
    independently.  Legacy CC rows without a source_lot_id are excluded from
    this set — they are handled by the ticker-level fallback in link_new_ccs_to_lots.
    """
    ensure_positions_files()
    rows = load_csv_rows(CC_POSITIONS_FILE)
    return {(r.get("source_lot_id") or "").strip()
            for r in rows
            if (r.get("status") or "").upper() == "OPEN"
            and (r.get("source_lot_id") or "").strip()}


def make_cc_position_id(ticker: str, expiry: str, strike: float, open_date: str) -> str:
    return f"{ticker}-{expiry}-{float(strike):.2f}-{open_date}"


def add_cc_position_from_candidate(today: str, idea: dict) -> str:
    ensure_positions_files()
    rows = load_csv_rows(CC_POSITIONS_FILE)
    cc_id = make_cc_position_id(idea["ticker"], idea["expiry"], idea["strike"], today)
    if any((r.get("id") or "") == cc_id for r in rows):
        return cc_id

    rows.append({
        "id": cc_id,
        "account": (idea.get("account") or INDIVIDUAL).strip().upper(),
        "open_date": today,
        "ticker": idea["ticker"],
        "expiry": idea["expiry"],
        "strike": f"{float(idea['strike']):.2f}",
        "contracts": str(int(idea["contracts"])),
        "premium": f"{float(idea['credit_mid'])*100.0*int(idea['contracts']):.2f}",
        "status": "OPEN",
        "close_date": "",
        "close_type": "",
        "source_lot_id": (idea.get("source_lot_id") or "").strip(),
        "notes": idea.get("reason", ""),
    })
    write_csv_rows(CC_POSITIONS_FILE, rows, CC_POSITIONS_COLUMNS)
    return cc_id