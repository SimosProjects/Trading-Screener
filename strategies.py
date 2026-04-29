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
    STOCK_MAX_POSITION_PCT,
    STOCK_TARGET_R_MULTIPLE,
    STOCK_BREAKEVEN_AFTER_R,
    STOCK_USE_BREAKEVEN_TRAIL,
    STOCK_STOP_ATR_PULLBACK,
    STOCK_STOP_ATR_BREAKOUT,

    # CSP rules
    CSP_POSITIONS_COLUMNS, CC_POSITIONS_COLUMNS,
    CSP_TARGET_DTE_MIN, CSP_TARGET_DTE_MAX,
    CSP_TAKE_PROFIT_PCT, CSP_TP_MAX_SPREAD_PCT,
    CSP_MAX_CASH_PER_TRADE, CSP_MAX_CONTRACTS,
    CSP_MIN_OI, CSP_MIN_OI_ETF, CSP_MIN_VOLUME, CSP_MIN_BID, CSP_MIN_IV,
    CSP_MAX_STOCK_PRICE, CSP_EXCLUDED_TICKERS,
    CSP_SMA200_MIN_SLOPE,
    CSP_RISK_OFF_VIX,
    CSP_STRIKE_MODE,
    CSP_STRIKE_BASE_NORMAL,
    CSP_NORMAL_MIN_OTM_PCT,
    CSP_MIN_ADX,
    CSP_MIN_PREMIUM_CONSERVATIVE, CSP_MIN_PREMIUM_BALANCED, CSP_MIN_PREMIUM_AGGRESSIVE,
    CSP_MIN_YIELD_CONSERVATIVE, CSP_MIN_YIELD_BALANCED, CSP_MIN_YIELD_AGGRESSIVE,
    CSP_MAX_AGGRESSIVE_TOTAL, CSP_MAX_AGGRESSIVE_PER_WEEK,
    CSP_MAX_POSITIONS_PER_SECTOR, CSP_TICKER_SECTOR,
    CSP_EARLY_ASSIGN_ITM_PCT, CSP_EARLY_ASSIGN_WARN_ONLY, CSP_EARLY_ASSIGN_MAX_DTE,
    CSP_ROLL_CANDIDATE_ITM_PCT, CSP_ROLL_CANDIDATE_MIN_DTE,

    # CC policy
    CC_TARGET_DTE_MIN, CC_TARGET_DTE_MAX,
    CC_ATR_MULT_NORMAL, CC_ATR_MULT_MILD, CC_ATR_MULT_DEEP, CC_ATR_MULT_SEVERE,
    CC_UNDERWATER_MILD_PCT, CC_UNDERWATER_DEEP_PCT,
    CC_STRIKE_FLOOR_BELOW_CURRENT_PCT,
    CC_MIN_BID,
    CC_TAKE_PROFIT_PCT, CC_TP_MAX_SPREAD_PCT,
    CC_DTE_BY_TIER,

    # Stock regime-dynamic parameters
    STOCK_MAX_POSITION_PCT,
    STOCK_MIN_ADX,
    STOCK_PULLBACK_RSI2_MAX,
    STOCK_PULLBACK_EMA_BAND,
    STOCK_BREAKOUT_VOL_MULT,

    # slippage / fill model
    STOCK_SLIPPAGE_PER_SHARE,
    OPT_SELL_FILL_PCT,
    OPT_BUY_FILL_PCT,
    OPT_COMMISSION_PER_CONTRACT,
)

log = get_logger(__name__)


def regime_val(param: object, regime: str, fallback=None):
    """Resolve a regime-dynamic parameter.

    If param is a dict keyed by regime name, return param[regime].
    If param is a scalar, return it directly (backward-compat).
    Falls back to fallback if the key is missing.
    """
    if isinstance(param, dict):
        return param.get(regime, param.get("BULL", fallback))
    return param


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


def get_live_price(ticker: str) -> Optional[float]:
    """Return the most current price available for a ticker.

    Tries yfinance fast_info (intraday last price) first so that pre-market
    screener runs use today's actual market price, not yesterday's close.
    Falls back to the DataCache/OHLCV iloc[-1] so the function always returns
    something even when fast_info is unavailable (weekends, network issues).

    This is used wherever a strategy *decision* depends on where the stock is
    right now — roll candidate detection, early assignment gating, CC strike
    selection — rather than just historical indicator calculation.
    """
    # Attempt live fast_info first
    try:
        info = yf.Ticker(ticker).fast_info
        price = float(info.get("last_price") or info.get("lastPrice") or 0.0)
        if price > 0:
            return price
    except Exception as e:
        log.debug("get_live_price: fast_info failed for %s: %s", ticker, e)

    # Fall back to DataCache / OHLCV last close
    try:
        df = download_ohlcv(ticker)
        if df is not None and not df.empty:
            return float(df["Close"].iloc[-1])
    except Exception as e:
        log.debug("get_live_price: ohlcv fallback failed for %s: %s", ticker, e)

    return None


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
    # 20-trading-day % change in the SMA200 — positive means the long-term trend
    # is still rising, negative means it has rolled over into a structural downtrend.
    # Used by is_csp_eligible instead of a hard close > SMA200 binary check.
    df["SMA200_SLOPE"] = df["SMA_200"].pct_change(periods=20)
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

def is_eligible(stock_row: pd.Series, regime: str = "BULL") -> bool:
    """Healthy-trend filter used for stock entries.

    ADX floor is regime-dynamic: in strong bull markets ADX naturally compresses
    after consolidation — a static floor of 20 would miss valid entries.
    """
    try:
        close = float(stock_row["Close"])
        sma50 = float(stock_row["SMA_50"])
        ema21 = float(stock_row["EMA_21"])
        adx   = float(stock_row["ADX_14"])
    except Exception as e:
        log.debug("is_eligible: indicator field missing or non-numeric: %s", e)
        return False
    adx_floor = float(regime_val(STOCK_MIN_ADX, regime, 18.0))
    return bool(close > sma50 and ema21 > sma50 and adx > adx_floor)


def is_csp_eligible(stock_row: pd.Series, *, allow_below_200: bool = False,
                    regime: str = "BULL") -> bool:
    """Eligibility filter for CSP scanning. All thresholds are regime-dynamic."""
    try:
        close        = float(stock_row["Close"])
        sma50        = float(stock_row.get("SMA_50", 0) or 0)
        sma200       = float(stock_row.get("SMA_200", 0) or 0)
        adx          = float(stock_row.get("ADX_14", 0) or 0)
        sma200_slope = stock_row.get("SMA200_SLOPE")
        sma200_slope = (float(sma200_slope)
                        if sma200_slope is not None
                        and not (isinstance(sma200_slope, float) and math.isnan(sma200_slope))
                        else None)
    except Exception as e:
        log.debug("is_csp_eligible: indicator field missing or non-numeric: %s", e)
        return False

    if sma50 <= 0:
        return False

    if CSP_MAX_STOCK_PRICE and close > float(CSP_MAX_STOCK_PRICE):
        log.info("is_csp_eligible REJECT price: close=%.2f > max=%.2f", close, float(CSP_MAX_STOCK_PRICE))
        return False

    if not allow_below_200:
        if sma200 <= 0:
            return False

        min_slope = float(regime_val(CSP_SMA200_MIN_SLOPE, regime, -0.002))
        if sma200_slope is not None:
            if sma200_slope < min_slope:
                log.info(
                    "is_csp_eligible REJECT slope: slope=%.4f < min=%.4f (close=%.2f sma200=%.2f)",
                    sma200_slope, min_slope, close, sma200,
                )
                return False
        else:
            if close < sma200:
                log.info(
                    "is_csp_eligible REJECT sma200 fallback: close=%.2f < sma200=%.2f",
                    close, sma200,
                )
                return False

        adx_floor = float(regime_val(CSP_MIN_ADX, regime, 15.0))
        if adx and adx < adx_floor:
            log.info("is_csp_eligible REJECT adx: adx=%.1f < %.1f (close=%.2f)", adx, adx_floor, close)
            return False
        return True

    # risk-off: close must be above SMA50 at minimum
    if close < sma50:
        return False
    if adx and adx < 10:
        return False
    return True


def pullback_signal(stock_row: pd.Series, regime: str = "BULL") -> bool:
    """RSI(2) oversold + close near EMA21. Both thresholds are regime-dynamic."""
    try:
        rsi2  = float(stock_row["RSI_2"])
        ema21 = float(stock_row["EMA_21"])
        close = float(stock_row["Close"])
    except Exception as e:
        log.debug("pullback_signal: indicator field missing or non-numeric: %s", e)
        return False
    rsi_max  = float(regime_val(STOCK_PULLBACK_RSI2_MAX,  regime, 5.0))
    ema_band = float(regime_val(STOCK_PULLBACK_EMA_BAND,  regime, 0.02))
    rsi_ok   = rsi2 < rsi_max
    near_ema = abs(close - ema21) / max(ema21, 1e-9) < ema_band
    return bool(rsi_ok and near_ema)


def breakout_signal(stock_row: pd.Series, regime: str = "BULL") -> bool:
    """20-day high breakout + volume expansion. Volume multiplier is regime-dynamic."""
    try:
        close   = float(stock_row["Close"])
        high20  = float(stock_row["HIGH_20"])
        vol     = float(stock_row["Volume"])
        vol_sma = float(stock_row["VOL_SMA_10"])
    except Exception as e:
        log.debug("breakout_signal: indicator field missing or non-numeric: %s", e)
        return False
    vol_mult = float(regime_val(STOCK_BREAKOUT_VOL_MULT, regime, 1.5))
    return bool(close > high20 and vol > vol_mult * vol_sma)


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
    regime: str = "BULL",
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

    max_pos_pct   = float(regime_val(STOCK_MAX_POSITION_PCT, regime, 0.15))
    max_pos_value = acct_size * max_pos_pct
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
            f"max_pos=${max_pos_value:,.0f} ({max_pos_pct*100:.0f}% of slice, regime={regime})"
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

        # Idempotent: if this account already holds this ticker, don't add again.
        # entry_date is intentionally excluded — retirement positions are long-hold
        # and re-enter the signal universe every day until manually closed.
        if any(
            (r.get("account") or "").strip().upper() == account
            and (r.get("ticker") or "").strip().upper() == ticker
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

    All float fields are rounded to 2 decimal places before writing to
    prevent binary floating-point garbage (e.g. 124.50000000000001).
    account column is included so IRA/ROTH trades are distinguishable.
    A trailing newline is ensured before appending to prevent row-gluing.
    """
    fieldnames = ["date","week_id","account","ticker","expiry","strike","contracts","premium","cash_reserved","tier"]
    file_exists = os.path.isfile(CSP_LEDGER_FILE)

    # normalize contracts
    try:
        contracts = int(float(row.get("contracts") or 0)) or 1
    except Exception as e:
        log.warning("append_csp_ledger_row: bad contracts value for %s: %s",
                    row.get("ticker", "?"), e)
        contracts = 1

    # compute premium if not supplied
    if "premium" not in row or row.get("premium") in ("", None):
        try:
            credit_mid = float(row.get("credit_mid") or 0.0)
            row["premium"] = round(credit_mid * 100.0 * contracts, 2)
        except Exception as e:
            log.warning("append_csp_ledger_row: could not compute premium for %s: %s",
                        row.get("ticker", "?"), e)
            row["premium"] = ""

    # Round all float fields to 2dp to eliminate binary float artifacts
    for fld in ("strike", "premium", "cash_reserved"):
        try:
            if row.get(fld) not in ("", None):
                row[fld] = f"{float(row[fld]):.2f}"
        except Exception:
            pass

    # Ensure trailing newline before appending (prevents row-gluing on crash/retry)
    if file_exists:
        try:
            with open(CSP_LEDGER_FILE, "rb") as f:
                f.seek(0, 2)
                if f.tell() > 0:
                    f.seek(-1, 2)
                    if f.read(1) != b"\n":
                        with open(CSP_LEDGER_FILE, "a") as fa:
                            fa.write("\n")
        except Exception as e:
            log.warning("append_csp_ledger_row: trailing newline check failed: %s", e)

    with open(CSP_LEDGER_FILE, mode="a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        if not file_exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in fieldnames})



def csp_already_logged(
    ledger_rows: List[dict],
    week_id: str,
    ticker: str,
    expiry: str,
    strike: float,
    account: str = "",
) -> bool:
    """Return True if this CSP has already been logged in the ledger.

    Matching requires week_id + ticker + expiry + strike to all match.
    If account is provided (non-empty), it must also match — this prevents
    the same ticker/strike/expiry in two different accounts being incorrectly
    treated as duplicates (e.g. selling same put in both IRA and ROTH).
    For legacy rows without an account column, account matching is skipped.
    """
    for r in ledger_rows:
        try:
            if (r["week_id"] == week_id
                    and r["ticker"] == ticker
                    and r["expiry"] == expiry
                    and abs(float(r["strike"]) - float(strike)) < 0.005):
                # Account check: only apply if both sides have a non-empty account
                row_acct = (r.get("account") or "").strip().upper()
                chk_acct = (account or "").strip().upper()
                if row_acct and chk_acct and row_acct != chk_acct:
                    continue   # same trade in a different account — not a duplicate
                return True
        except Exception as e:
            log.debug("csp_already_logged: bad row data (week=%s ticker=%s): %s", week_id, ticker, e)
            continue
    return False


def has_upcoming_ex_dividend(ticker: str, days_window: int = 10) -> bool:
    """Return True if the stock has an ex-dividend date within days_window days of today.

    Deep-ITM American-style puts can be exercised early the day before ex-dividend
    so the put holder captures the dividend.  We skip opening new CSPs whenever a
    dividend is imminent — the theta premium does not compensate for the unmodeled
    early-assignment risk and the sudden cost-basis surprise.

    days_window=10: conservative buffer since market makers begin hedging
    aggressively in the week before ex-div even for mildly ITM puts.
    """
    # ETFs distribute dividends differently and are not subject to early assignment
    # for dividend capture in practice.  Skip the check to avoid noise.
    from config import CSP_TICKER_SECTOR
    if CSP_TICKER_SECTOR.get(ticker, "OTHER") == "ETF_BROAD":
        return False
    try:
        today   = dt.date.today()
        cutoff  = today + dt.timedelta(days=days_window)
        divs    = yf.Ticker(ticker).dividends
        if divs is None or divs.empty:
            return False
        for ts in divs.index:
            try:
                ex_date = ts.date() if hasattr(ts, "date") else dt.date.fromisoformat(str(ts)[:10])
            except Exception:
                continue
            if today <= ex_date <= cutoff:
                log.info(
                    "has_upcoming_ex_dividend: %s ex-div %s within %d days — skipping CSP open",
                    ticker, ex_date, days_window,
                )
                return True
    except Exception as e:
        log.debug("has_upcoming_ex_dividend: could not check %s: %s", ticker, e)
    return False


def has_earnings_within_window(ticker: str, expiry_str: str, buffer_days: int = 2) -> bool:
    """Return True if an earnings announcement falls within the CSP's lifetime.

    The check window is today → expiry + buffer_days.  buffer_days=2 gives a
    small pad so we don't open a 30-DTE put expiring the Friday right before
    a Monday earnings call.

    Logic:
      - ETFs are skipped immediately — they have no earnings calendar.
      - If we can't get the date, return False (fail-open — don't block on
        missing data from yfinance).
      - yfinance returns earnings dates from the calendar property.
        We look at 'Earnings Date' which can be a single value or a range.

    Why this matters:
      IV spikes into earnings then collapses immediately after (IV crush).
      Selling a CSP before earnings means: (1) you capture inflated IV on the
      way in, but (2) the stock can gap 10-20% on the announcement, blowing
      through your strike with no opportunity to manage.  The premium collected
      never compensates for that tail risk.  Skip it — better candidates exist.
    """
    # ETFs have no earnings — yfinance returns a 404 if you try to fetch their
    # calendar.  Skip immediately rather than burning a network call.
    from config import CSP_TICKER_SECTOR
    if CSP_TICKER_SECTOR.get(ticker, "OTHER") == "ETF_BROAD":
        return False
    try:
        today   = dt.date.today()
        exp     = dt.date.fromisoformat(expiry_str)
        cutoff  = exp + dt.timedelta(days=buffer_days)

        cal = yf.Ticker(ticker).calendar
        if cal is None or (hasattr(cal, "empty") and cal.empty):
            return False

        # calendar can be a dict or a DataFrame depending on yfinance version
        if isinstance(cal, dict):
            earn_raw = cal.get("Earnings Date")
        else:
            # DataFrame: index is field names, column 0 is value
            try:
                earn_raw = cal.loc["Earnings Date"].iloc[0] if "Earnings Date" in cal.index else None
            except Exception:
                earn_raw = None

        if earn_raw is None:
            return False

        # Normalise to a list of dates
        dates_to_check = []
        if hasattr(earn_raw, "__iter__") and not isinstance(earn_raw, str):
            for v in earn_raw:
                try:
                    d = v.date() if hasattr(v, "date") else dt.date.fromisoformat(str(v)[:10])
                    dates_to_check.append(d)
                except Exception:
                    continue
        else:
            try:
                d = earn_raw.date() if hasattr(earn_raw, "date") else dt.date.fromisoformat(str(earn_raw)[:10])
                dates_to_check.append(d)
            except Exception:
                pass

        for earn_date in dates_to_check:
            if today <= earn_date <= cutoff:
                log.info(
                    "has_earnings_within_window: %s earnings %s falls within CSP window "
                    "(today=%s expiry=%s +%dd) — skipping",
                    ticker, earn_date, today, expiry_str, buffer_days,
                )
                return True

    except Exception as e:
        log.debug("has_earnings_within_window: could not check %s: %s", ticker, e)

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
    """Evaluate a single CSP candidate.

    Contracts are sized by available_capital so expensive stocks (MSFT, NVDA)
    are never silently rejected by price — they get 1 contract when that is
    all capital supports.  CSP_MAX_CONTRACTS is the hard ceiling per position.
    """
    if df is None or df.empty:
        return None

    stock_last = df.iloc[-1]
    try:
        close_px = float(stock_last.get("Close", 0) or 0)
    except Exception as e:
        log.debug("evaluate_csp_candidate: bad Close price for %s: %s", ticker, e)
        close_px = 0.0
    if close_px <= 0:
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
        # ETF_BROAD tickers are deeply liquid — lower OI floor is fine.
        # All other tickers use the higher stock OI floor for stress-scenario rolls.
        oi_floor = CSP_MIN_OI_ETF if get_ticker_sector(ticker) == "ETF_BROAD" else CSP_MIN_OI
        if oi < oi_floor or vol < CSP_MIN_VOLUME:
            return None
        if CSP_MIN_IV and iv < CSP_MIN_IV:
            return None

        mid = (bid + ask) / 2.0

        cash_required_per_contract = strike * 100.0
        # Size contracts by how many fit in the per-trade budget, bounded by
        # CSP_MAX_CONTRACTS.  plan_weekly_csp_orders will re-size again using
        # the actual account-level remaining capital before selecting.
        contracts = min(
            int(CSP_MAX_CASH_PER_TRADE // cash_required_per_contract),
            int(CSP_MAX_CONTRACTS),
        )
        if contracts < 1:
            log.debug(
                "evaluate_csp_candidate: %s skipped — 1 contract ($%.0f) > per-trade budget ($%.0f)",
                ticker, cash_required_per_contract, CSP_MAX_CASH_PER_TRADE,
            )
            return None

        est_premium = mid * 100.0 * contracts
        cash_reserved = cash_required_per_contract * contracts
        yield_pct = est_premium / cash_reserved if cash_reserved > 0 else 0.0

        # Slippage-adjusted fill: retail put sells fill between bid and mid.
        # Deduct per-contract commission.  This is the actual dollars collected
        # and is stored separately from est_premium so cost_basis at assignment
        # uses the real fill, not the optimistic mid-price.
        fill_per_share = bid + (mid - bid) * float(OPT_SELL_FILL_PCT)
        fill_premium = round(
            fill_per_share * 100.0 * contracts - OPT_COMMISSION_PER_CONTRACT * contracts,
            2,
        )

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
            "est_premium": float(est_premium),   # mid-price; used for display/scoring
            "fill_premium": float(fill_premium),  # actual fill after slippage+commission
            "yield_pct": float(yield_pct),
            "atr_mult": float(atr_mult),
            "reason": f"Strike≈{base_ma}-{atr_mult:.2f}*ATR, minOTM={min_otm_pct:.0%} (raw {raw_strike:.2f})",
        }
    except Exception as e:
        log.warning("evaluate_csp_candidate failed for %s: %s", ticker, e)
        return None


def csp_regime(vix_close: float) -> str:
    # Simplified — LOW_IV removed, HIGH_IV merged into RISK_OFF.
    # Full parameter tuning uses market_regime() in market.py.
    if vix_close <= float(CSP_RISK_OFF_VIX):
        return "NORMAL"
    return "RISK_OFF"


def classify_csp_tier(idea: dict, regime: str = "BULL") -> str:
    """Classify a CSP idea using regime-dynamic yield floors."""
    prem = float(idea["est_premium"])
    y    = float(idea["yield_pct"])
    if prem >= CSP_MIN_PREMIUM_AGGRESSIVE and y >= float(regime_val(CSP_MIN_YIELD_AGGRESSIVE, regime, 0.018)):
        return "AGGRESSIVE"
    if prem >= CSP_MIN_PREMIUM_BALANCED and y >= float(regime_val(CSP_MIN_YIELD_BALANCED, regime, 0.013)):
        return "BALANCED"
    if prem >= CSP_MIN_PREMIUM_CONSERVATIVE and y >= float(regime_val(CSP_MIN_YIELD_CONSERVATIVE, regime, 0.008)):
        return "CONSERVATIVE"
    return "REJECT"


def score_csp_idea(idea: dict) -> float:
    prem = float(idea["est_premium"])
    y    = float(idea["yield_pct"])
    iv   = float(idea["iv"])
    dte  = float(idea["dte"])
    s = 0.0
    s += min(prem / 250.0, 2.0)
    s += min(y / 0.04, 2.0)
    s += min(iv / 0.45, 1.5)
    s += 0.5 if 30 <= dte <= 40 else 0.0
    return float(s)


def allowed_tiers_for_regime(reg: str) -> set:
    # All three tiers always allowed — no regime blocks AGGRESSIVE outright.
    # The yield floor in classify_csp_tier naturally gates thin-premium ideas.
    return {"CONSERVATIVE", "BALANCED", "AGGRESSIVE"}


def classify_csp_tier_for_regime(idea: dict, reg: str) -> str:
    """Classify using regime-dynamic floors. reg is passed through to classify_csp_tier."""
    return classify_csp_tier(idea, regime=reg)


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
    live_vix: Optional[float] = None,
    regime: str = "BULL",
) -> Dict[str, object]:
    from config import VIX_INTRADAY_SPIKE_THRESHOLD
    week_id = iso_week_id(today)
    reg     = regime   # use the market regime passed from screener
    allowed = allowed_tiers_for_regime(reg)

    # Intraday VIX spike guard: if live VIX has moved sharply above the prior-day
    # EOD close, the market regime is worse than yesterday suggested. Downgrade
    # AGGRESSIVE candidates to BALANCED so we don't sell puts into a morning panic.
    vix_spiked = (
        live_vix is not None
        and live_vix > vix_close + float(VIX_INTRADAY_SPIKE_THRESHOLD)
    )
    if vix_spiked:
        log.warning(
            "Intraday VIX spike: live %.2f vs EOD close %.2f (+%.1f pts) — "
            "AGGRESSIVE tier downgraded to BALANCED for this run.",
            live_vix, vix_close, live_vix - vix_close,
        )

    enriched: List[dict] = []
    for idea in csp_candidates:
        tier = classify_csp_tier_for_regime(idea, reg)
        if tier == "REJECT" or tier not in allowed:
            continue
        # Apply intraday VIX guard: silently cap AGGRESSIVE at BALANCED
        if vix_spiked and tier == "AGGRESSIVE":
            tier = "BALANCED"
        idea2 = dict(idea)
        idea2["tier"]  = tier
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

        cash_per_contract = float(idea.get("strike", 0)) * 100.0
        if cash_per_contract <= 0:
            continue

        # Re-size using actual account capital available right now.
        # evaluate_csp_candidate sized conservatively; here we know the exact budget.
        budget = min(week_remaining, total_remaining)
        contracts = min(int(budget // cash_per_contract), int(CSP_MAX_CONTRACTS))
        if contracts < 1:
            log.debug("CSP %s skipped: 1 contract ($%.0f) > available capital ($%.0f)",
                      tkr, cash_per_contract, budget)
            continue

        cash = cash_per_contract * contracts

        # Scale premium fields linearly to the re-sized contract count.
        orig_contracts = max(int(idea.get("contracts", 1)), 1)
        scale = contracts / orig_contracts
        est_premium  = round(float(idea.get("est_premium",  0)) * scale, 2)
        fill_premium = round(float(idea.get("fill_premium", idea.get("est_premium", 0))) * scale, 2)

        if cash > week_remaining or cash > total_remaining:
            continue

        if idea["tier"] == "AGGRESSIVE":
            if aggressive_total >= CSP_MAX_AGGRESSIVE_TOTAL:
                continue
            if aggressive_week >= CSP_MAX_AGGRESSIVE_PER_WEEK:
                continue

        # Sector concentration check — per-account, regime-dynamic cap.
        sector = get_ticker_sector(tkr)
        sector_cap = int(regime_val(CSP_MAX_POSITIONS_PER_SECTOR, reg, 3))
        if sector != "OTHER" and sector_counts.get(sector, 0) >= sector_cap:
            log.debug(
                "CSP %s skipped: sector %s already at limit (%d) for regime %s",
                tkr, sector, sector_cap, reg,
            )
            continue

        selected.append({
            **idea,
            "contracts":    contracts,
            "cash_reserved": cash,
            "est_premium":  est_premium,
            "fill_premium": fill_premium,
        })
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
    # Include seconds-level timestamp so a crash-and-restart on the same day
    # produces a distinct ID rather than silently colliding with the first attempt.
    ts = dt.datetime.now().strftime("%H%M%S")
    return f"{ticker}-{expiry}-{float(strike):.2f}-{open_date}-{ts}"


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
        # fill_premium = slippage+commission-adjusted actual fill.
        # Falls back to est_premium for hand-crafted or legacy idea dicts.
        "fill_premium": f"{float(idea.get('fill_premium') or idea.get('est_premium') or 0.0):.2f}",
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


def process_csp_take_profits(today: dt.date, regime: str = "BULL") -> Dict[str, List[str]]:
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

        # Regime-dynamic take-profit threshold.
        # In bull markets hold longer to collect more premium; in risk-off close faster.
        tp_pct = float(regime_val(CSP_TAKE_PROFIT_PCT, regime, 0.60))
        tp_threshold = orig_premium * tp_pct

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
            f"TP at {tp_pct*100:.0f}%: "
            f"orig ${orig_premium:.0f} → current ${current_value:.0f} | profit ${profit:.0f}"
        )
        changed = True
        closed.append({
            "summary":   f"{ticker} {exp_str} {strike:.0f}P (${profit:.0f} profit)",
            "ticker":    ticker,
            "account":   (r.get("account") or INDIVIDUAL).strip().upper(),
            "expiry":    exp_str,
            "strike":    strike,
            "contracts": contracts,
            "ref_id":    (r.get("id") or ""),
            "buyback":   float(current_value),   # dollars paid to close
            "profit":    float(profit),
        })
        log.info("CSP TP closed: %s %s %.0fP — orig $%.0f current $%.0f profit $%.0f",
                 ticker, exp_str, strike, orig_premium, current_value, profit)

    if changed:
        write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)

    return {"closed": closed}


# ============================================================
# CC take-profit scan (mirrors CSP TP logic)
# ============================================================

def scan_cc_take_profits(today: dt.date, regime: str = "BULL") -> Dict[str, List[dict]]:
    """Close open CCs that have decayed to <= 50% of their opening premium.

    Closing early frees the lot's CC slot so a new covered call can be
    opened at the current (likely higher) strike for fresh premium income.
    This compounds the wheel income without waiting for full expiry.

    Returns {"closed": [{summary, ticker, expiry, strike, contracts,
                          ref_id, source_lot_id, buyback, profit}, ...]}
    """
    ensure_positions_files()
    rows = load_csv_rows(CC_POSITIONS_FILE)
    if not rows:
        return {"closed": []}

    closed: List[dict] = []
    changed = False

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue

        exp_str = (r.get("expiry") or "").strip()
        try:
            if not exp_str:
                continue
            if dt.date.fromisoformat(exp_str) <= today:
                continue   # expiry handled by process_cc_expirations
        except Exception as e:
            log.warning("scan_cc_take_profits: bad expiry %r for %s: %s",
                        exp_str, r.get("ticker", "?"), e)
            continue

        ticker    = (r.get("ticker") or "").strip().upper()
        strike    = safe_float(r.get("strike"), 0.0)
        contracts = safe_int(r.get("contracts"), 0)
        orig_premium = safe_float(r.get("premium"), 0.0)

        if orig_premium <= 0 or strike <= 0 or contracts < 1 or not ticker:
            continue

        cc_tp_pct    = float(regime_val(CC_TAKE_PROFIT_PCT, regime, 0.60))
        tp_threshold = orig_premium * cc_tp_pct

        try:
            t         = yf.Ticker(ticker)
            chain_key = f"{ticker}-{exp_str}"
            if chain_key not in _chain_cache:
                _chain_cache[chain_key] = t.option_chain(exp_str)
            chain = _chain_cache[chain_key]
            calls = chain.calls
            if calls is None or calls.empty:
                log.info("CC TP %s %s: no call chain; skipping", ticker, exp_str)
                continue

            row = calls.loc[calls["strike"] == strike]
            if row.empty:
                log.info("CC TP %s %s: strike %.2f not in chain; skipping", ticker, exp_str, strike)
                continue
            row = row.iloc[0]

            bid = safe_float(row.get("bid"), 0.0)
            ask = safe_float(row.get("ask"), 0.0)
            if bid <= 0 or ask <= 0 or ask < bid:
                log.info("CC TP %s %s: bad quote bid=%.2f ask=%.2f; skipping",
                         ticker, exp_str, bid, ask)
                continue

            mid        = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid
            if spread_pct > float(CC_TP_MAX_SPREAD_PCT):
                log.info("CC TP %s %s: spread %.0f%% too wide; skipping",
                         ticker, exp_str, spread_pct * 100)
                continue

            current_value = mid * 100.0 * contracts

        except Exception as e:
            log.warning("scan_cc_take_profits: chain fetch failed for %s %s: %s",
                        ticker, exp_str, e)
            continue

        if current_value > tp_threshold:
            continue   # not enough decay yet

        profit = orig_premium - current_value
        r["status"]     = "CLOSED_TP"
        r["close_date"] = today.isoformat()
        r["close_type"] = "CLOSED_TAKE_PROFIT"
        r["notes"]      = (
            f"CC TP at {cc_tp_pct*100:.0f}%: "
            f"orig ${orig_premium:.0f} → now ${current_value:.0f} | profit ${profit:.0f}"
        )
        changed = True
        closed.append({
            "summary":       f"{ticker} {exp_str} {strike:.0f}C (${profit:.0f} profit)",
            "ticker":        ticker,
            "account":       (r.get("account") or INDIVIDUAL).strip().upper(),
            "expiry":        exp_str,
            "strike":        strike,
            "contracts":     contracts,
            "ref_id":        (r.get("id") or ""),
            "source_lot_id": (r.get("source_lot_id") or ""),
            "buyback":       float(current_value),
            "profit":        float(profit),
        })
        log.info("CC TP closed: %s %s %.0fC — orig $%.0f now $%.0f profit $%.0f",
                 ticker, exp_str, strike, orig_premium, current_value, profit)

    if changed:
        write_csv_rows(CC_POSITIONS_FILE, rows, CC_POSITIONS_COLUMNS)

        # Clear the has_open_cc / cc_id flags on parent lots so new CCs can
        # be planned for these lots next run.
        #
        # IMPORTANT: We cannot use `from wheel import ...` here because Python
        # will resolve "wheel" to the installed system 'wheel' package instead
        # of our local wheel.py. Use sys.modules to get our already-imported
        # wheel module directly (it is always imported at screener startup via
        # `import wheel as _wheel_mod`).
        try:
            import sys
            _whl = sys.modules.get("wheel")
            if _whl is None:
                raise ImportError("wheel module not found in sys.modules — was it imported?")
            _whl_read   = _whl._read_rows
            _whl_write  = _whl._write_rows
            _WLF        = _whl.WHEEL_LOTS_FILE
            _LOT_FIELDS = _whl.LOT_FIELDS
            lots = _whl_read(_WLF)
            tp_lot_ids = {c["source_lot_id"] for c in closed if c.get("source_lot_id")}
            lots_changed = False
            for lot in lots:
                if (lot.get("lot_id") or "").strip() in tp_lot_ids:
                    lot["has_open_cc"]          = "0"
                    lot["cc_id"]                = ""
                    # Update net_cost_basis to reflect the CC premium kept.
                    # Find the matching closed entry to get the profit amount.
                    matching = next(
                        (c for c in closed
                         if c.get("source_lot_id") == (lot.get("lot_id") or "").strip()),
                        None,
                    )
                    if matching:
                        orig_basis   = float(lot.get("cost_basis") or 0)
                        cc_net_kept  = float(matching.get("profit", 0))
                        prev_collected = float(lot.get("cc_premium_collected") or 0)
                        new_collected  = prev_collected + cc_net_kept
                        # net_cost_basis is always (cost_basis - cc_premium_collected)
                        # Compute from both source fields so it stays consistent
                        # even if one field was manually edited.
                        new_net_basis = max(0.0, orig_basis - new_collected)
                        lot["cc_premium_collected"] = f"{new_collected:.2f}"
                        lot["net_cost_basis"]       = f"{new_net_basis:.2f}"
                    lots_changed = True
            if lots_changed:
                _whl_write(_WLF, lots, _LOT_FIELDS)
                log.info("Cleared has_open_cc on %d lot(s) and updated net_cost_basis after CC TP close",
                         len(tp_lot_ids))
        except Exception as e:
            log.warning("scan_cc_take_profits: failed to clear lot cc flags: %s", e)

    return {"closed": closed}


# ============================================================
# CSP roll candidate detection (display-only)
# ============================================================

def scan_csp_roll_candidates(today: dt.date) -> List[dict]:
    """Return open CSPs that are significantly ITM with DTE remaining to roll.

    A roll candidate satisfies BOTH:
      1. current_price < strike * (1 - CSP_ROLL_CANDIDATE_ITM_PCT)   [>=10% ITM]
      2. DTE remaining > CSP_ROLL_CANDIDATE_MIN_DTE                   [>10 days left]

    This is purely informational — no state change, no automated roll.
    The human reviews and decides whether to buy-to-close and re-open at a
    lower strike / further expiry for a net credit.

    Returns list of dicts sorted most-ITM first.
    """
    ensure_positions_files()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    if not rows:
        return []

    itm_pct = float(CSP_ROLL_CANDIDATE_ITM_PCT)
    min_dte  = int(CSP_ROLL_CANDIDATE_MIN_DTE)
    candidates: List[dict] = []

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue

        exp_str = (r.get("expiry") or "").strip()
        try:
            exp = dt.date.fromisoformat(exp_str)
        except Exception:
            continue

        if exp <= today:
            continue   # expiry / early-assignment handled elsewhere

        dte = (exp - today).days
        if dte <= min_dte:
            continue   # too close — this is an assignment situation, not a roll

        ticker    = (r.get("ticker") or "").strip().upper()
        strike    = safe_float(r.get("strike"), 0.0)
        contracts = safe_int(r.get("contracts"), 0)
        if not ticker or strike <= 0:
            continue

        try:
            current_price = get_live_price(ticker)
            if current_price is None:
                log.warning("scan_csp_roll_candidates: no price for %s; skipping", ticker)
                continue
        except Exception as e:
            log.warning("scan_csp_roll_candidates: price fetch failed for %s: %s", ticker, e)
            continue

        # Only flag when stock is sufficiently below strike
        if current_price >= strike * (1.0 - itm_pct):
            continue

        pct_itm = (strike - current_price) / strike * 100.0
        candidates.append({
            "ticker":        ticker,
            "account":       (r.get("account") or INDIVIDUAL).strip().upper(),
            "expiry":        exp_str,
            "strike":        strike,
            "contracts":     contracts,
            "dte":           dte,
            "pct_itm":       round(pct_itm, 1),
            "current_price": round(current_price, 2),
            "orig_premium":  safe_float(r.get("premium"), 0.0),
        })
        log.info("CSP roll candidate: %s %s %.0fP | %.1f%% ITM | %dd | px %.2f",
                 ticker, exp_str, strike, pct_itm, dte, current_price)

    candidates.sort(key=lambda x: x["pct_itm"], reverse=True)
    return candidates


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
            # Use fill_premium (actual slippage+commission-adjusted fill) so cost
            # basis reflects dollars truly collected. Legacy rows missing fill_premium
            # fall back to premium, then est_premium.
            actual_prem = float(
                r.get("fill_premium") or r.get("premium") or r.get("est_premium") or 0.0
            )
            r["assignment_cost_basis"] = f"{(strike * shares - actual_prem):.2f}"
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
            current_price = get_live_price(ticker)
            if current_price is None:
                log.warning("scan_early_assignments: no price for %s; skipping", ticker)
                continue
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
        shares = contracts * 100
        # fill_premium = actual slippage+commission-adjusted dollars collected.
        # Fall back to premium for legacy rows that predate this field.
        actual_prem = float(
            r.get("fill_premium") or r.get("premium") or 0.0
        )

        r["status"]                     = "ASSIGNED"
        r["close_type"]                 = "ASSIGNED_EARLY"
        r["close_date"]                 = today.isoformat()
        r["underlying_close_at_expiry"] = f"{current_price:.2f}"
        r["shares_if_assigned"]         = str(shares)
        r["assignment_cost_basis"]      = f"{(strike * shares - actual_prem):.2f}"

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
            atr = float(last.get("ATR_14") or 0)
            # Use live price for the strike decision so the CC is placed relative
            # to where the stock actually is right now, not yesterday's close.
            # ATR is computed from historical data (correct — it's a rolling avg).
            live = get_live_price(ticker)
            current_price = live if live else float(last["Close"])
        except Exception as e:
            log.warning("plan_covered_calls: price fetch failed for %s: %s", ticker, e)
            continue

        _decision, raw_target, cc_reason = decide_cc_strike(current_price, net_cost_basis_per_share, atr)

        # Determine the CC underwater tier so we can pick the right DTE window.
        # Mirrors the tier logic inside decide_cc_strike.
        if net_cost_basis_per_share <= 0 or atr <= 0:
            cc_tier = "NORMAL"
        else:
            pct_vs = (current_price - net_cost_basis_per_share) / net_cost_basis_per_share
            if pct_vs >= 0.0:
                cc_tier = "NORMAL"
            elif pct_vs >= -float(CC_UNDERWATER_MILD_PCT):
                cc_tier = "MILD"
            elif pct_vs >= -float(CC_UNDERWATER_DEEP_PCT):
                cc_tier = "DEEP"
            else:
                cc_tier = "SEVERE"

        # Tier-aware DTE: deeper underwater → longer DTE → more premium per cycle.
        dte_min, dte_max = CC_DTE_BY_TIER.get(cc_tier, (CC_TARGET_DTE_MIN, CC_TARGET_DTE_MAX))

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
            exp_str, _ = _pick_expiry_in_dte_range(t, dte_min, dte_max)
            if not exp_str:
                # Fall back to global window if the tier window has no available expiry
                exp_str, _ = _pick_expiry_in_dte_range(t, CC_TARGET_DTE_MIN, CC_TARGET_DTE_MAX)
            if not exp_str:
                log.info(
                    "CC %s: no expiry in DTE window (tier=%s %d–%d or global %d–%d)",
                    ticker, cc_tier, dte_min, dte_max, CC_TARGET_DTE_MIN, CC_TARGET_DTE_MAX,
                )
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
                "credit_mid":    float(net_credit),
                "cc_tier":       cc_tier,
                "reason":        f"[{cc_tier}] {cc_reason} | basis {net_cost_basis_per_share:.2f}",
                "account":       (pos.get("account") or INDIVIDUAL).strip().upper(),
                "source_lot_id": lot_id,
            })
        except Exception as e:
            log.warning("plan_covered_calls failed for %s: %s", ticker, e)
            continue

    return ideas


def execute_cc_roll(
    today: dt.date,
    cc_row: dict,
    roll_up: bool,
) -> Dict[str, object]:
    """Close an existing open CC and open a replacement on the same lot.

    This is the execution path for the interactive CC roll prompt.  The caller
    has already confirmed the action; this function does the work and returns
    a result dict so the caller can report back to the user.

    Parameters
    ----------
    today    : run date
    cc_row   : the OPEN row from cc_positions.csv to be rolled
    roll_up  : True  → roll UP & out (new strike above current price via ATR tier)
               False → roll OUT only  (keep same strike, push expiry out)

    Steps
    -----
    1. Fetch live price + ATR for the ticker.
    2. Determine the new expiry using the same tier-aware DTE window as a fresh CC.
    3. Determine the new strike:
         roll_up   → decide_cc_strike() (ATR-tier logic, same as fresh CC open)
         roll_out  → keep current strike, round to nearest chain strike
    4. Look up both legs on the chain:
         close leg : buyback cost  (ask-side fill: ask - (ask-mid)*OPT_BUY_FILL_PCT)
         open leg  : credit received (bid-side fill: bid + (mid-bid)*OPT_SELL_FILL_PCT)
    5. Compute net credit = new_credit - buyback_cost.
       BLOCK if net_credit < 0 (debit roll policy).
    6. Mark old CC row CLOSED_ROLLED, open new CC row, update parent lot atomically.

    Returns dict with keys:
      ok         : bool
      reason     : human-readable outcome string
      net_credit : float (0.0 if ok=False)
      new_expiry : str
      new_strike : float
    """
    ticker    = (cc_row.get("ticker") or "").strip().upper()
    old_exp   = (cc_row.get("expiry") or "").strip()
    old_strike = safe_float(cc_row.get("strike"), 0.0)
    contracts  = safe_int(cc_row.get("contracts"), 1)
    account    = (cc_row.get("account") or INDIVIDUAL).strip().upper()
    source_lot_id = (cc_row.get("source_lot_id") or "").strip()

    FAIL = lambda msg: {"ok": False, "reason": msg, "net_credit": 0.0,
                        "new_expiry": "", "new_strike": 0.0}

    # ── 1. Price & ATR ────────────────────────────────────────────────────
    try:
        df = add_indicators(download_ohlcv(ticker))
        if df is None or df.empty:
            return FAIL(f"No OHLCV data for {ticker}")
        last = df.iloc[-1]
        atr  = safe_float(last.get("ATR_14"), 0.0)
        live = get_live_price(ticker)
        current_price = live if live else safe_float(last.get("Close"), 0.0)
        if current_price <= 0:
            return FAIL(f"Could not get current price for {ticker}")
    except Exception as e:
        return FAIL(f"Price fetch failed for {ticker}: {e}")

    # ── 2. Tier + DTE window ──────────────────────────────────────────────
    # Derive net_cost_basis from the parent lot for tier calculation.
    try:
        import sys
        _whl = sys.modules.get("wheel")
        lots = _whl._read_rows(_whl.WHEEL_LOTS_FILE) if _whl else []
        parent_lot = next(
            (l for l in lots if (l.get("lot_id") or "").strip() == source_lot_id),
            None
        ) if source_lot_id else None
        net_basis_per_sh = 0.0
        if parent_lot:
            shares_lot = safe_int(parent_lot.get("shares"), 0)
            net_cb     = safe_float(parent_lot.get("net_cost_basis") or
                                    parent_lot.get("cost_basis"), 0.0)
            net_basis_per_sh = (net_cb / shares_lot) if shares_lot > 0 else 0.0
    except Exception:
        net_basis_per_sh = 0.0

    if net_basis_per_sh <= 0 or atr <= 0:
        cc_tier = "NORMAL"
    else:
        pct_vs = (current_price - net_basis_per_sh) / net_basis_per_sh
        if pct_vs >= 0.0:
            cc_tier = "NORMAL"
        elif pct_vs >= -float(CC_UNDERWATER_MILD_PCT):
            cc_tier = "MILD"
        elif pct_vs >= -float(CC_UNDERWATER_DEEP_PCT):
            cc_tier = "DEEP"
        else:
            cc_tier = "SEVERE"

    dte_min, dte_max = CC_DTE_BY_TIER.get(cc_tier, (CC_TARGET_DTE_MIN, CC_TARGET_DTE_MAX))

    # ── 3. New expiry ─────────────────────────────────────────────────────
    try:
        t = yf.Ticker(ticker)
        new_exp, _ = _pick_expiry_in_dte_range(t, dte_min, dte_max)
        if not new_exp:
            new_exp, _ = _pick_expiry_in_dte_range(t, CC_TARGET_DTE_MIN, CC_TARGET_DTE_MAX)
        if not new_exp:
            return FAIL(f"No expiry available in DTE window for {ticker}")
        # Roll must push the expiry OUT, not backwards
        if new_exp <= old_exp:
            # Try one window wider
            new_exp, _ = _pick_expiry_in_dte_range(t, dte_max, dte_max + 21)
        if not new_exp or new_exp <= old_exp:
            return FAIL(
                f"Could not find a later expiry than {old_exp} for {ticker}. "
                f"Try again closer to expiration."
            )
    except Exception as e:
        return FAIL(f"Expiry fetch failed for {ticker}: {e}")

    # ── 4. New strike ─────────────────────────────────────────────────────
    try:
        chain_key_new = f"{ticker}-{new_exp}"
        if chain_key_new not in _chain_cache:
            _chain_cache[chain_key_new] = t.option_chain(new_exp)
        calls_new = _chain_cache[chain_key_new].calls.copy()
        if calls_new.empty:
            return FAIL(f"No call chain for {ticker} {new_exp}")

        if roll_up:
            _dec, raw_target, _reason = decide_cc_strike(
                current_price, net_basis_per_sh, atr
            )
            new_strike = _round_call_strike_to_chain(calls_new, raw_target)
            # Floor: new strike must be above current price (genuinely OTM)
            if new_strike <= current_price:
                otm_fallback = current_price * 1.01
                new_strike = _round_call_strike_to_chain(calls_new, otm_fallback)
        else:
            # Roll out: keep the same strike on the new expiry
            new_strike = _round_call_strike_to_chain(calls_new, old_strike)

        # Apply hard OTM floor regardless of roll type
        if CC_STRIKE_FLOOR_BELOW_CURRENT_PCT > 0:
            floor = current_price * (1.0 - float(CC_STRIKE_FLOOR_BELOW_CURRENT_PCT))
            if new_strike < floor:
                return FAIL(
                    f"New strike {new_strike:.2f} is below OTM floor {floor:.2f} "
                    f"(current {current_price:.2f}). Strike too close to money — wait."
                )
    except Exception as e:
        return FAIL(f"Strike selection failed for {ticker}: {e}")

    # ── 5. Quote both legs ────────────────────────────────────────────────
    try:
        # Close leg: buy back the old CC
        chain_key_old = f"{ticker}-{old_exp}"
        if chain_key_old not in _chain_cache:
            _chain_cache[chain_key_old] = t.option_chain(old_exp)
        calls_old = _chain_cache[chain_key_old].calls.copy()
        old_row = calls_old.loc[calls_old["strike"] == old_strike]
        if old_row.empty:
            return FAIL(f"Old strike {old_strike:.2f} not found in chain for {old_exp}")
        old_row = old_row.iloc[0]
        bid_old = safe_float(old_row.get("bid"), 0.0)
        ask_old = safe_float(old_row.get("ask"), 0.0)
        if ask_old <= 0:
            return FAIL(f"Cannot get buyback quote for {ticker} {old_exp} {old_strike:.0f}C")
        mid_old     = (bid_old + ask_old) / 2.0
        # Buying back: pay between mid and ask (worse than mid, conservative)
        buyback_per_sh = ask_old - (ask_old - mid_old) * float(OPT_BUY_FILL_PCT)
        buyback_total  = buyback_per_sh * 100.0 * contracts

        # Open leg: sell the new CC
        new_row = calls_new.loc[calls_new["strike"] == new_strike]
        if new_row.empty:
            return FAIL(f"New strike {new_strike:.2f} not found in chain for {new_exp}")
        new_row = new_row.iloc[0]
        bid_new = safe_float(new_row.get("bid"), 0.0)
        ask_new = safe_float(new_row.get("ask"), 0.0)
        if bid_new < float(CC_MIN_BID):
            return FAIL(
                f"New CC {ticker} {new_exp} {new_strike:.0f}C bid {bid_new:.2f} "
                f"below minimum {CC_MIN_BID}. No useful premium available."
            )
        mid_new       = (bid_new + ask_new) / 2.0
        credit_per_sh = bid_new + (mid_new - bid_new) * float(OPT_SELL_FILL_PCT)
        commission    = float(OPT_COMMISSION_PER_CONTRACT) * contracts
        credit_total  = credit_per_sh * 100.0 * contracts - commission

    except Exception as e:
        return FAIL(f"Chain quote failed for {ticker}: {e}")

    # ── 6. Net credit check ───────────────────────────────────────────────
    net_credit = credit_total - buyback_total
    if net_credit < 0:
        return FAIL(
            f"Roll is a net DEBIT of ${abs(net_credit):.2f} "
            f"(buyback ${buyback_total:.2f}, new credit ${credit_total:.2f}). "
            f"Debit rolls are blocked by policy. Wait for more decay or try roll-out only."
        )

    # ── 7. Execute: mark old CC closed, open new CC, update lot ──────────
    try:
        rows = load_csv_rows(CC_POSITIONS_FILE)
        old_id = (cc_row.get("id") or "").strip()
        for r in rows:
            if (r.get("id") or "").strip() == old_id:
                r["status"]     = "CLOSED_ROLLED"
                r["close_date"] = today.isoformat()
                r["close_type"] = "ROLLED"
                r["notes"]      = (
                    f"Rolled {'up ' if roll_up else ''}& out → "
                    f"{new_exp} {new_strike:.0f}C | "
                    f"buyback ${buyback_total:.0f}, credit ${credit_total:.0f}, "
                    f"net ${net_credit:.0f}"
                )
                break
        write_csv_rows(CC_POSITIONS_FILE, rows, CC_POSITIONS_COLUMNS)

        # Open the replacement CC
        new_idea = {
            "ticker":        ticker,
            "expiry":        new_exp,
            "strike":        float(new_strike),
            "contracts":     contracts,
            "credit_mid":    credit_per_sh,
            "cc_tier":       cc_tier,
            "reason":        f"[ROLL {'UP+OUT' if roll_up else 'OUT'}] {cc_tier} tier → {new_exp} {new_strike:.0f}C | net ${net_credit:.0f}",
            "account":       account,
            "source_lot_id": source_lot_id,
        }
        new_cc_id = add_cc_position_from_candidate(today.isoformat(), new_idea)

        # Update parent lot: link to new CC id
        import sys
        _whl = sys.modules.get("wheel")
        if _whl and source_lot_id:
            lots = _whl._read_rows(_whl.WHEEL_LOTS_FILE)
            for lot in lots:
                if (lot.get("lot_id") or "").strip() == source_lot_id:
                    lot["cc_id"]      = new_cc_id
                    lot["has_open_cc"] = "1"
                    break
            _whl._write_rows(_whl.WHEEL_LOTS_FILE, lots, _whl.LOT_FIELDS)

        log.info(
            "CC rolled %s: %s %.0fC → %s %.0fC | buyback $%.0f credit $%.0f net $%.0f",
            ticker, old_exp, old_strike, new_exp, new_strike,
            buyback_total, credit_total, net_credit,
        )

        return {
            "ok":         True,
            "reason":     (
                f"Rolled {ticker} {'up ' if roll_up else ''}& out: "
                f"{old_exp} {old_strike:.0f}C → {new_exp} {new_strike:.0f}C | "
                f"buyback ${buyback_total:.0f}  new credit ${credit_total:.0f}  "
                f"net credit ${net_credit:.0f}"
            ),
            "net_credit": net_credit,
            "new_expiry": new_exp,
            "new_strike": new_strike,
        }

    except Exception as e:
        log.error("execute_cc_roll: write failed for %s: %s", ticker, e)
        return FAIL(f"Failed to write position files: {e}")


def execute_cc_close_and_exit(
    today: dt.date,
    cc_row: dict,
) -> Dict[str, object]:
    """Buy to close an open CC and immediately sell the underlying shares.

    This is the full wheel exit path: the position is unwound entirely.
    Used when the trader wants to cut losses or take profits and redeploy
    capital rather than continuing the CC cycle.

    Steps
    -----
    1. Fetch live price for both the share sale and P&L calculation.
    2. Buy back the CC at ask-side fill (same as TP buyback).
    3. Sell shares at live price (stock slippage applied).
    4. Mark CC row CLOSED_MANUAL_EXIT.
    5. Mark wheel lot CLOSED, write stock_trades.csv record with full P&L.
    6. Record a CC_MANUAL_EXIT wheel event so monthly reports are complete.

    Returns dict with keys:
      ok             : bool
      reason         : human-readable outcome
      share_proceeds : float
      cc_buyback     : float
      net_pnl        : float
    """
    ticker        = (cc_row.get("ticker") or "").strip().upper()
    old_exp       = (cc_row.get("expiry") or "").strip()
    old_strike    = safe_float(cc_row.get("strike"), 0.0)
    contracts     = safe_int(cc_row.get("contracts"), 1)
    account       = (cc_row.get("account") or INDIVIDUAL).strip().upper()
    source_lot_id = (cc_row.get("source_lot_id") or "").strip()

    FAIL = lambda msg: {"ok": False, "reason": msg,
                        "share_proceeds": 0.0, "cc_buyback": 0.0, "net_pnl": 0.0}

    # ── 1. Live price ─────────────────────────────────────────────────────
    try:
        live = get_live_price(ticker)
        if not live or live <= 0:
            df = add_indicators(download_ohlcv(ticker))
            if df is None or df.empty:
                return FAIL(f"No price data for {ticker}")
            live = safe_float(df.iloc[-1].get("Close"), 0.0)
        if live <= 0:
            return FAIL(f"Could not get current price for {ticker}")
    except Exception as e:
        return FAIL(f"Price fetch failed for {ticker}: {e}")

    # ── 2. CC buyback quote ───────────────────────────────────────────────
    cc_buyback_total = 0.0
    try:
        t         = yf.Ticker(ticker)
        chain_key = f"{ticker}-{old_exp}"
        if chain_key not in _chain_cache:
            _chain_cache[chain_key] = t.option_chain(old_exp)
        calls = _chain_cache[chain_key].calls.copy()
        row   = calls.loc[calls["strike"] == old_strike]
        if not row.empty:
            row            = row.iloc[0]
            bid_cc         = safe_float(row.get("bid"), 0.0)
            ask_cc         = safe_float(row.get("ask"), 0.0)
            if ask_cc > 0:
                mid_cc           = (bid_cc + ask_cc) / 2.0
                buyback_per_sh   = ask_cc - (ask_cc - mid_cc) * float(OPT_BUY_FILL_PCT)
                cc_buyback_total = buyback_per_sh * 100.0 * contracts
    except Exception as e:
        log.warning(
            "execute_cc_close_and_exit: CC quote failed for %s, treating as $0: %s",
            ticker, e,
        )

    # ── 3. Locate the parent wheel lot ────────────────────────────────────
    import sys
    _whl = sys.modules.get("wheel")
    if not _whl:
        return FAIL("wheel module not available in sys.modules")

    lots = _whl._read_rows(_whl.WHEEL_LOTS_FILE)
    parent_lot = next(
        (l for l in lots if (l.get("lot_id") or "").strip() == source_lot_id),
        None,
    ) if source_lot_id else None

    if not parent_lot:
        return FAIL(
            f"Could not find wheel lot {source_lot_id!r} for {ticker}. "
            f"Cannot exit safely — close the CC manually and update the lot."
        )

    shares    = safe_int(parent_lot.get("shares"), 0)
    net_basis = safe_float(
        parent_lot.get("net_cost_basis") or parent_lot.get("cost_basis"), 0.0
    )
    open_date = (parent_lot.get("open_date") or "").strip()

    if shares <= 0:
        return FAIL(f"Lot {source_lot_id} has 0 shares — cannot sell.")

    # ── 4. Share sale (slippage applied) ──────────────────────────────────
    exit_price     = max(live - float(STOCK_SLIPPAGE_PER_SHARE), 0.01)
    share_proceeds = exit_price * shares

    net_pnl        = share_proceeds - cc_buyback_total - net_basis
    net_pnl_pct    = (net_pnl / net_basis * 100.0) if net_basis > 0 else 0.0
    entry_per_sh   = (net_basis / shares) if shares > 0 else 0.0

    # ── 5. Write all records ──────────────────────────────────────────────
    try:
        # Close the CC row
        cc_rows = load_csv_rows(CC_POSITIONS_FILE)
        old_id  = (cc_row.get("id") or "").strip()
        for r in cc_rows:
            if (r.get("id") or "").strip() == old_id:
                r["status"]     = "CLOSED_MANUAL"
                r["close_date"] = today.isoformat()
                r["close_type"] = "CLOSED_MANUAL_EXIT"
                r["notes"]      = (
                    f"Manual exit: CC buyback ${cc_buyback_total:.0f}, "
                    f"shares sold @ ${exit_price:.2f}, net P&L ${net_pnl:.0f}"
                )
                break
        write_csv_rows(CC_POSITIONS_FILE, cc_rows, CC_POSITIONS_COLUMNS)

        # Close the wheel lot
        for lot in lots:
            if (lot.get("lot_id") or "").strip() == source_lot_id:
                lot["status"]      = "CLOSED"
                lot["has_open_cc"] = "0"
                lot["cc_id"]       = ""
                break
        _whl._write_rows(_whl.WHEEL_LOTS_FILE, lots, _whl.LOT_FIELDS)

        # Stock trade record — appears in stock_monthly report
        _whl._append_stock_trade_record({
            "id":          f"{ticker}-{today.isoformat()}-MANUAL_EXIT",
            "account":     account,
            "ticker":      ticker,
            "entry_date":  open_date,
            "entry_price": f"{entry_per_sh:.4f}",
            "shares":      str(shares),
            "exit_date":   today.isoformat(),
            "exit_price":  f"{exit_price:.2f}",
            "reason":      "MANUAL_EXIT",
            "close_type":  "CC_MANUAL_EXIT",
            "pnl_abs":     f"{net_pnl:.2f}",
            "pnl_pct":     f"{net_pnl_pct:.2f}",
        })

        # Wheel event 1: buyback cost as negative premium so the wheel monthly
        # report correctly nets the CC income against what was paid to close it.
        # Mirrors the CC_CLOSE_TP pattern — premium is negative (cash outflow).
        _whl.record_event(
            date=today.isoformat(),
            account=account,
            ticker=ticker,
            event_type="CC_MANUAL_EXIT_BUYBACK",
            ref_id=old_id,
            expiry=old_exp,
            strike=old_strike,
            contracts=contracts,
            premium=-abs(cc_buyback_total),
        )

        # Wheel event 2: zero-premium marker so the position shows as exited
        # in event logs (useful for backfill / integrity checks).
        _whl.record_event(
            date=today.isoformat(),
            account=account,
            ticker=ticker,
            event_type="CC_MANUAL_EXIT",
            ref_id=old_id,
            expiry=old_exp,
            strike=old_strike,
            contracts=contracts,
            premium=0.0,
        )

        log.info(
            "CC manual exit — %s %d sh sold @ $%.2f, CC buyback $%.0f, "
            "net_basis $%.0f, P&L $%.0f (%.1f%%)",
            ticker, shares, exit_price, cc_buyback_total,
            net_basis, net_pnl, net_pnl_pct,
        )

        return {
            "ok":             True,
            "reason": (
                f"Exited {ticker}: {shares} sh @ ${exit_price:.2f} "
                f"(proceeds ${share_proceeds:.0f}), "
                f"CC buyback ${cc_buyback_total:.0f}, "
                f"net P&L ${net_pnl:+.0f} ({net_pnl_pct:+.1f}%)"
            ),
            "share_proceeds": share_proceeds,
            "cc_buyback":     cc_buyback_total,
            "net_pnl":        net_pnl,
        }

    except Exception as e:
        log.error("execute_cc_close_and_exit: write failed for %s: %s", ticker, e)
        return FAIL(f"Failed to write position files: {e}")


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