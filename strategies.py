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
    RETIREMENT_TARGET_R_MULTIPLE,

    # stock rules
    STOCK_REQUIRE_NEXTDAY_VALIDATION,
    STOCK_MAX_POSITION_PCT,
    STOCK_MAX_OPEN_POSITIONS,
    STOCK_TARGET_R_MULTIPLE,
    STOCK_BREAKEVEN_AFTER_R,
    STOCK_USE_BREAKEVEN_TRAIL,
    STOCK_STOP_ATR_PULLBACK,
    STOCK_STOP_ATR_EMA8,
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
    STOCK_MIN_ADX,
    STOCK_EMA8_PULLBACK_RSI14_MIN,
    STOCK_EMA8_PULLBACK_RSI14_MAX,
    STOCK_EMA8_BAND,
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
    """Resolve a regime-dynamic parameter."""
    if isinstance(param, dict):
        return param.get(regime, param.get("BULL", fallback))
    return param


# ============================================================
# Data / indicators
# ============================================================

_cache = None


def set_data_cache(cache) -> None:
    global _cache
    _cache = cache


_chain_cache: Dict[str, object] = {}
_expiry_cache: Dict[str, tuple] = {}


def reset_chain_cache() -> None:
    global _chain_cache, _expiry_cache
    _chain_cache = {}
    _expiry_cache = {}


def download_ohlcv(ticker: str, period: str = DATA_PERIOD, interval: str = DATA_INTERVAL) -> pd.DataFrame:
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
    """Return the most current price (~15min delayed during market hours).

    Tries yfinance fast_info first (intraday), falls back to last daily close.
    Used everywhere a current price is needed for decisions or display.
    """
    try:
        info  = yf.Ticker(ticker).fast_info
        price = float(info.get("last_price") or info.get("lastPrice") or 0.0)
        if price > 0:
            return price
    except Exception as e:
        log.debug("get_live_price: fast_info failed for %s: %s", ticker, e)

    try:
        df = download_ohlcv(ticker)
        if df is not None and not df.empty:
            return float(df["Close"].iloc[-1])
    except Exception as e:
        log.debug("get_live_price: ohlcv fallback failed for %s: %s", ticker, e)

    return None


def _batch_last_close(tickers: List[str]) -> Dict[str, float]:
    """Batch daily close fetch via yfinance download. Internal helper."""
    prices: Dict[str, float] = {}

    if _cache is not None:
        prices  = _cache.last_closes(tickers)
        tickers = [t for t in tickers if t not in prices]
        if not tickers:
            return prices

    try:
        df = yf.download(
            tickers=" ".join(tickers),
            period="7d", interval="1d",
            auto_adjust=False, progress=False, group_by="column",
        )
    except Exception as e:
        log.warning("_batch_last_close: download failed: %s", e)
        return prices

    if isinstance(df.columns, pd.MultiIndex):
        if "Close" in df.columns.get_level_values(0) and len(df) > 0:
            close = df["Close"].dropna(how="all")
            if len(close) > 0:
                for k, v in close.iloc[-1].to_dict().items():
                    try:
                        prices[str(k).upper()] = float(v)
                    except Exception:
                        pass
    elif "Close" in df.columns and len(df) > 0:
        try:
            prices[tickers[0]] = float(df["Close"].dropna().iloc[-1])
        except Exception:
            pass

    return prices


def live_prices(tickers: List[str]) -> Dict[str, float]:
    """
    Fetch the most current available price for each ticker.

    ≤20 tickers  — calls get_live_price per ticker (fast_info, ~15min delay
                   during market hours, falls back to daily close).
    >20 tickers  — batch yfinance daily download (fast enough for universe scans).

    This is the single price-fetch function used everywhere in the system.
    last_close_prices() is kept as an alias so nothing breaks.
    """
    tickers = sorted({(t or "").strip().upper() for t in tickers if (t or "").strip()})
    if not tickers:
        return {}

    if len(tickers) > 20:
        return _batch_last_close(tickers)

    prices: Dict[str, float] = {}
    for tkr in tickers:
        px = get_live_price(tkr)
        if px and px > 0:
            prices[tkr] = px
        else:
            fb = _batch_last_close([tkr])
            prices.update(fb)
    return prices


def last_close_prices(tickers: List[str]) -> Dict[str, float]:
    """Alias for live_prices — kept for backward compatibility."""
    return live_prices(tickers)


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add all technical indicators including EMA_8 and HIGH_52W."""
    if df is None or df.empty:
        return pd.DataFrame()

    df     = df.copy()
    close  = df["Close"]
    high   = df["High"]
    low    = df["Low"]
    volume = df["Volume"]

    df["SMA_50"]       = ta.trend.sma_indicator(close, window=50)
    df["SMA_200"]      = ta.trend.sma_indicator(close, window=200)
    df["SMA200_SLOPE"] = df["SMA_200"].pct_change(periods=20)
    df["EMA_8"]        = ta.trend.ema_indicator(close, window=8)
    df["EMA_21"]       = ta.trend.ema_indicator(close, window=21)
    df["EMA_50"]       = ta.trend.ema_indicator(close, window=50)
    df["EMA_10"]       = ta.trend.ema_indicator(close, window=10)

    df["ATR_14"]     = ta.volatility.average_true_range(high=high, low=low, close=close, window=14)
    df["RSI_2"]      = ta.momentum.rsi(close, window=2)
    df["RSI_14"]     = ta.momentum.rsi(close, window=14)
    df["ADX_14"]     = ta.trend.adx(high=high, low=low, close=close, window=14)
    df["VOL_SMA_10"] = volume.rolling(window=10).mean()
    df["HIGH_20"]    = close.shift(1).rolling(window=20).max()
    df["LOW_20"]     = close.shift(1).rolling(window=20).min()
    df["HIGH_52W"]   = close.rolling(window=252).max()

    return df


# ============================================================
# Market regime (SPY/QQQ/VIX)
# ============================================================

def market_context_from_dfs(spy_df: pd.DataFrame, qqq_df: pd.DataFrame, vix_df: pd.DataFrame) -> Dict:
    """Compute market context flags including spy_near_52w_high for MOMENTUM regime."""
    spy_df = add_indicators(spy_df)
    qqq_df = add_indicators(qqq_df)
    if spy_df.empty or qqq_df.empty or vix_df is None or vix_df.empty:
        return {
            "spy_close": 0.0, "qqq_close": 0.0, "vix_close": 99.0,
            "spy_above_200": False, "spy_above_50": False, "spy_above_21": False,
            "qqq_above_50": False, "vix_below_18": False, "vix_below_25": False,
            "spy_near_52w_high": False,
        }

    spy_last  = spy_df.iloc[-1]
    qqq_last  = qqq_df.iloc[-1]
    vix_close = float(vix_df["Close"].iloc[-1])
    spy_close = float(spy_last["Close"])
    qqq_close = float(qqq_last["Close"])
    high_52w  = float(spy_last.get("HIGH_52W") or spy_close)

    return {
        "spy_close":         spy_close,
        "qqq_close":         qqq_close,
        "vix_close":         vix_close,
        "spy_above_200":     bool(spy_close > float(spy_last["SMA_200"])),
        "spy_above_50":      bool(spy_close > float(spy_last["SMA_50"])),
        "spy_above_21":      bool(spy_close > float(spy_last["EMA_21"])),
        "qqq_above_50":      bool(qqq_close > float(qqq_last["SMA_50"])),
        "vix_below_18":      bool(vix_close < 18),
        "vix_below_25":      bool(vix_close < 25),
        "spy_near_52w_high": bool(high_52w > 0 and spy_close >= high_52w * 0.97),
    }


def market_context(today: dt.date) -> Dict:
    return market_context_from_dfs(
        download_ohlcv("SPY"), download_ohlcv("QQQ"), download_ohlcv("^VIX"))


def trading_allowed(mkt: Dict) -> bool:
    return bool(mkt.get("spy_above_200") and mkt.get("spy_above_50") and mkt.get("vix_below_25"))


# ============================================================
# Stock entry logic
# ============================================================

def is_eligible(stock_row: pd.Series, regime: str = "BULL") -> bool:
    try:
        close = float(stock_row["Close"])
        sma50 = float(stock_row["SMA_50"])
        ema21 = float(stock_row["EMA_21"])
        adx   = float(stock_row["ADX_14"])
    except Exception as e:
        log.debug("is_eligible: %s", e)
        return False
    return bool(close > sma50 and ema21 > sma50 and adx > float(regime_val(STOCK_MIN_ADX, regime, 18.0)))


def is_csp_eligible(stock_row: pd.Series, *, allow_below_200: bool = False, regime: str = "BULL") -> bool:
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
        log.debug("is_csp_eligible: %s", e)
        return False

    if sma50 <= 0:
        return False

    price_ceiling = float(regime_val(CSP_MAX_STOCK_PRICE, regime, 0.0))
    if price_ceiling and close > price_ceiling:
        log.info("is_csp_eligible REJECT price: close=%.2f > max=%.2f", close, price_ceiling)
        return False

    if not allow_below_200 and sma200 <= 0:
        return False

    min_slope = regime_val(CSP_SMA200_MIN_SLOPE, regime, -0.003)
    if min_slope is None:
        if close < sma200 * 0.95:
            log.info("is_csp_eligible REJECT price-vs-sma200: close=%.2f < sma200*0.95=%.2f", close, sma200 * 0.95)
            return False
    elif sma200_slope is not None:
        if sma200_slope < float(min_slope):
            log.info("is_csp_eligible REJECT slope: slope=%.4f < min=%.4f (close=%.2f sma200=%.2f)",
                     sma200_slope, float(min_slope), close, sma200)
            return False
    else:
        if close < sma200:
            log.info("is_csp_eligible REJECT sma200 fallback: close=%.2f < sma200=%.2f", close, sma200)
            return False
        adx_floor = float(regime_val(CSP_MIN_ADX, regime, 15.0))
        if adx and adx < adx_floor:
            log.info("is_csp_eligible REJECT adx: adx=%.1f < %.1f", adx, adx_floor)
            return False
        return True

    if close < sma50 or (adx and adx < 10):
        return False
    return True


def ema8_pullback_signal(stock_row: pd.Series, regime: str = "BULL") -> bool:
    """EMA8 pullback — primary signal for trending/momentum markets."""
    try:
        close = float(stock_row["Close"])
        ema8  = float(stock_row["EMA_8"])
        ema21 = float(stock_row["EMA_21"])
        sma50 = float(stock_row["SMA_50"])
        rsi14 = float(stock_row["RSI_14"])
        adx   = float(stock_row["ADX_14"])
    except Exception:
        return False

    if not (ema8 > ema21 > sma50):
        return False

    band = float(regime_val(STOCK_EMA8_BAND, regime, 0.018))
    if not (-band * 1.5 <= (close - ema8) / max(ema8, 1e-9) <= band):
        return False

    rsi_min = float(regime_val(STOCK_EMA8_PULLBACK_RSI14_MIN, regime, 40.0))
    rsi_max = float(regime_val(STOCK_EMA8_PULLBACK_RSI14_MAX, regime, 70.0))
    if not (rsi_min <= rsi14 <= rsi_max):
        return False

    return adx >= float(regime_val(STOCK_MIN_ADX, regime, 18.0))


def pullback_signal(stock_row: pd.Series, regime: str = "BULL") -> bool:
    try:
        rsi2  = float(stock_row["RSI_2"])
        ema21 = float(stock_row["EMA_21"])
        close = float(stock_row["Close"])
    except Exception as e:
        log.debug("pullback_signal: %s", e)
        return False
    return bool(
        rsi2 < float(regime_val(STOCK_PULLBACK_RSI2_MAX, regime, 8.0))
        and abs(close - ema21) / max(ema21, 1e-9) < float(regime_val(STOCK_PULLBACK_EMA_BAND, regime, 0.025))
    )


def breakout_signal(stock_row: pd.Series, regime: str = "BULL") -> bool:
    try:
        close   = float(stock_row["Close"])
        high20  = float(stock_row["HIGH_20"])
        vol     = float(stock_row["Volume"])
        vol_sma = float(stock_row["VOL_SMA_10"])
    except Exception as e:
        log.debug("breakout_signal: %s", e)
        return False
    return bool(close > high20 and vol > float(regime_val(STOCK_BREAKOUT_VOL_MULT, regime, 1.2)) * vol_sma)


def nextday_valid_for_entry(signal: str, last: pd.Series) -> bool:
    if not STOCK_REQUIRE_NEXTDAY_VALIDATION:
        return True
    try:
        atr    = float(last.get("ATR_14", 0) or 0)
        high20 = float(last["HIGH_20"])
        close  = float(last["Close"])
    except Exception:
        return False
    return not (signal == "BREAKOUT" and atr > 0 and close > high20 + atr)


# ============================================================
# Retirement holdings inventory
# ============================================================

RETIREMENT_FIELDS = [
    "account", "ticker", "shares", "entry_price", "entry_date",
    "current_price", "pct_change", "breakeven_target", "flag_breakeven_only",
    "target_price", "notes",
]


def ensure_retirement_file() -> None:
    if not os.path.isfile(RETIREMENT_POSITIONS_FILE):
        with open(RETIREMENT_POSITIONS_FILE, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=RETIREMENT_FIELDS).writeheader()


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
    return bool((current_price - entry_price) / entry_price <= -float(RETIREMENT_BREAKEVEN_ONLY_DD_PCT))


def update_retirement_marks() -> Tuple[Dict[str, dict], List[str]]:
    """Update current_price/pct_change + flags using live prices."""
    rows = load_retirement_positions()
    if not rows:
        return {}, []

    tickers = sorted({(r.get("ticker") or "").strip().upper() for r in rows
                      if (r.get("ticker") or "").strip()})
    px_map  = live_prices(tickers)   # live prices for holdings

    flagged: set = set()
    by_key: Dict[str, dict] = {}

    for r in rows:
        acct = (r.get("account") or "").strip().upper()
        tkr  = (r.get("ticker") or "").strip().upper()
        if not acct or not tkr:
            continue

        try:
            entry = float(r.get("entry_price") or 0.0)
        except Exception:
            entry = 0.0

        px = px_map.get(tkr)
        if px is None:
            continue

        r["current_price"] = f"{px:.2f}"
        if entry > 0:
            pct                      = (px - entry) / entry
            r["pct_change"]          = f"{pct*100:.2f}"
            be_only                  = retirement_flag_breakeven_only(entry, px)
            r["flag_breakeven_only"] = "1" if be_only else "0"
            r["breakeven_target"]    = f"{entry:.2f}" if be_only else ""
            if be_only:
                flagged.add(tkr)

        by_key[f"{acct}:{tkr}"] = r

    write_retirement_positions(rows)
    return by_key, sorted(flagged)


def close_retirement_stops(today: dt.date) -> Dict[str, List[str]]:
    """Close retirement positions that have hit their stop or target.

    Fetches live prices at call time so intraday hits are caught immediately.
    """
    if not RETIREMENT_STOP_LOSS_PCT or float(RETIREMENT_STOP_LOSS_PCT) <= 0:
        return {"stopped": [], "targets": []}

    rows = load_retirement_positions()
    if not rows:
        return {"stopped": [], "targets": []}

    tickers = sorted({(r.get("ticker") or "").strip().upper() for r in rows
                      if (r.get("ticker") or "").strip()
                      and (r.get("account") or "").strip().upper() in (IRA, ROTH)})
    px_map = live_prices(tickers) if tickers else {}

    stopped:   List[str] = []
    targets:   List[str] = []
    surviving: List[dict] = []
    changed = False

    for r in rows:
        acct = (r.get("account") or "").strip().upper()
        tkr  = (r.get("ticker") or "").strip().upper()

        if acct not in (IRA, ROTH):
            surviving.append(r)
            continue

        try:
            entry     = float(r.get("entry_price") or 0.0)
            sh        = int(float(r.get("shares") or 0))
            target_px = float(r.get("target_price") or 0.0)
        except Exception as e:
            log.warning("close_retirement_stops: bad field for %s %s: %s", acct, tkr, e)
            surviving.append(r)
            continue

        # Live price — fall back to CSV if unavailable
        cur = px_map.get(tkr)
        if not cur or cur <= 0:
            try:
                cur = float(r.get("current_price") or 0.0)
            except Exception:
                cur = 0.0

        if entry <= 0 or cur <= 0 or sh <= 0:
            surviving.append(r)
            continue

        stop_level  = entry * (1.0 - float(RETIREMENT_STOP_LOSS_PCT))
        exit_reason = None
        if target_px > 0 and cur >= target_px:
            exit_reason = "TARGET"
        elif cur <= stop_level:
            exit_reason = "STOP"

        if not exit_reason:
            surviving.append(r)
            continue

        pnl_abs = (cur - entry) * sh
        pnl_pct = (cur - entry) / entry

        if exit_reason == "STOP":
            log.warning("Retirement stop: %s %s %d sh — entry %.2f, now %.2f (%.1f%%), stop %.2f",
                        acct, tkr, sh, entry, cur, pnl_pct*100, stop_level)
        else:
            log.info("Retirement target hit: %s %s %d sh — entry %.2f, now %.2f (%.1f%%), target %.2f",
                     acct, tkr, sh, entry, cur, pnl_pct*100, target_px)

        append_stock_trade({
            "id":          f"{acct}-{tkr}-{today.isoformat()}-{exit_reason}",
            "account":     acct, "ticker": tkr,
            "entry_date":  (r.get("entry_date") or ""),
            "entry_price": f"{entry:.2f}", "shares": str(sh),
            "exit_date":   today.isoformat(), "exit_price": f"{cur:.2f}",
            "reason":      exit_reason, "close_type": exit_reason,
            "pnl_abs":     f"{pnl_abs:.2f}", "pnl_pct": f"{pnl_pct*100:.2f}",
        })
        append_stock_fill({
            "date": today.isoformat(), "account": acct, "ticker": tkr,
            "action": "CLOSE", "price": f"{cur:.2f}", "shares": str(sh),
            "reason": exit_reason,
        })

        summary = f"{tkr} @{cur:.2f} ({pnl_pct*100:+.1f}%)"
        (stopped if exit_reason == "STOP" else targets).append(summary)
        changed = True

    if changed:
        write_retirement_positions(surviving)

    return {"stopped": stopped, "targets": targets}


def retirement_market_value_by_account(ret_by_key: Dict[str, dict]) -> Dict[str, float]:
    mv = {INDIVIDUAL: 0.0, IRA: 0.0, ROTH: 0.0}
    for _, r in ret_by_key.items():
        acct = (r.get("account") or "").strip().upper()
        if acct not in mv:
            continue
        try:
            mv[acct] += float(r.get("shares") or 0.0) * float(r.get("current_price") or 0.0)
        except Exception:
            pass
    return mv


STOCK_POS_FIELDS = [
    "id", "account", "ticker", "signal", "plan_date", "entry_date",
    "entry_price", "shares", "adds", "last_add_date", "initial_entry_price",
    "initial_shares", "stop_price", "target_price", "risk_per_share",
    "r_multiple_target", "status", "exit_date", "exit_price", "exit_reason",
    "pnl_abs", "pnl_pct", "notes",
]
STOCK_TRADE_FIELDS = [
    "id", "account", "ticker", "entry_date", "entry_price", "shares",
    "exit_date", "exit_price", "reason", "close_type", "pnl_abs", "pnl_pct",
]
STOCK_FILL_FIELDS = ["date", "account", "ticker", "action", "price", "shares", "reason"]


def ensure_stock_files() -> None:
    for path, fields in [
        (STOCK_POSITIONS_FILE, STOCK_POS_FIELDS),
        (STOCK_TRADES_FILE,    STOCK_TRADE_FIELDS),
        (STOCK_FILLS_FILE,     STOCK_FILL_FIELDS),
    ]:
        if not os.path.isfile(path):
            with open(path, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=fields).writeheader()


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
        csv.DictWriter(f, fieldnames=STOCK_TRADE_FIELDS).writerow(
            {k: row.get(k, "") for k in STOCK_TRADE_FIELDS})


def append_stock_fill(row: dict) -> None:
    ensure_stock_files()
    with open(STOCK_FILLS_FILE, "a", newline="") as f:
        csv.DictWriter(f, fieldnames=STOCK_FILL_FIELDS).writerow(
            {k: row.get(k, "") for k in STOCK_FILL_FIELDS})


def rebuild_stock_monthly_from_trades() -> None:
    from config import IRA_ACCOUNTS
    ensure_stock_files()
    if not os.path.isfile(STOCK_TRADES_FILE):
        return
    with open(STOCK_TRADES_FILE, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return

    os.makedirs(STOCK_MONTHLY_DIR, exist_ok=True)
    out_fields = ["date", "account", "ticker", "shares",
                  "entry_price", "exit_price", "close_type", "pnl_abs", "pnl_pct"]

    by_bucket: Dict[tuple, List[dict]] = {}
    for r in rows:
        d = (r.get("exit_date") or "").strip()
        if len(d) < 7:
            continue
        month = d[:7]
        acct  = (r.get("account") or INDIVIDUAL).strip().upper()
        by_bucket.setdefault((month, "IRA" if acct in IRA_ACCOUNTS else "INDIVIDUAL"), []).append(r)

    for (month, group), mrows in sorted(by_bucket.items()):
        total    = 0.0
        out_rows = []
        for r in sorted(mrows, key=lambda x: x.get("exit_date") or ""):
            try:
                pnl = float(r.get("pnl_abs") or 0.0)
            except Exception:
                pnl = 0.0
            total += pnl
            out_rows.append({
                "date": r.get("exit_date",""), "account": r.get("account",""),
                "ticker": r.get("ticker",""), "shares": r.get("shares",""),
                "entry_price": r.get("entry_price",""), "exit_price": r.get("exit_price",""),
                "close_type": r.get("close_type",""), "pnl_abs": f"{pnl:.2f}",
                "pnl_pct": r.get("pnl_pct",""),
            })
        out_rows.append({"date":"","account":"","ticker":"TOTAL","shares":"",
                         "entry_price":"","exit_price":"","close_type":"",
                         "pnl_abs": f"{total:.2f}","pnl_pct":""})
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
        tkr  = (r.get("ticker") or "").strip().upper()
        if acct not in mv or not tkr:
            continue
        px = prices.get(tkr)
        if px is None:
            continue
        try:
            mv[acct] += float(r.get("shares") or 0.0) * px
        except Exception:
            pass
    return mv


def plan_stock_trade(
    *,
    account: str,
    ticker: str,
    signal: str,
    last: pd.Series,
    mkt: Dict,
    existing_open_tickers: set,
    acct_current_mv: float,
    retirement_breakeven_only: bool,
    regime: str = "BULL",
) -> Optional[dict]:
    """Build a paper trade plan.

    Entry price: live price via get_live_price() (~15min delayed intraday,
    falls back to last daily close outside market hours).
    Indicators (EMA8, EMA21, ATR, HIGH_20): still from daily OHLCV — correct.
    """
    account = account.upper().strip()
    ticker  = ticker.upper().strip()
    signal  = signal.upper().strip()

    if not ticker or ticker in existing_open_tickers:
        return None

    try:
        ema21  = float(last["EMA_21"])
        ema8   = float(last.get("EMA_8", ema21) or ema21)
        atr    = float(last.get("ATR_14", 0) or 0)
        high20 = float(last["HIGH_20"])
    except Exception as e:
        log.warning("plan_stock_trade: indicator field missing for %s: %s", ticker, e)
        return None

    # Live price for entry — most recent available
    live_px = get_live_price(ticker)
    close   = live_px if (live_px and live_px > 0) else float(last.get("Close", 0) or 0)

    if close <= 0:
        return None
    if signal not in ("PULLBACK", "BREAKOUT", "EMA8_PULLBACK"):
        return None

    log.info("plan_stock_trade: %s price=%.2f (live=%s daily=%.2f)",
             ticker, close,
             f"{live_px:.2f}" if live_px else "n/a",
             float(last.get("Close", 0) or 0))

    # ── Retirement buy-and-hold ───────────────────────────────────────────────
    if account in (IRA, ROTH):
        if signal not in ("PULLBACK", "EMA8_PULLBACK"):
            return None
        if ticker not in RETIREMENT_STOCKS or retirement_breakeven_only:
            return None

        slice_cap = float(RETIREMENT_STOCK_CAPS.get(account, 0))
        if slice_cap <= 0:
            return None

        pos_value = slice_cap * float(RETIREMENT_POSITION_SIZE_PCT)
        shares    = int(pos_value / close)
        if shares < 1:
            return None

        remaining = max(slice_cap - float(acct_current_mv or 0.0), 0.0)
        if remaining < pos_value * 0.5:
            return None

        shares = min(shares, int(remaining / close))
        if shares < 1:
            return None

        stop    = close * (1.0 - float(RETIREMENT_STOP_LOSS_PCT))
        risk_ps = close - stop
        target  = close + RETIREMENT_TARGET_R_MULTIPLE * risk_ps

        return {
            "account": account, "ticker": ticker, "signal": signal,
            "entry_price": float(close), "stop_price": float(stop),
            "target_price": float(target), "shares": int(shares),
            "risk_per_share": float(risk_ps), "r_multiple_target": 0.0,
            "notes": (f"RETIRE | pos_value=${pos_value:,.0f} "
                      f"stop={RETIREMENT_STOP_LOSS_PCT*100:.0f}% below | "
                      f"target={RETIREMENT_TARGET_R_MULTIPLE}R (${target:.2f})"),
        }

    # ── INDIVIDUAL swing trade ────────────────────────────────────────────────
    if not nextday_valid_for_entry(signal, last):
        return None

    if signal == "EMA8_PULLBACK":
        stop    = ema8 - (STOCK_STOP_ATR_EMA8 * atr) if atr > 0 else ema8 * 0.97
        risk_ps = max(close - stop, 0.01)
        target  = close + STOCK_TARGET_R_MULTIPLE * risk_ps
    elif signal == "PULLBACK":
        stop    = ema21 - (STOCK_STOP_ATR_PULLBACK * atr) if atr > 0 else ema21 * 0.97
        risk_ps = max(close - stop, 0.01)
        target  = max(high20, close + STOCK_TARGET_R_MULTIPLE * risk_ps)
    else:  # BREAKOUT
        stop    = high20 - (STOCK_STOP_ATR_BREAKOUT * atr) if atr > 0 else high20 * 0.96
        risk_ps = max(close - stop, 0.01)
        target  = close + STOCK_TARGET_R_MULTIPLE * risk_ps

    acct_size = float(INDIVIDUAL_STOCK_CAP)
    if acct_size <= 0:
        return None

    max_pos_pct      = float(regime_val(STOCK_MAX_POSITION_PCT, regime, 0.20))
    max_pos_value    = acct_size * max_pos_pct
    remaining_value  = max(acct_size - float(acct_current_mv or 0.0), 0.0)
    value_cap_shares = int(max_pos_value / close)
    remaining_shares = int(remaining_value / close)

    if value_cap_shares < 1 or remaining_shares < 1:
        return None

    shares       = min(value_cap_shares, remaining_shares)
    risk_dollars = risk_ps * shares

    return {
        "account": account, "ticker": ticker, "signal": signal,
        "entry_price": float(close), "stop_price": float(stop),
        "target_price": float(target), "shares": int(shares),
        "risk_per_share": float(risk_ps), "r_multiple_target": float(STOCK_TARGET_R_MULTIPLE),
        "notes": (f"{signal} | max_pos=${max_pos_value:,.0f} ({max_pos_pct*100:.0f}% of ${acct_size:,.0f}) "
                  f"| risk=${risk_dollars:,.0f} ({risk_ps:.2f}/sh) | regime={regime}"),
    }


def execute_stock_plan(today: dt.date, plan: dict) -> str:
    account    = (plan.get("account") or "").strip().upper()
    entry_date = today.isoformat()

    if account in (IRA, ROTH):
        ticker   = (plan.get("ticker") or "").strip().upper()
        rows     = load_retirement_positions()
        if any((r.get("account") or "").strip().upper() == account
               and (r.get("ticker") or "").strip().upper() == ticker for r in rows):
            return f"{account}-{ticker}-{entry_date}"
        entry_px = float(plan["entry_price"])
        rows.append({
            "account": account, "ticker": ticker,
            "shares": str(int(plan["shares"])),
            "entry_price": f"{entry_px:.2f}", "entry_date": entry_date,
            "current_price": f"{entry_px:.2f}", "pct_change": "0.00",
            "breakeven_target": "", "flag_breakeven_only": "0",
            "target_price": f"{float(plan.get('target_price', 0.0)):.2f}",
            "notes": plan.get("notes", "BUY-HOLD"),
        })
        write_retirement_positions(rows)
        log.info("execute_stock_plan: %s %s %d sh @ %.2f → retirement_positions",
                 account, ticker, int(plan["shares"]), entry_px)
        return f"{account}-{ticker}-{entry_date}"

    ensure_stock_files()
    rows   = load_stock_positions()
    pos_id = _stock_position_id(account, plan["ticker"], entry_date)
    if any((r.get("id") or "") == pos_id for r in rows):
        return pos_id

    rows.append({
        "id": pos_id, "account": account, "ticker": plan["ticker"],
        "signal": plan["signal"], "plan_date": entry_date, "entry_date": entry_date,
        "entry_price":       f"{float(plan['entry_price']):.2f}",
        "shares":            str(int(plan["shares"])),
        "stop_price":        f"{float(plan['stop_price']):.2f}",
        "target_price":      f"{float(plan['target_price']):.2f}",
        "risk_per_share":    f"{float(plan['risk_per_share']):.4f}",
        "r_multiple_target": f"{float(plan['r_multiple_target']):.2f}",
        "status": "OPEN", "exit_date": "", "exit_price": "", "exit_reason": "",
        "pnl_abs": "", "pnl_pct": "", "notes": plan.get("notes", ""),
    })
    write_stock_positions(rows)
    return pos_id


def update_and_close_stock_positions(today: dt.date, mkt: Dict) -> Dict[str, List[str]]:
    """Check open swing positions against live prices and close on stop/target."""
    ensure_stock_files()
    rows      = load_stock_positions()
    open_rows = [r for r in rows if (r.get("status") or "").upper() == "OPEN"]
    if not open_rows:
        return {"stops": [], "targets": []}

    tickers = sorted({(r.get("ticker") or "").strip().upper()
                      for r in open_rows if (r.get("ticker") or "").strip()})
    prices  = live_prices(tickers)   # live prices for open positions

    stops: List[str] = []; targets: List[str] = []; changed = False

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        tkr = (r.get("ticker") or "").strip().upper()
        px  = prices.get(tkr)
        if px is None:
            continue

        try:
            entry  = float(r.get("entry_price") or 0.0)
            stop   = float(r.get("stop_price") or 0.0)
            target = float(r.get("target_price") or 0.0)
            sh     = int(float(r.get("shares") or 0))
        except Exception as e:
            log.warning("update_and_close_stock_positions: bad field for %s: %s", tkr, e)
            continue

        if sh <= 0 or entry <= 0:
            continue

        if STOCK_USE_BREAKEVEN_TRAIL:
            try:
                risk_ps = float(r.get("risk_per_share") or 0.0)
                if risk_ps > 0 and (px - entry) >= (STOCK_BREAKEVEN_AFTER_R * risk_ps):
                    new_stop = max(stop, entry)
                    if new_stop != stop:
                        stop = new_stop; r["stop_price"] = f"{stop:.2f}"; changed = True
            except Exception:
                pass

        exit_reason = None
        if stop > 0 and px <= stop:
            exit_reason = "STOP"
        elif target > 0 and px >= target:
            exit_reason = "TARGET"

        if not exit_reason:
            continue

        pnl_abs = (px - entry) * sh
        pnl_pct = (px - entry) / entry
        r["status"]      = "CLOSED"
        r["exit_date"]   = today.isoformat()
        r["exit_price"]  = f"{px:.2f}"
        r["exit_reason"] = exit_reason
        r["pnl_abs"]     = f"{pnl_abs:.2f}"
        r["pnl_pct"]     = f"{pnl_pct*100:.2f}"
        changed = True

        append_stock_trade({
            "id": r.get("id",""), "account": r.get("account",""), "ticker": tkr,
            "entry_date": r.get("entry_date",""), "entry_price": r.get("entry_price",""),
            "shares": r.get("shares",""), "exit_date": r.get("exit_date",""),
            "exit_price": r.get("exit_price",""), "reason": exit_reason,
            "close_type": exit_reason, "pnl_abs": r.get("pnl_abs",""),
            "pnl_pct": r.get("pnl_pct",""),
        })
        append_stock_fill({
            "date": today.isoformat(), "account": r.get("account",""), "ticker": tkr,
            "action": "CLOSE", "price": f"{px:.2f}", "shares": str(int(sh)),
            "reason": exit_reason,
        })
        (stops if exit_reason == "STOP" else targets).append(f"{tkr} @{px:.2f}")

    if changed:
        write_stock_positions(rows)
    return {"stops": stops, "targets": targets}


# ============================================================
# CSP planning / bookkeeping (paper)
# ============================================================

def ensure_positions_files() -> None:
    for path, cols in [(CSP_POSITIONS_FILE, CSP_POSITIONS_COLUMNS),
                       (CC_POSITIONS_FILE,  CC_POSITIONS_COLUMNS)]:
        if not os.path.isfile(path):
            with open(path, "w", newline="") as f:
                csv.DictWriter(f, fieldnames=cols).writeheader()


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
    fieldnames  = ["date","week_id","account","ticker","expiry","strike","contracts","premium","cash_reserved","tier"]
    file_exists = os.path.isfile(CSP_LEDGER_FILE)
    try:
        contracts = int(float(row.get("contracts") or 0)) or 1
    except Exception:
        contracts = 1
    if "premium" not in row or row.get("premium") in ("", None):
        try:
            row["premium"] = round(float(row.get("credit_mid") or 0.0) * 100.0 * contracts, 2)
        except Exception:
            row["premium"] = ""
    for fld in ("strike", "premium", "cash_reserved"):
        try:
            if row.get(fld) not in ("", None):
                row[fld] = f"{float(row[fld]):.2f}"
        except Exception:
            pass
    if file_exists:
        try:
            with open(CSP_LEDGER_FILE, "rb") as f:
                f.seek(0, 2)
                if f.tell() > 0:
                    f.seek(-1, 2)
                    if f.read(1) != b"\n":
                        with open(CSP_LEDGER_FILE, "a") as fa:
                            fa.write("\n")
        except Exception:
            pass
    with open(CSP_LEDGER_FILE, mode="a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        if not file_exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in fieldnames})


def csp_already_logged(ledger_rows, week_id, ticker, expiry, strike, account=""):
    for r in ledger_rows:
        try:
            if (r["week_id"] == week_id and r["ticker"] == ticker and r["expiry"] == expiry
                    and abs(float(r["strike"]) - float(strike)) < 0.005):
                row_acct = (r.get("account") or "").strip().upper()
                chk_acct = (account or "").strip().upper()
                if row_acct and chk_acct and row_acct != chk_acct:
                    continue
                return True
        except Exception:
            continue
    return False


def has_upcoming_ex_dividend(ticker: str, days_window: int = 10) -> bool:
    from config import CSP_TICKER_SECTOR
    if CSP_TICKER_SECTOR.get(ticker, "OTHER") == "ETF_BROAD":
        return False
    try:
        today  = dt.date.today()
        cutoff = today + dt.timedelta(days=days_window)
        divs   = yf.Ticker(ticker).dividends
        if divs is None or divs.empty:
            return False
        for ts in divs.index:
            try:
                ex_date = ts.date() if hasattr(ts, "date") else dt.date.fromisoformat(str(ts)[:10])
                if today <= ex_date <= cutoff:
                    log.info("has_upcoming_ex_dividend: %s ex-div %s within %d days", ticker, ex_date, days_window)
                    return True
            except Exception:
                continue
    except Exception as e:
        log.debug("has_upcoming_ex_dividend: %s: %s", ticker, e)
    return False


def has_earnings_within_window(ticker: str, expiry_str: str, buffer_days: int = 2) -> bool:
    from config import CSP_TICKER_SECTOR
    if CSP_TICKER_SECTOR.get(ticker, "OTHER") == "ETF_BROAD":
        return False
    try:
        today  = dt.date.today()
        cutoff = dt.date.fromisoformat(expiry_str) + dt.timedelta(days=buffer_days)
        cal    = yf.Ticker(ticker).calendar
        if cal is None or (hasattr(cal, "empty") and cal.empty):
            return False
        earn_raw = cal.get("Earnings Date") if isinstance(cal, dict) else (
            cal.loc["Earnings Date"].iloc[0] if "Earnings Date" in cal.index else None)
        if earn_raw is None:
            return False
        dates = []
        if hasattr(earn_raw, "__iter__") and not isinstance(earn_raw, str):
            for v in earn_raw:
                try:
                    dates.append(v.date() if hasattr(v, "date") else dt.date.fromisoformat(str(v)[:10]))
                except Exception:
                    pass
        else:
            try:
                dates.append(earn_raw.date() if hasattr(earn_raw, "date") else dt.date.fromisoformat(str(earn_raw)[:10]))
            except Exception:
                pass
        for earn_date in dates:
            if today <= earn_date <= cutoff:
                log.info("has_earnings_within_window: %s earnings %s — skipping", ticker, earn_date)
                return True
    except Exception as e:
        log.debug("has_earnings_within_window: %s: %s", ticker, e)
    return False


def _pick_expiry_in_dte_range(ticker_obj: yf.Ticker, dte_min: int, dte_max: int):
    today = dt.date.today()
    sym   = ticker_obj.ticker
    if sym not in _expiry_cache:
        _expiry_cache[sym] = ticker_obj.options
    for exp_str in _expiry_cache[sym]:
        try:
            dte = (dt.date.fromisoformat(exp_str) - today).days
            if dte_min <= dte <= dte_max:
                return exp_str, dte
        except Exception:
            continue
    return None, None


def _suggest_put_strike(stock_last, atr_mult, *, risk_off=False, min_otm_pct=0.0, base_ma="EMA_21"):
    close = float(stock_last["Close"])
    atr14 = float(stock_last.get("ATR_14", 0) or 0)
    bmu   = (base_ma or "").upper()
    base  = (float(stock_last.get("SMA_50", close) or close) if bmu == "SMA_50" else
             float(stock_last.get("EMA_50", close) or close) if bmu == "EMA_50" else
             float(stock_last.get("EMA_21", close) or close) if bmu == "EMA_21" else close)
    raw = (base - atr_mult * atr14) if CSP_STRIKE_MODE == "ema21_atr" else close * 0.92
    if min_otm_pct and close > 0:
        raw = min(raw, close * (1.0 - float(min_otm_pct)))
    return float(raw)


def _round_strike_to_chain(puts_df, target_strike):
    strikes = sorted([float(s) for s in puts_df["strike"].tolist()])
    below   = [s for s in strikes if s <= target_strike]
    return below[-1] if below else strikes[0]


def evaluate_csp_candidate(ticker, df, atr_mult=0.50, *, risk_off=False,
                            min_otm_pct=0.0, base_ma=CSP_STRIKE_BASE_NORMAL):
    if df is None or df.empty:
        return None
    stock_last = df.iloc[-1]
    try:
        close_px = float(stock_last.get("Close", 0) or 0)
    except Exception:
        return None
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
        puts = _chain_cache[chain_key].puts.copy()
        if puts.empty:
            return None
        raw_strike = _suggest_put_strike(stock_last, atr_mult, risk_off=risk_off,
                                         min_otm_pct=min_otm_pct, base_ma=base_ma)
        strike = _round_strike_to_chain(puts, raw_strike)
        row    = puts.loc[puts["strike"] == strike]
        if row.empty:
            return None
        row  = row.iloc[0]
        bid  = float(row.get("bid", 0) or 0)
        ask  = float(row.get("ask", 0) or 0)
        _oi  = float(row.get("openInterest", 0) or 0)
        _vol = float(row.get("volume", 0) or 0)
        oi   = int(_oi)  if math.isfinite(_oi)  else 0
        vol  = int(_vol) if math.isfinite(_vol) else 0
        iv   = float(row.get("impliedVolatility", 0) or 0)
        if bid < CSP_MIN_BID or ask <= 0 or ask < bid:
            return None
        oi_floor = CSP_MIN_OI_ETF if get_ticker_sector(ticker) == "ETF_BROAD" else CSP_MIN_OI
        if oi < oi_floor or vol < CSP_MIN_VOLUME or (CSP_MIN_IV and iv < CSP_MIN_IV):
            return None
        mid           = (bid + ask) / 2.0
        cpc           = strike * 100.0
        contracts     = min(int(CSP_MAX_CASH_PER_TRADE // cpc), int(CSP_MAX_CONTRACTS))
        if contracts < 1:
            return None
        est_premium   = mid * 100.0 * contracts
        cash_reserved = cpc * contracts
        yield_pct     = est_premium / cash_reserved if cash_reserved > 0 else 0.0
        fill_premium  = round(
            (bid + (mid - bid) * float(OPT_SELL_FILL_PCT)) * 100.0 * contracts
            - OPT_COMMISSION_PER_CONTRACT * contracts, 2)
        return {
            "ticker": ticker, "expiry": exp_str, "dte": int(dte or 0),
            "strike": float(strike), "bid": bid, "ask": ask, "mid": mid, "iv": iv,
            "contracts": int(contracts), "cash_reserved": float(cash_reserved),
            "est_premium": float(est_premium), "fill_premium": float(fill_premium),
            "yield_pct": float(yield_pct), "atr_mult": float(atr_mult),
            "reason": f"Strike≈{base_ma}-{atr_mult:.2f}*ATR, minOTM={min_otm_pct:.0%} (raw {raw_strike:.2f})",
        }
    except Exception as e:
        log.warning("evaluate_csp_candidate failed for %s: %s", ticker, e)
        return None


def csp_regime(vix_close: float) -> str:
    return "NORMAL" if vix_close <= float(CSP_RISK_OFF_VIX) else "RISK_OFF"


def classify_csp_tier(idea: dict, regime: str = "BULL") -> str:
    prem = float(idea["est_premium"]); y = float(idea["yield_pct"])
    if prem >= CSP_MIN_PREMIUM_AGGRESSIVE and y >= float(regime_val(CSP_MIN_YIELD_AGGRESSIVE, regime, 0.018)):
        return "AGGRESSIVE"
    if prem >= CSP_MIN_PREMIUM_BALANCED and y >= float(regime_val(CSP_MIN_YIELD_BALANCED, regime, 0.013)):
        return "BALANCED"
    if prem >= CSP_MIN_PREMIUM_CONSERVATIVE and y >= float(regime_val(CSP_MIN_YIELD_CONSERVATIVE, regime, 0.008)):
        return "CONSERVATIVE"
    return "REJECT"


def score_csp_idea(idea: dict) -> float:
    s  = min(float(idea["est_premium"]) / 250.0, 2.0)
    s += min(float(idea["yield_pct"]) / 0.04, 2.0)
    s += min(float(idea["iv"]) / 0.45, 1.5)
    s += 0.5 if 30 <= float(idea["dte"]) <= 40 else 0.0
    return float(s)


def allowed_tiers_for_regime(reg: str) -> set:
    return {"CONSERVATIVE", "BALANCED", "AGGRESSIVE"}


def classify_csp_tier_for_regime(idea: dict, reg: str) -> str:
    return classify_csp_tier(idea, regime=reg)


def get_ticker_sector(ticker: str) -> str:
    return CSP_TICKER_SECTOR.get((ticker or "").strip().upper(), "OTHER")


def plan_weekly_csp_orders(
    csp_candidates, *, today, vix_close, total_remaining_cap, week_remaining_cap,
    aggressive_total, aggressive_week, open_sector_counts={}, live_vix=None, regime="BULL",
):
    from config import VIX_INTRADAY_SPIKE_THRESHOLD
    week_id    = iso_week_id(today)
    reg        = regime
    allowed    = allowed_tiers_for_regime(reg)
    vix_spiked = live_vix is not None and live_vix > vix_close + float(VIX_INTRADAY_SPIKE_THRESHOLD)
    if vix_spiked:
        log.warning("Intraday VIX spike: %.2f vs %.2f — AGGRESSIVE downgraded.", live_vix, vix_close)

    enriched = []
    for idea in csp_candidates:
        tier = classify_csp_tier_for_regime(idea, reg)
        if tier == "REJECT" or tier not in allowed:
            continue
        if vix_spiked and tier == "AGGRESSIVE":
            tier = "BALANCED"
        idea2 = dict(idea); idea2["tier"] = tier; idea2["score"] = score_csp_idea(idea2)
        enriched.append(idea2)
    enriched.sort(key=lambda x: x["score"], reverse=True)

    selected = []; used = set()
    total_remaining = float(total_remaining_cap)
    week_remaining  = float(week_remaining_cap)
    sector_counts   = dict(open_sector_counts)

    for idea in enriched:
        tkr = idea["ticker"]
        if tkr in used:
            continue
        cpc = float(idea.get("strike", 0)) * 100.0
        if cpc <= 0:
            continue
        budget    = min(week_remaining, total_remaining)
        contracts = min(int(budget // cpc), int(CSP_MAX_CONTRACTS))
        if contracts < 1:
            continue
        cash = cpc * contracts
        if cash > week_remaining or cash > total_remaining:
            continue
        if idea["tier"] == "AGGRESSIVE":
            if aggressive_total >= CSP_MAX_AGGRESSIVE_TOTAL or aggressive_week >= CSP_MAX_AGGRESSIVE_PER_WEEK:
                continue
        sector     = get_ticker_sector(tkr)
        sector_cap = int(regime_val(CSP_MAX_POSITIONS_PER_SECTOR, reg, 3))
        if sector != "OTHER" and sector_counts.get(sector, 0) >= sector_cap:
            continue
        orig  = max(int(idea.get("contracts", 1)), 1)
        scale = contracts / orig
        selected.append({
            **idea, "contracts": contracts, "cash_reserved": cash,
            "est_premium":  round(float(idea.get("est_premium", 0)) * scale, 2),
            "fill_premium": round(float(idea.get("fill_premium", idea.get("est_premium", 0))) * scale, 2),
        })
        used.add(tkr)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        week_remaining  -= cash; total_remaining -= cash
        if idea["tier"] == "AGGRESSIVE":
            aggressive_total += 1; aggressive_week += 1
        if week_remaining < CSP_MAX_CASH_PER_TRADE * 0.8:
            break

    return {
        "week_id": week_id, "regime": reg, "vix_close": float(vix_close),
        "selected": selected,
        "week_remaining_after":  max(week_remaining, 0.0),
        "total_remaining_after": max(total_remaining, 0.0),
    }


def load_open_csp_tickers(today=None):
    ensure_positions_files()
    out = set()
    for r in load_csv_rows(CSP_POSITIONS_FILE):
        if (r.get("status") or "").strip().upper() != "OPEN":
            continue
        tkr = (r.get("ticker") or "").strip().upper()
        if not tkr:
            continue
        if today:
            try:
                exp_str = (r.get("expiry") or "").strip()
                if exp_str and dt.date.fromisoformat(exp_str) < today:
                    continue
            except Exception:
                pass
        out.add(tkr)
    return out


def make_csp_position_id(ticker, expiry, strike, open_date):
    return f"{ticker}-{expiry}-{float(strike):.2f}-{open_date}-{dt.datetime.now().strftime('%H%M%S')}"


def add_csp_position_from_selected(today, week_id, idea):
    ensure_positions_files()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    tkr  = (idea.get("ticker") or "").strip().upper()
    if tkr:
        for r in rows:
            if (r.get("status") or "").strip().upper() != "OPEN":
                continue
            if (r.get("ticker") or "").strip().upper() == tkr:
                eid = (r.get("id") or "").strip()
                return (eid or make_csp_position_id(tkr, r.get("expiry") or "",
                        float(r.get("strike") or 0.0), r.get("open_date") or today), False)
    pos_id = make_csp_position_id(tkr, idea["expiry"], idea["strike"], today)
    if any((r.get("id") or "") == pos_id for r in rows):
        return (pos_id, False)
    rows.append({
        "id": pos_id, "account": (idea.get("account") or INDIVIDUAL).strip().upper(),
        "open_date": today, "week_id": week_id, "ticker": tkr,
        "expiry": idea["expiry"], "dte_open": str(int(idea["dte"])),
        "strike": f"{float(idea['strike']):.2f}", "contracts": str(int(idea["contracts"])),
        "cash_reserved": f"{float(idea['cash_reserved']):.2f}",
        "premium": f"{float(idea['est_premium']):.2f}",
        "fill_premium": f"{float(idea.get('fill_premium') or idea.get('est_premium') or 0.0):.2f}",
        "tier": idea.get("tier",""), "status": "OPEN", "close_date": "",
        "close_type": "", "underlying_close_at_expiry": "",
        "shares_if_assigned": "", "assignment_cost_basis": "", "notes": "",
    })
    write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)
    return (pos_id, True)


def _fetch_option_quote(ticker, exp_str, strike, option_type="put"):
    """Fetch bid/ask/mid for a single option. Returns (bid, ask, mid) or None."""
    chain_key = f"{ticker}-{exp_str}"
    if chain_key not in _chain_cache:
        _chain_cache[chain_key] = yf.Ticker(ticker).option_chain(exp_str)
    chain = _chain_cache[chain_key]
    df    = chain.puts if option_type == "put" else chain.calls
    if df is None or df.empty:
        return None
    row = df.loc[df["strike"] == strike]
    if row.empty:
        return None
    row = row.iloc[0]
    bid = safe_float(row.get("bid"), 0.0)
    ask = safe_float(row.get("ask"), 0.0)
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    return bid, ask, (bid + ask) / 2.0


def process_csp_take_profits(today: dt.date, regime: str = "BULL") -> Dict:
    ensure_positions_files()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    closed = []; changed = False

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        exp_str = (r.get("expiry") or "").strip()
        try:
            if exp_str and dt.date.fromisoformat(exp_str) <= today:
                continue
        except Exception:
            continue
        ticker       = (r.get("ticker") or "").strip().upper()
        strike       = safe_float(r.get("strike"), 0.0)
        contracts    = safe_int(r.get("contracts"), 0)
        orig_premium = safe_float(r.get("premium"), 0.0)
        if orig_premium <= 0 or strike <= 0 or contracts < 1 or not ticker:
            continue

        tp_pct = float(regime_val(CSP_TAKE_PROFIT_PCT, regime, 0.60))
        quote  = _fetch_option_quote(ticker, exp_str, strike, "put")
        if quote is None:
            continue
        bid, ask, mid = quote
        spread_pct = (ask - bid) / mid
        if spread_pct > float(CSP_TP_MAX_SPREAD_PCT):
            log.info("CSP TP %s %s: spread %.0f%% too wide", ticker, exp_str, spread_pct*100)
            continue
        current_value = mid * 100.0 * contracts
        if current_value > orig_premium * tp_pct:
            continue

        profit = orig_premium - current_value
        r["status"] = "CLOSED_TP"; r["close_date"] = today.isoformat()
        r["close_type"] = "CLOSED_TAKE_PROFIT"
        r["notes"] = f"TP at {tp_pct*100:.0f}%: orig ${orig_premium:.0f} → ${current_value:.0f} profit ${profit:.0f}"
        changed = True
        closed.append({
            "summary": f"{ticker} {exp_str} {strike:.0f}P (${profit:.0f} profit)",
            "ticker": ticker, "account": (r.get("account") or INDIVIDUAL).strip().upper(),
            "expiry": exp_str, "strike": strike, "contracts": contracts,
            "ref_id": (r.get("id") or ""), "buyback": float(current_value), "profit": float(profit),
        })
        log.info("CSP TP closed: %s %s %.0fP — profit $%.0f", ticker, exp_str, strike, profit)

    if changed:
        write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)
    return {"closed": closed}


def scan_cc_take_profits(today: dt.date, regime: str = "BULL") -> Dict:
    ensure_positions_files()
    rows = load_csv_rows(CC_POSITIONS_FILE)
    closed = []; changed = False

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        exp_str = (r.get("expiry") or "").strip()
        try:
            if not exp_str or dt.date.fromisoformat(exp_str) <= today:
                continue
        except Exception:
            continue
        ticker       = (r.get("ticker") or "").strip().upper()
        strike       = safe_float(r.get("strike"), 0.0)
        contracts    = safe_int(r.get("contracts"), 0)
        orig_premium = safe_float(r.get("premium"), 0.0)
        if orig_premium <= 0 or strike <= 0 or contracts < 1 or not ticker:
            continue

        cc_tp_pct = float(regime_val(CC_TAKE_PROFIT_PCT, regime, 0.60))
        quote     = _fetch_option_quote(ticker, exp_str, strike, "call")
        if quote is None:
            continue
        bid, ask, mid = quote
        spread_pct = (ask - bid) / mid
        if spread_pct > float(CC_TP_MAX_SPREAD_PCT):
            log.info("CC TP %s %s: spread %.0f%% too wide", ticker, exp_str, spread_pct*100)
            continue
        current_value = mid * 100.0 * contracts
        if current_value > orig_premium * cc_tp_pct:
            continue

        profit = orig_premium - current_value
        r["status"] = "CLOSED_TP"; r["close_date"] = today.isoformat()
        r["close_type"] = "CLOSED_TAKE_PROFIT"
        r["notes"] = f"CC TP at {cc_tp_pct*100:.0f}%: orig ${orig_premium:.0f} → ${current_value:.0f} profit ${profit:.0f}"
        changed = True
        closed.append({
            "summary": f"{ticker} {exp_str} {strike:.0f}C (${profit:.0f} profit)",
            "ticker": ticker, "account": (r.get("account") or INDIVIDUAL).strip().upper(),
            "expiry": exp_str, "strike": strike, "contracts": contracts,
            "ref_id": (r.get("id") or ""), "source_lot_id": (r.get("source_lot_id") or ""),
            "buyback": float(current_value), "profit": float(profit),
        })
        log.info("CC TP closed: %s %s %.0fC — profit $%.0f", ticker, exp_str, strike, profit)

    if changed:
        write_csv_rows(CC_POSITIONS_FILE, rows, CC_POSITIONS_COLUMNS)
        try:
            import sys
            _whl = sys.modules.get("wheel")
            if not _whl:
                raise ImportError("wheel not in sys.modules")
            lots       = _whl._read_rows(_whl.WHEEL_LOTS_FILE)
            tp_lot_ids = {c["source_lot_id"] for c in closed if c.get("source_lot_id")}
            changed_l  = False
            for lot in lots:
                if (lot.get("lot_id") or "").strip() not in tp_lot_ids:
                    continue
                lot["has_open_cc"] = "0"; lot["cc_id"] = ""
                matching = next((c for c in closed
                                 if c.get("source_lot_id") == (lot.get("lot_id") or "").strip()), None)
                if matching:
                    orig_basis     = float(lot.get("cost_basis") or 0)
                    prev_collected = float(lot.get("cc_premium_collected") or 0)
                    new_collected  = prev_collected + float(matching.get("profit", 0))
                    lot["cc_premium_collected"] = f"{new_collected:.2f}"
                    lot["net_cost_basis"]       = f"{max(0.0, orig_basis - new_collected):.2f}"
                changed_l = True
            if changed_l:
                _whl._write_rows(_whl.WHEEL_LOTS_FILE, lots, _whl.LOT_FIELDS)
        except Exception as e:
            log.warning("scan_cc_take_profits: lot flag cleanup failed: %s", e)

    return {"closed": closed}


def scan_csp_roll_candidates(today: dt.date) -> List[dict]:
    ensure_positions_files()
    candidates = []
    for r in load_csv_rows(CSP_POSITIONS_FILE):
        if (r.get("status") or "").upper() != "OPEN":
            continue
        exp_str = (r.get("expiry") or "").strip()
        try:
            exp = dt.date.fromisoformat(exp_str)
        except Exception:
            continue
        if exp <= today:
            continue
        dte = (exp - today).days
        if dte <= int(CSP_ROLL_CANDIDATE_MIN_DTE):
            continue
        ticker    = (r.get("ticker") or "").strip().upper()
        strike    = safe_float(r.get("strike"), 0.0)
        contracts = safe_int(r.get("contracts"), 0)
        if not ticker or strike <= 0:
            continue
        current_price = get_live_price(ticker)
        if current_price is None or current_price >= strike * (1.0 - float(CSP_ROLL_CANDIDATE_ITM_PCT)):
            continue
        pct_itm = (strike - current_price) / strike * 100.0
        candidates.append({
            "ticker": ticker, "account": (r.get("account") or INDIVIDUAL).strip().upper(),
            "expiry": exp_str, "strike": strike, "contracts": contracts, "dte": dte,
            "pct_itm": round(pct_itm, 1), "current_price": round(current_price, 2),
            "orig_premium": safe_float(r.get("premium"), 0.0),
        })
    candidates.sort(key=lambda x: x["pct_itm"], reverse=True)
    return candidates


def process_csp_expirations(today: dt.date) -> Dict:
    ensure_positions_files()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    expired = []; assigned = []

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        exp_str = (r.get("expiry") or "").strip()
        try:
            exp = dt.date.fromisoformat(exp_str)
        except Exception as e:
            log.warning("process_csp_expirations: bad expiry %r: %s", exp_str, e); continue
        if exp > today:
            continue

        ticker    = (r.get("ticker") or "").strip().upper()
        strike    = float(r.get("strike") or 0.0)
        contracts = int(float(r.get("contracts") or 0.0))
        shares    = contracts * 100
        underlying_close = None

        try:
            cached_df = download_ohlcv(ticker)
            if not cached_df.empty:
                cached_df.index = pd.to_datetime(cached_df.index)
                exp_ts = pd.Timestamp(exp)
                exact  = cached_df[cached_df.index.normalize() == exp_ts]
                if not exact.empty:
                    underlying_close = float(exact["Close"].iloc[-1])
                else:
                    prior = cached_df[cached_df.index.normalize() < exp_ts]
                    if not prior.empty and prior.index[-1].date() >= (exp - dt.timedelta(days=5)):
                        underlying_close = float(prior["Close"].iloc[-1])
            if underlying_close is None:
                start = (exp - dt.timedelta(days=7)).isoformat()
                end   = (exp + dt.timedelta(days=2)).isoformat()
                df    = yf.download(ticker, start=start, end=end, interval="1d",
                                    auto_adjust=False, progress=False)
                df.dropna(inplace=True)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                if not df.empty:
                    df.index = pd.to_datetime(df.index)
                    exp_ts   = pd.Timestamp(exp)
                    exact    = df[df.index.normalize() == exp_ts]
                    if not exact.empty:
                        underlying_close = float(exact["Close"].iloc[-1])
                    else:
                        prior = df[df.index.normalize() < exp_ts]
                        if not prior.empty:
                            underlying_close = float(prior["Close"].iloc[-1])
        except Exception as e:
            log.warning("CSP expiry price fetch failed for %s %s: %s", ticker, exp_str, e)

        if underlying_close is None:
            log.warning("CSP expiry: could not determine close for %s %s — skipping", ticker, exp_str)
            continue

        r["underlying_close_at_expiry"] = f"{underlying_close:.2f}"
        r["close_date"]                 = exp.isoformat()
        if underlying_close >= strike:
            r["status"] = "EXPIRED"; r["close_type"] = "EXPIRED_OTM"
            expired.append(f"{ticker} {exp_str} {strike:.0f}P")
        else:
            r["status"] = "ASSIGNED"; r["close_type"] = "ASSIGNED_ITM"
            r["shares_if_assigned"] = str(shares)
            actual_prem = float(r.get("fill_premium") or r.get("premium") or r.get("est_premium") or 0.0)
            r["assignment_cost_basis"] = f"{(strike * shares - actual_prem):.2f}"
            assigned.append(f"{ticker} {exp_str} {strike:.0f}P -> {shares} sh")

    write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)
    return {"expired": expired, "assigned": assigned}


def scan_early_assignments(today: dt.date) -> Dict:
    ensure_positions_files()
    rows      = load_csv_rows(CSP_POSITIONS_FILE)
    threshold = float(CSP_EARLY_ASSIGN_ITM_PCT)
    warned = []; assigned = []; changed = False

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        exp_str = (r.get("expiry") or "").strip()
        try:
            exp = dt.date.fromisoformat(exp_str)
        except Exception:
            continue
        if exp <= today or (exp - today).days > CSP_EARLY_ASSIGN_MAX_DTE:
            continue
        ticker    = (r.get("ticker") or "").strip().upper()
        strike    = safe_float(r.get("strike"), 0.0)
        contracts = safe_int(r.get("contracts"), 0)
        if not ticker or strike <= 0 or contracts < 1:
            continue
        current_price = get_live_price(ticker)
        if current_price is None or current_price > strike * (1.0 - threshold):
            continue
        pct_itm = (strike - current_price) / strike * 100.0
        label   = f"{ticker} {exp_str} {strike:.0f}P ({pct_itm:.1f}% ITM, {(exp-today).days}d left, current {current_price:.2f})"
        if CSP_EARLY_ASSIGN_WARN_ONLY:
            warned.append(label); continue
        shares = contracts * 100
        actual_prem = float(r.get("fill_premium") or r.get("premium") or 0.0)
        r["status"] = "ASSIGNED"; r["close_type"] = "ASSIGNED_EARLY"
        r["close_date"] = today.isoformat()
        r["underlying_close_at_expiry"] = f"{current_price:.2f}"
        r["shares_if_assigned"] = str(shares)
        r["assignment_cost_basis"] = f"{(strike * shares - actual_prem):.2f}"
        assigned.append(label); changed = True

    if changed:
        write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)
    return {"warned": warned, "assigned": assigned}


# ============================================================
# CC planning from assigned CSPs (classic wheel)
# ============================================================

def decide_cc_strike(current_price, net_cost_basis_per_share, atr):
    if atr <= 0:
        return "SELL_CC", current_price * 1.02, "ATR unavailable; 2% OTM fallback"
    if net_cost_basis_per_share <= 0:
        return "SELL_CC", current_price + CC_ATR_MULT_NORMAL * atr, f"no basis; {CC_ATR_MULT_NORMAL}×ATR"
    pct = (current_price - net_cost_basis_per_share) / net_cost_basis_per_share
    mult, tier = (
        (CC_ATR_MULT_NORMAL, "NORMAL") if pct >= 0 else
        (CC_ATR_MULT_MILD,   "MILD")   if pct >= -float(CC_UNDERWATER_MILD_PCT) else
        (CC_ATR_MULT_DEEP,   "DEEP")   if pct >= -float(CC_UNDERWATER_DEEP_PCT) else
        (CC_ATR_MULT_SEVERE, "SEVERE")
    )
    target = current_price + mult * atr
    return "SELL_CC", target, (f"{tier} ({pct*100:+.1f}% vs basis {net_cost_basis_per_share:.2f}); "
                               f"{mult}×ATR ({atr:.2f}) → target {target:.2f}")


def _round_call_strike_to_chain(calls_df, target_strike):
    strikes = sorted([float(s) for s in calls_df["strike"].tolist()])
    above   = [s for s in strikes if s >= target_strike]
    return above[0] if above else strikes[-1]


def _cc_tier_for(current_price, net_cost_basis_per_share, atr):
    if net_cost_basis_per_share <= 0 or atr <= 0:
        return "NORMAL"
    pct = (current_price - net_cost_basis_per_share) / net_cost_basis_per_share
    return ("NORMAL" if pct >= 0 else
            "MILD"   if pct >= -float(CC_UNDERWATER_MILD_PCT) else
            "DEEP"   if pct >= -float(CC_UNDERWATER_DEEP_PCT) else "SEVERE")


def plan_covered_calls(today: dt.date, assigned_rows: List[dict], open_cc_lot_ids: set) -> List[dict]:
    ideas = []
    for pos in assigned_rows:
        ticker = (pos.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        lot_id = (pos.get("lot_id") or "").strip()
        if lot_id and lot_id in open_cc_lot_ids:
            continue
        try:
            shares = int(float(pos.get("shares_if_assigned") or 0))
        except Exception:
            shares = 0
        contracts = shares // 100
        if contracts < 1:
            continue
        try:
            net_basis = float(pos.get("net_cost_basis_per_share") or pos.get("strike") or 0.0)
        except Exception:
            net_basis = 0.0
        try:
            df            = add_indicators(download_ohlcv(ticker))
            if df.empty: continue
            atr           = float(df.iloc[-1].get("ATR_14") or 0)
            live          = get_live_price(ticker)
            current_price = live if live else float(df.iloc[-1]["Close"])
        except Exception as e:
            log.warning("plan_covered_calls: price fetch failed for %s: %s", ticker, e); continue

        _d, raw_target, cc_reason = decide_cc_strike(current_price, net_basis, atr)
        cc_tier = _cc_tier_for(current_price, net_basis, atr)
        dte_min, dte_max = CC_DTE_BY_TIER.get(cc_tier, (CC_TARGET_DTE_MIN, CC_TARGET_DTE_MAX))

        if CC_STRIKE_FLOOR_BELOW_CURRENT_PCT > 0:
            if raw_target < current_price * (1.0 - float(CC_STRIKE_FLOOR_BELOW_CURRENT_PCT)):
                continue

        try:
            t          = yf.Ticker(ticker)
            exp_str, _ = _pick_expiry_in_dte_range(t, dte_min, dte_max)
            if not exp_str:
                exp_str, _ = _pick_expiry_in_dte_range(t, CC_TARGET_DTE_MIN, CC_TARGET_DTE_MAX)
            if not exp_str: continue
            chain_key = f"{ticker}-{exp_str}"
            if chain_key not in _chain_cache:
                _chain_cache[chain_key] = t.option_chain(exp_str)
            calls  = _chain_cache[chain_key].calls.copy()
            if calls.empty: continue
            strike = _round_call_strike_to_chain(calls, raw_target)
            if CC_STRIKE_FLOOR_BELOW_CURRENT_PCT > 0:
                if strike < current_price * (1.0 - float(CC_STRIKE_FLOOR_BELOW_CURRENT_PCT)): continue
            row = calls.loc[calls["strike"] == strike].iloc[0]
            bid = float(row.get("bid", 0) or 0)
            ask = float(row.get("ask", 0) or 0)
            if bid < CC_MIN_BID or ask < bid: continue
            mid        = (bid + ask) / 2.0
            net_credit = bid + (mid - bid) * OPT_SELL_FILL_PCT - OPT_COMMISSION_PER_CONTRACT * contracts / 100.0
            ideas.append({
                "ticker": ticker, "expiry": exp_str, "strike": float(strike),
                "contracts": int(contracts), "credit_mid": float(net_credit), "cc_tier": cc_tier,
                "reason": f"[{cc_tier}] {cc_reason} | basis {net_basis:.2f}",
                "account": (pos.get("account") or INDIVIDUAL).strip().upper(),
                "source_lot_id": lot_id,
            })
        except Exception as e:
            log.warning("plan_covered_calls failed for %s: %s", ticker, e)

    return ideas


def execute_cc_roll(today: dt.date, cc_row: dict, roll_up: bool) -> Dict:
    ticker        = (cc_row.get("ticker") or "").strip().upper()
    old_exp       = (cc_row.get("expiry") or "").strip()
    old_strike    = safe_float(cc_row.get("strike"), 0.0)
    contracts     = safe_int(cc_row.get("contracts"), 1)
    account       = (cc_row.get("account") or INDIVIDUAL).strip().upper()
    source_lot_id = (cc_row.get("source_lot_id") or "").strip()
    FAIL = lambda msg: {"ok": False, "reason": msg, "net_credit": 0.0, "new_expiry": "", "new_strike": 0.0}

    try:
        df = add_indicators(download_ohlcv(ticker))
        if df is None or df.empty: return FAIL(f"No OHLCV data for {ticker}")
        atr           = safe_float(df.iloc[-1].get("ATR_14"), 0.0)
        live          = get_live_price(ticker)
        current_price = live if live else safe_float(df.iloc[-1].get("Close"), 0.0)
        if current_price <= 0: return FAIL(f"Could not get price for {ticker}")
    except Exception as e:
        return FAIL(f"Price fetch failed: {e}")

    try:
        import sys
        _whl = sys.modules.get("wheel")
        lots = _whl._read_rows(_whl.WHEEL_LOTS_FILE) if _whl else []
        parent_lot = next((l for l in lots if (l.get("lot_id") or "").strip() == source_lot_id), None) if source_lot_id else None
        net_basis_per_sh = 0.0
        if parent_lot:
            sh_lot = safe_int(parent_lot.get("shares"), 0)
            net_cb = safe_float(parent_lot.get("net_cost_basis") or parent_lot.get("cost_basis"), 0.0)
            net_basis_per_sh = (net_cb / sh_lot) if sh_lot > 0 else 0.0
    except Exception:
        net_basis_per_sh = 0.0

    cc_tier = _cc_tier_for(current_price, net_basis_per_sh, atr)
    dte_min, dte_max = CC_DTE_BY_TIER.get(cc_tier, (CC_TARGET_DTE_MIN, CC_TARGET_DTE_MAX))

    try:
        t = yf.Ticker(ticker)
        new_exp, _ = _pick_expiry_in_dte_range(t, dte_min, dte_max)
        if not new_exp: new_exp, _ = _pick_expiry_in_dte_range(t, CC_TARGET_DTE_MIN, CC_TARGET_DTE_MAX)
        if not new_exp: return FAIL("No expiry available")
        if new_exp <= old_exp: new_exp, _ = _pick_expiry_in_dte_range(t, dte_max, dte_max + 21)
        if not new_exp or new_exp <= old_exp: return FAIL(f"Could not find later expiry than {old_exp}")
    except Exception as e:
        return FAIL(f"Expiry fetch failed: {e}")

    try:
        chain_key_new = f"{ticker}-{new_exp}"
        if chain_key_new not in _chain_cache:
            _chain_cache[chain_key_new] = t.option_chain(new_exp)
        calls_new = _chain_cache[chain_key_new].calls.copy()
        if calls_new.empty: return FAIL(f"No call chain for {new_exp}")
        if roll_up:
            _, raw_target, _ = decide_cc_strike(current_price, net_basis_per_sh, atr)
            new_strike = _round_call_strike_to_chain(calls_new, raw_target)
            if new_strike <= current_price:
                new_strike = _round_call_strike_to_chain(calls_new, current_price * 1.01)
        else:
            new_strike = _round_call_strike_to_chain(calls_new, old_strike)
        if CC_STRIKE_FLOOR_BELOW_CURRENT_PCT > 0:
            floor = current_price * (1.0 - float(CC_STRIKE_FLOOR_BELOW_CURRENT_PCT))
            if new_strike < floor: return FAIL(f"Strike {new_strike:.2f} below OTM floor {floor:.2f}")
    except Exception as e:
        return FAIL(f"Strike selection failed: {e}")

    try:
        chain_key_old = f"{ticker}-{old_exp}"
        if chain_key_old not in _chain_cache:
            _chain_cache[chain_key_old] = t.option_chain(old_exp)
        calls_old = _chain_cache[chain_key_old].calls.copy()
        old_row   = calls_old.loc[calls_old["strike"] == old_strike]
        if old_row.empty: return FAIL("Old strike not found in chain")
        old_row = old_row.iloc[0]
        ask_old = safe_float(old_row.get("ask"), 0.0)
        if ask_old <= 0: return FAIL("Cannot get buyback quote")
        mid_old       = (safe_float(old_row.get("bid"), 0.0) + ask_old) / 2.0
        buyback_total = (ask_old - (ask_old - mid_old) * float(OPT_BUY_FILL_PCT)) * 100.0 * contracts
        new_row = calls_new.loc[calls_new["strike"] == new_strike]
        if new_row.empty: return FAIL("New strike not found in chain")
        new_row = new_row.iloc[0]
        bid_new = safe_float(new_row.get("bid"), 0.0)
        if bid_new < float(CC_MIN_BID): return FAIL(f"New CC bid {bid_new:.2f} below minimum")
        mid_new      = (bid_new + safe_float(new_row.get("ask"), 0.0)) / 2.0
        credit_per_sh = bid_new + (mid_new - bid_new) * float(OPT_SELL_FILL_PCT)
        credit_total  = credit_per_sh * 100.0 * contracts - float(OPT_COMMISSION_PER_CONTRACT) * contracts
    except Exception as e:
        return FAIL(f"Chain quote failed: {e}")

    net_credit = credit_total - buyback_total
    if net_credit < 0:
        return FAIL(f"Roll is a net DEBIT of ${abs(net_credit):.2f}. Debit rolls blocked.")

    try:
        rows   = load_csv_rows(CC_POSITIONS_FILE)
        old_id = (cc_row.get("id") or "").strip()
        for r in rows:
            if (r.get("id") or "").strip() == old_id:
                r["status"] = "CLOSED_ROLLED"; r["close_date"] = today.isoformat()
                r["close_type"] = "ROLLED"
                r["notes"] = (f"Rolled {'up ' if roll_up else ''}& out → {new_exp} {new_strike:.0f}C | "
                              f"buyback ${buyback_total:.0f}, credit ${credit_total:.0f}, net ${net_credit:.0f}")
                break
        write_csv_rows(CC_POSITIONS_FILE, rows, CC_POSITIONS_COLUMNS)

        new_cc_id = add_cc_position_from_candidate(today.isoformat(), {
            "ticker": ticker, "expiry": new_exp, "strike": float(new_strike),
            "contracts": contracts, "credit_mid": credit_per_sh, "cc_tier": cc_tier,
            "reason": f"[ROLL {'UP+OUT' if roll_up else 'OUT'}] → {new_exp} {new_strike:.0f}C | net ${net_credit:.0f}",
            "account": account, "source_lot_id": source_lot_id,
        })

        import sys
        _whl = sys.modules.get("wheel")
        if _whl and source_lot_id:
            lots = _whl._read_rows(_whl.WHEEL_LOTS_FILE)
            for lot in lots:
                if (lot.get("lot_id") or "").strip() == source_lot_id:
                    lot["cc_id"] = new_cc_id; lot["has_open_cc"] = "1"; break
            _whl._write_rows(_whl.WHEEL_LOTS_FILE, lots, _whl.LOT_FIELDS)

        return {
            "ok": True,
            "reason": (f"Rolled {ticker} {'up ' if roll_up else ''}& out: "
                       f"{old_exp} {old_strike:.0f}C → {new_exp} {new_strike:.0f}C | "
                       f"buyback ${buyback_total:.0f}  credit ${credit_total:.0f}  net ${net_credit:.0f}"),
            "net_credit": net_credit, "new_expiry": new_exp, "new_strike": new_strike,
        }
    except Exception as e:
        log.error("execute_cc_roll: write failed for %s: %s", ticker, e)
        return FAIL(f"Failed to write files: {e}")


def execute_cc_close_and_exit(today: dt.date, cc_row: dict) -> Dict:
    ticker        = (cc_row.get("ticker") or "").strip().upper()
    old_exp       = (cc_row.get("expiry") or "").strip()
    old_strike    = safe_float(cc_row.get("strike"), 0.0)
    contracts     = safe_int(cc_row.get("contracts"), 1)
    account       = (cc_row.get("account") or INDIVIDUAL).strip().upper()
    source_lot_id = (cc_row.get("source_lot_id") or "").strip()
    FAIL = lambda msg: {"ok": False, "reason": msg, "share_proceeds": 0.0, "cc_buyback": 0.0, "net_pnl": 0.0}

    live = get_live_price(ticker)
    if not live or live <= 0:
        return FAIL(f"Could not get current price for {ticker}")

    cc_buyback_total = 0.0
    try:
        quote = _fetch_option_quote(ticker, old_exp, old_strike, "call")
        if quote:
            bid_cc, ask_cc, mid_cc = quote
            cc_buyback_total = (ask_cc - (ask_cc - mid_cc) * float(OPT_BUY_FILL_PCT)) * 100.0 * contracts
    except Exception as e:
        log.warning("execute_cc_close_and_exit: CC quote failed for %s: %s", ticker, e)

    import sys
    _whl = sys.modules.get("wheel")
    if not _whl: return FAIL("wheel not in sys.modules")

    lots       = _whl._read_rows(_whl.WHEEL_LOTS_FILE)
    parent_lot = next((l for l in lots if (l.get("lot_id") or "").strip() == source_lot_id), None) if source_lot_id else None
    if not parent_lot: return FAIL(f"Could not find wheel lot {source_lot_id!r}")

    shares     = safe_int(parent_lot.get("shares"), 0)
    net_basis  = safe_float(parent_lot.get("net_cost_basis") or parent_lot.get("cost_basis"), 0.0)
    open_date  = (parent_lot.get("open_date") or "").strip()
    if shares <= 0: return FAIL("Lot has 0 shares")

    exit_price     = max(live - float(STOCK_SLIPPAGE_PER_SHARE), 0.01)
    share_proceeds = exit_price * shares
    net_pnl        = share_proceeds - cc_buyback_total - net_basis
    net_pnl_pct    = (net_pnl / net_basis * 100.0) if net_basis > 0 else 0.0
    entry_per_sh   = (net_basis / shares) if shares > 0 else 0.0

    try:
        cc_rows = load_csv_rows(CC_POSITIONS_FILE)
        old_id  = (cc_row.get("id") or "").strip()
        for r in cc_rows:
            if (r.get("id") or "").strip() == old_id:
                r["status"] = "CLOSED_MANUAL"; r["close_date"] = today.isoformat()
                r["close_type"] = "CLOSED_MANUAL_EXIT"
                r["notes"] = f"Manual exit: CC buyback ${cc_buyback_total:.0f}, shares @ ${exit_price:.2f}, P&L ${net_pnl:.0f}"
                break
        write_csv_rows(CC_POSITIONS_FILE, cc_rows, CC_POSITIONS_COLUMNS)

        for lot in lots:
            if (lot.get("lot_id") or "").strip() == source_lot_id:
                lot["status"] = "CLOSED"; lot["has_open_cc"] = "0"; lot["cc_id"] = ""; break
        _whl._write_rows(_whl.WHEEL_LOTS_FILE, lots, _whl.LOT_FIELDS)

        _whl._append_stock_trade_record({
            "id": f"{ticker}-{today.isoformat()}-MANUAL_EXIT", "account": account,
            "ticker": ticker, "entry_date": open_date, "entry_price": f"{entry_per_sh:.4f}",
            "shares": str(shares), "exit_date": today.isoformat(),
            "exit_price": f"{exit_price:.2f}", "reason": "MANUAL_EXIT",
            "close_type": "CC_MANUAL_EXIT", "pnl_abs": f"{net_pnl:.2f}",
            "pnl_pct": f"{net_pnl_pct:.2f}",
        })
        _whl.record_event(date=today.isoformat(), account=account, ticker=ticker,
                          event_type="CC_MANUAL_EXIT_BUYBACK", ref_id=old_id,
                          expiry=old_exp, strike=old_strike, contracts=contracts,
                          premium=-abs(cc_buyback_total))
        _whl.record_event(date=today.isoformat(), account=account, ticker=ticker,
                          event_type="CC_MANUAL_EXIT", ref_id=old_id,
                          expiry=old_exp, strike=old_strike, contracts=contracts, premium=0.0)

        log.info("CC manual exit — %s %d sh @ $%.2f, P&L $%.0f (%.1f%%)",
                 ticker, shares, exit_price, net_pnl, net_pnl_pct)
        return {
            "ok": True,
            "reason": (f"Exited {ticker}: {shares} sh @ ${exit_price:.2f} "
                       f"(proceeds ${share_proceeds:.0f}), CC buyback ${cc_buyback_total:.0f}, "
                       f"net P&L ${net_pnl:+.0f} ({net_pnl_pct:+.1f}%)"),
            "share_proceeds": share_proceeds, "cc_buyback": cc_buyback_total, "net_pnl": net_pnl,
        }
    except Exception as e:
        log.error("execute_cc_close_and_exit: write failed for %s: %s", ticker, e)
        return FAIL(f"Failed to write files: {e}")


def load_open_cc_tickers() -> set:
    ensure_positions_files()
    return {(r.get("ticker") or "").strip().upper()
            for r in load_csv_rows(CC_POSITIONS_FILE)
            if (r.get("status") or "").upper() == "OPEN"}


def load_open_cc_lot_ids() -> set:
    ensure_positions_files()
    return {(r.get("source_lot_id") or "").strip()
            for r in load_csv_rows(CC_POSITIONS_FILE)
            if (r.get("status") or "").upper() == "OPEN"
            and (r.get("source_lot_id") or "").strip()}


def make_cc_position_id(ticker, expiry, strike, open_date):
    return f"{ticker}-{expiry}-{float(strike):.2f}-{open_date}"


def add_cc_position_from_candidate(today: str, idea: dict) -> str:
    ensure_positions_files()
    rows  = load_csv_rows(CC_POSITIONS_FILE)
    cc_id = make_cc_position_id(idea["ticker"], idea["expiry"], idea["strike"], today)
    if any((r.get("id") or "") == cc_id for r in rows):
        return cc_id
    rows.append({
        "id": cc_id, "account": (idea.get("account") or INDIVIDUAL).strip().upper(),
        "open_date": today, "ticker": idea["ticker"], "expiry": idea["expiry"],
        "strike": f"{float(idea['strike']):.2f}", "contracts": str(int(idea["contracts"])),
        "premium": f"{float(idea['credit_mid'])*100.0*int(idea['contracts']):.2f}",
        "status": "OPEN", "close_date": "", "close_type": "",
        "source_lot_id": (idea.get("source_lot_id") or "").strip(),
        "notes": idea.get("reason", ""),
    })
    write_csv_rows(CC_POSITIONS_FILE, rows, CC_POSITIONS_COLUMNS)
    return cc_id
