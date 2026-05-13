"""market.py

Market regime detection and trading gates.
"""

from __future__ import annotations

import datetime as dt
from typing import Dict

import pandas as pd

import strategies as strat
from utils import get_logger
from config import (
    CSP_RISK_OFF_VIX,
    REGIME_VIX_MOMENTUM,
    REGIME_VIX_STRONG_BULL,
    REGIME_VIX_BULL,
    REGIME_VIX_NEUTRAL,
)

log = get_logger(__name__)


# ============================================================
# Market hours
# ============================================================

def is_market_hours() -> bool:
    """Return True if NYSE is currently open for regular trading.

    Uses Eastern Time (ET) regardless of where the script is run.
    Regular session: 9:30 AM – 4:00 PM ET, Monday–Friday.
    Does NOT account for market holidays — the GitHub Actions workflow
    already skips non-trading days via exchange_calendars. For local runs
    on holidays this will return True but the screener will gracefully
    handle stale/missing data.

    When False, the screener skips:
      - New stock trade scanning and entry
      - New CSP scanning and entry
      - New CC planning and entry
    Position maintenance (stop/target checks, trailing stop updates,
    expiration processing) always runs regardless of market hours.
    """
    try:
        import zoneinfo
        et = zoneinfo.ZoneInfo("America/New_York")
    except ImportError:
        # Python < 3.9 fallback — use UTC offset (ET = UTC-4 in summer, UTC-5 in winter)
        # This is approximate; install 'tzdata' for accuracy on older Python.
        import time
        utc_offset = -4 if time.daylight else -5
        et = dt.timezone(dt.timedelta(hours=utc_offset))

    now_et  = dt.datetime.now(tz=et)
    weekday = now_et.weekday()   # 0=Mon, 6=Sun

    if weekday >= 5:   # Saturday or Sunday
        return False

    open_time  = now_et.replace(hour=9,  minute=30, second=0, microsecond=0)
    close_time = now_et.replace(hour=16, minute=0,  second=0, microsecond=0)
    return open_time <= now_et <= close_time


# ============================================================
# Market data
# ============================================================

def fetch_market_context(cache=None) -> Dict:
    """Download SPY, QQQ, VIX and compute regime flags."""
    if cache is not None:
        strat.set_data_cache(cache)

    spy_df = strat.download_ohlcv("SPY")
    qqq_df = strat.download_ohlcv("QQQ")
    vix_df = strat.download_ohlcv("^VIX")
    return strat.market_context_from_dfs(spy_df, qqq_df, vix_df)


# ============================================================
# Trading gates
# ============================================================

def allow_swing_trades(mkt: Dict) -> bool:
    """Gate for INDIVIDUAL swing entries.

    Requires SPY above 200 AND 50 SMA, VIX < 25.
    EMA21 requirement REMOVED — it was too restrictive during recovery
    rallies (SPY above 200/50 but lagging on the 21 EMA) and blocked
    entries at exactly the best early-trend entry points.
    """
    return bool(
        mkt.get("spy_above_200")
        and mkt.get("spy_above_50")
        and mkt.get("vix_below_25")
    )


def allow_retirement_tactical(mkt: Dict) -> bool:
    """Softer gate for IRA/ROTH tactical stock trades."""
    return bool(
        mkt.get("spy_above_200")
        and mkt.get("vix_below_25")
    )


def market_regime(mkt: Dict) -> str:
    """
    Classify the current market into one of five regimes.

    MOMENTUM    : VIX < 16 AND SPY above all 3 MAs AND near/at 52W highs
                  High-octane bull — size up, chase breakouts, collect rich premiums.
    STRONG_BULL : VIX < 18 AND SPY above all 3 MAs
                  Calm confirmed uptrend — standard aggressive parameters.
    BULL        : VIX < 22 AND SPY above 200 + 50
                  Normal healthy market — standard parameters.
    NEUTRAL     : VIX < 25 AND SPY above 200
                  Elevated uncertainty — tighten slightly.
    RISK_OFF    : VIX >= 25 OR SPY below 200 SMA
                  Defensive mode — protect capital first.

    Defaults to RISK_OFF on missing data (fail-safe).
    """
    try:
        vix = float(mkt.get("vix_close") or 99.0)
    except Exception:
        vix = 99.0

    spy_above_200 = bool(mkt.get("spy_above_200"))
    spy_above_50  = bool(mkt.get("spy_above_50"))
    spy_above_21  = bool(mkt.get("spy_above_21"))
    spy_near_high = bool(mkt.get("spy_near_52w_high"))   # new flag

    if not spy_above_200 or vix >= float(REGIME_VIX_NEUTRAL):
        return "RISK_OFF"

    # MOMENTUM: low VIX, all MAs stacked, price near 52W high
    if (vix < float(REGIME_VIX_MOMENTUM)
            and spy_above_50
            and spy_above_21
            and spy_near_high):
        return "MOMENTUM"

    if vix < float(REGIME_VIX_STRONG_BULL) and spy_above_50 and spy_above_21:
        return "STRONG_BULL"

    if vix < float(REGIME_VIX_BULL) and spy_above_50:
        return "BULL"

    return "NEUTRAL"


def csp_mode(mkt: Dict) -> str:
    """Return 'NORMAL' or 'RISK_OFF' for the CSP engine gate."""
    reg = market_regime(mkt)
    return "RISK_OFF" if reg == "RISK_OFF" else "NORMAL"
