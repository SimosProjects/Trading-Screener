"""market.py

Market regime detection and trading gates.

Extracted from strategies.py (market_context*) and screener.py (allow_* / csp_mode).
Everything here is read-only logic against market data — no positions, no files.
"""

from __future__ import annotations

import datetime as dt
from typing import Dict

import pandas as pd

import strategies as strat
from utils import get_logger
from config import (
    CSP_RISK_OFF_VIX,
    REGIME_VIX_STRONG_BULL,
    REGIME_VIX_BULL,
    REGIME_VIX_NEUTRAL,
)

log = get_logger(__name__)


# ============================================================
# Market data
# ============================================================

def fetch_market_context(cache=None) -> Dict:
    """
    Download SPY, QQQ, VIX and compute regime flags.

    Passes the session cache through to strategies so those tickers
    are served from memory rather than hitting the network again.
    """
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
    """Full-quality gate for INDIVIDUAL swing entries."""
    return bool(
        mkt.get("spy_above_200")
        and mkt.get("spy_above_50")
        and mkt.get("spy_above_21")
        and mkt.get("qqq_above_50")
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
    Classify the current market environment into one of four regimes.
    This single classifier drives all dynamic parameters in the screener —
    OTM floors, sector caps, signal thresholds, take-profit levels, etc.

    STRONG_BULL : VIX < REGIME_VIX_STRONG_BULL (18) AND SPY above all 3 MAs
                  Low vol, confirmed uptrend — loosen filters, collect more premium.
    BULL        : VIX < REGIME_VIX_BULL (22) AND SPY above 200 + 50
                  Normal healthy market — standard parameters.
    NEUTRAL     : VIX < REGIME_VIX_NEUTRAL (25) AND SPY above 200
                  Elevated uncertainty — tighten slightly, more cushion.
    RISK_OFF    : VIX >= 25 OR SPY below 200 SMA
                  Defensive mode — wide OTM, defensive universe, fast TP.

    Defaults to RISK_OFF on missing data (fail-safe).
    """
    try:
        vix = float(mkt.get("vix_close") or 99.0)
    except Exception:
        vix = 99.0

    spy_above_200 = bool(mkt.get("spy_above_200"))
    spy_above_50  = bool(mkt.get("spy_above_50"))
    spy_above_21  = bool(mkt.get("spy_above_21"))

    if not spy_above_200 or vix >= float(REGIME_VIX_NEUTRAL):
        return "RISK_OFF"
    if vix < float(REGIME_VIX_STRONG_BULL) and spy_above_50 and spy_above_21:
        return "STRONG_BULL"
    if vix < float(REGIME_VIX_BULL) and spy_above_50:
        return "BULL"
    return "NEUTRAL"


def csp_mode(mkt: Dict) -> str:
    """
    Return 'NORMAL' or 'RISK_OFF' for the CSP engine gate.
    Full parameter tuning uses market_regime() — this just controls
    whether CSP scanning runs at all.
    """
    reg = market_regime(mkt)
    return "RISK_OFF" if reg == "RISK_OFF" else "NORMAL"
