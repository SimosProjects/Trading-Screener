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


def csp_mode(mkt: Dict) -> str:
    """
    Return 'NORMAL', 'LOW_IV', or 'RISK_OFF' based on SPY trend + VIX level.

    LOW_IV  : VIX < 18 — premiums thin; tighter yield floors, AGGRESSIVE blocked.
    NORMAL  : VIX 18–25, SPY above 200 — standard full operation.
    RISK_OFF: VIX > 25 or SPY below 200 — defensive names only, farther OTM.
    Defaults to RISK_OFF when market data is unavailable (fail-safe).
    """
    try:
        vix = float(mkt.get("vix_close") or 99.0)
    except Exception:
        vix = 99.0

    spy_above_200 = bool(mkt.get("spy_above_200"))
    if (not spy_above_200) or (vix > float(CSP_RISK_OFF_VIX)):
        return "RISK_OFF"
    if vix < 18.0:
        return "LOW_IV"
    return "NORMAL"
