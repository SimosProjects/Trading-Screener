"""screener_positions.py

Compute open equity holdings and account market values across all accounts.

These are pure calculations against loaded position data + prices —
no file writes, no decisions.  Results feed the display layer and
the trade-planning logic.
"""

from __future__ import annotations

import datetime as dt
from typing import Dict, List, Tuple

import strategies as strat
from utils import get_logger
from config import INDIVIDUAL, IRA, ROTH
from wheel import get_open_lots

log = get_logger(__name__)


def _f(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _i(x, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(default)


def build_holdings_and_mv(
    px: Dict[str, float],
) -> Tuple[List[dict], float, Dict[str, float]]:
    """
    Build the flat holdings list for display and compute per-account MV.

    Returns:
        holdings   — list of dicts for print_open_holdings
        wheel_mv   — total market value of open wheel lots (at current price)
        mv_stock   — {account: MV} for swing + retirement stock positions
    """
    stock_rows  = strat.load_stock_positions()
    ret_rows    = strat.load_retirement_positions()
    open_lots   = get_open_lots()

    holdings: List[dict] = []

    # --- Swing positions ---
    for r in stock_rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        acct  = (r.get("account") or "").strip().upper()
        tkr   = (r.get("ticker")  or "").strip().upper()
        sh    = _i(r.get("shares"), 0)
        entry = _f(r.get("entry_price"), 0.0)
        cur   = _f(px.get(tkr), _f(r.get("current_price"), 0.0))
        pnl   = (cur - entry) * sh if (entry > 0 and sh > 0 and cur > 0) else 0.0
        pnl_pct = ((cur - entry) / entry) if (entry > 0 and cur > 0) else 0.0
        holdings.append({
            "account": acct, "ticker": tkr, "shares": sh,
            "entry": entry, "cur": cur,
            "pnl": pnl, "pnl_pct": pnl_pct, "source": "SWING",
        })

    # --- Retirement long holds ---
    for r in ret_rows:
        acct  = (r.get("account") or "").strip().upper()
        tkr   = (r.get("ticker")  or "").strip().upper()
        sh    = _i(r.get("shares"), 0)
        entry = _f(r.get("entry_price"), 0.0)
        cur   = _f(px.get(tkr), _f(r.get("current_price"), 0.0))
        pnl   = (cur - entry) * sh if (entry > 0 and sh > 0 and cur > 0) else 0.0
        pnl_pct = ((cur - entry) / entry) if (entry > 0 and cur > 0) else 0.0
        holdings.append({
            "account": acct, "ticker": tkr, "shares": sh,
            "entry": entry, "cur": cur,
            "pnl": pnl, "pnl_pct": pnl_pct, "source": "RETIRE",
        })

    # --- Wheel assigned lots ---
    wheel_mv = 0.0
    for r in open_lots:
        tkr        = (r.get("ticker") or "").strip().upper()
        sh         = _i(r.get("shares"), 0)
        cost_basis = _f(r.get("cost_basis"), 0.0)
        entry      = (cost_basis / sh) if (sh > 0 and cost_basis > 0) else 0.0
        cur        = _f(px.get(tkr), 0.0)
        pnl        = (cur * sh - cost_basis) if (cur > 0 and sh > 0 and cost_basis > 0) else 0.0
        pnl_pct    = ((cur * sh - cost_basis) / cost_basis) if (cost_basis > 0 and cur > 0) else 0.0
        if sh > 0 and cur > 0:
            wheel_mv += cur * sh
        holdings.append({
            "account": INDIVIDUAL, "ticker": tkr, "shares": sh,
            "entry": entry, "cur": cur,
            "pnl": pnl, "pnl_pct": pnl_pct, "source": "WHEEL",
        })

    mv_stock = strat.stock_market_value_by_account(stock_rows, px)
    return holdings, wheel_mv, mv_stock


def compute_acct_mv(
    ret_by_key: dict,
    mv_stock: Dict[str, float],
    wheel_mv: float,
) -> Dict[str, float]:
    """
    Aggregate current market value per account.

    Used by the stock planning loop to enforce per-account position caps.
    Does NOT include CSP collateral (that's tracked separately in wheel exposure).
    """
    mv_ret = strat.retirement_market_value_by_account(ret_by_key)
    indiv_stock_mv = float(mv_stock.get(INDIVIDUAL, 0.0)) + float(wheel_mv)
    return {
        INDIVIDUAL: indiv_stock_mv,
        IRA:  float(mv_ret.get(IRA,  0.0)) + float(mv_stock.get(IRA,  0.0)),
        ROTH: float(mv_ret.get(ROTH, 0.0)) + float(mv_stock.get(ROTH, 0.0)),
    }


def collect_tickers_for_price_fetch(ret_by_key: dict) -> List[str]:
    """
    Build the full list of tickers we need current prices for.

    Called before build_holdings_and_mv so the cache (or fallback fetch)
    can be primed with the right universe.
    """
    stock_rows = strat.load_stock_positions()
    ret_rows   = strat.load_retirement_positions()
    open_lots  = get_open_lots()

    tickers: List[str] = []
    tickers += [(r.get("ticker") or "") for r in stock_rows if (r.get("status") or "").upper() == "OPEN"]
    tickers += [(r.get("ticker") or "") for r in ret_rows]
    tickers += [(r.get("ticker") or "") for r in open_lots]
    return [t.strip().upper() for t in tickers if t and str(t).strip()]
