"""screener_options.py

CSP candidate scanning and CC idea generation.

Extracted from screener.py.  Pure planning — no file writes here.
"""

from __future__ import annotations

import datetime as dt
from typing import Dict, List, Optional

import strategies as strat
from utils import get_logger
from config import (
    CSP_STOCKS,
    CSP_DEFENSIVE_STOCKS,
    CSP_RISK_OFF_VIX,
    CSP_RISK_OFF_MIN_OTM_PCT_DEFENSIVE,
    CSP_RISK_OFF_MIN_OTM_PCT_RISKY,
    CSP_NORMAL_MIN_OTM_PCT,
    CSP_STRIKE_BASE_NORMAL,
    CSP_STRIKE_BASE_RISK_OFF,
    CSP_ATR_MULTS,
)
from wheel import get_open_lots

log = get_logger(__name__)


# ============================================================
# CSP candidates
# ============================================================

def build_csp_candidates(mkt: Dict, mode: str) -> List[dict]:
    defensive_set = set(CSP_DEFENSIVE_STOCKS)

    if mode == "RISK_OFF":
        print(
            f"\n🛡️  CSP MODE: RISK_OFF | VIX {float(mkt.get('vix_close') or 0):.2f} > {float(CSP_RISK_OFF_VIX):.1f}"
            f" | Universe: DEFENSIVE ONLY ({len(defensive_set)} tickers)"
            f" | Strike base: {CSP_STRIKE_BASE_RISK_OFF} | Min OTM: {float(CSP_RISK_OFF_MIN_OTM_PCT_DEFENSIVE)*100:.0f}%"
        )
    else:
        print(
            f"\n🟦 CSP MODE: NORMAL | VIX {float(mkt.get('vix_close') or 0):.2f} <= {float(CSP_RISK_OFF_VIX):.1f}"
            f" | Universe: STANDARD ({len(CSP_STOCKS)} tickers)"
            f" | Strike base: {CSP_STRIKE_BASE_NORMAL} | Min OTM: {float(CSP_NORMAL_MIN_OTM_PCT)*100:.0f}%"
        )

    candidates: List[dict] = []

    for tkr in CSP_STOCKS:
        try:
            tkr_u = (tkr or "").strip().upper()
            if not tkr_u:
                continue

            defensive = tkr_u in defensive_set

            if mode == "RISK_OFF" and not defensive:
                continue

            df = strat.add_indicators(strat.download_ohlcv(tkr_u))
            if df is None or df.empty:
                continue
            last = df.iloc[-1]

            if mode == "NORMAL":
                if not strat.is_csp_eligible(last, allow_below_200=False):
                    continue
                min_otm  = float(CSP_NORMAL_MIN_OTM_PCT)
                base_ma  = str(CSP_STRIKE_BASE_NORMAL)
            else:
                if not strat.is_csp_eligible(last, allow_below_200=True):
                    continue
                min_otm = float(CSP_RISK_OFF_MIN_OTM_PCT_DEFENSIVE if defensive
                                else CSP_RISK_OFF_MIN_OTM_PCT_RISKY)
                base_ma = str(CSP_STRIKE_BASE_RISK_OFF)

            best       = None
            best_score = -1e9

            for atr_mult in CSP_ATR_MULTS:
                c = strat.evaluate_csp_candidate(
                    tkr_u, df, atr_mult=float(atr_mult),
                    risk_off=(mode == "RISK_OFF"),
                    min_otm_pct=min_otm,
                    base_ma=base_ma,
                )
                if not c:
                    continue

                close   = float(last.get("Close", 0) or 0)
                strike  = float(c.get("strike", 0) or 0)
                cushion = (close - strike) / close if close > 0 else 0.0

                # In RISK_OFF prioritise cushion over yield to reduce assignment risk.
                score = 0.0
                if mode == "RISK_OFF":
                    score += cushion * 100.0
                    score += float(c.get("yield_pct", 0.0)) * 50.0
                else:
                    score += float(c.get("yield_pct", 0.0)) * 100.0
                    score += cushion * 20.0
                score += float(c.get("est_premium", 0.0)) / 250.0

                if score > best_score:
                    best       = c
                    best_score = score

            if best:
                best["mode"]      = mode
                best["defensive"] = bool(defensive)
                candidates.append(best)

        except Exception as e:
            log.warning("CSP candidate eval failed for %s: %s", tkr, e)
            continue

    return candidates


# ============================================================
# CC ideas from open lots
# ============================================================

def plan_ccs_from_open_lots() -> List[dict]:
    """
    Generate CC candidates only for OPEN lots with no active CC.

    Does not open a CC on any ticker that already has one in cc_positions.csv.
    """
    ideas: List[dict] = []
    lots = get_open_lots()
    if not lots:
        return ideas

    open_cc_tickers = strat.load_open_cc_tickers()

    assigned_rows = []
    for lot in lots:
        if (lot.get("has_open_cc") or "").strip().lower() in ("1", "true"):
            continue
        tkr = (lot.get("ticker") or "").strip().upper()
        if tkr in open_cc_tickers:
            continue
        assigned_rows.append({
            "ticker":           tkr,
            "shares_if_assigned": lot.get("shares") or "100",
            "strike":           lot.get("assigned_strike") or "",
        })

    if not assigned_rows:
        return ideas

    try:
        ideas = strat.plan_covered_calls(dt.date.today(), assigned_rows, open_cc_tickers)
    except Exception as e:
        log.warning("plan_covered_calls failed: %s", e)
        ideas = []

    return ideas
