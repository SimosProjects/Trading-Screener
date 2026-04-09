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
    CSP_EXCLUDED_TICKERS,
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
    elif mode == "LOW_IV":
        print(
            f"\n🟡 CSP MODE: LOW_IV | VIX {float(mkt.get('vix_close') or 0):.2f} < 18"
            f" | Universe: STANDARD ({len(CSP_STOCKS)} tickers)"
            f" | Tighter yield floors | AGGRESSIVE blocked"
            f" | Strike base: {CSP_STRIKE_BASE_NORMAL} | Min OTM: {float(CSP_NORMAL_MIN_OTM_PCT)*100:.0f}%"
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

            # Excluded tickers: growth/speculative names kept in STOCKS for swing
            # trades but explicitly barred from the Wheel universe.
            if tkr_u in CSP_EXCLUDED_TICKERS:
                continue

            defensive = tkr_u in defensive_set

            if mode == "RISK_OFF" and not defensive:
                continue

            df = strat.add_indicators(strat.download_ohlcv(tkr_u))
            if df is None or df.empty:
                continue
            last = df.iloc[-1]

            if mode in ("NORMAL", "LOW_IV"):
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

            # Per-ticker guards — run once here, not inside each ATR iteration.
            # Ex-dividend: early assignment risk before ex-date.
            if strat.has_upcoming_ex_dividend(tkr_u):
                continue

            # Earnings: IV crush + gap risk if announcement falls within CSP window.
            # Need an expiry estimate — use the first valid expiry in range for the check.
            # evaluate_csp_candidate will select the exact expiry; this is just the guard.
            try:
                import yfinance as yf
                _t = yf.Ticker(tkr_u)
                _exp, _ = strat._pick_expiry_in_dte_range(_t, strat.CSP_TARGET_DTE_MIN, strat.CSP_TARGET_DTE_MAX)
                if _exp and strat.has_earnings_within_window(tkr_u, _exp):
                    continue
            except Exception:
                pass  # fail-open — let evaluate_csp_candidate proceed

            for atr_mult in CSP_ATR_MULTS:
                c = strat.evaluate_csp_candidate(
                    tkr_u, df, atr_mult=float(atr_mult),
                    risk_off=(mode == "RISK_OFF"),
                    min_otm_pct=min_otm,
                    base_ma=base_ma,
                )
                # Note: available_capital defaults to CSP_MAX_CASH_PER_TRADE inside
                # evaluate_csp_candidate — sufficient to admit 1-contract candidates
                # for any priced stock.  Actual per-account sizing happens in
                # plan_weekly_csp_orders which knows the real remaining capital.
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

def _net_basis_per_share(lot: dict) -> str:
    """
    Return the effective per-share cost basis for a lot as a string.

    Priority:
      1. net_cost_basis  — already reduced by accumulated CC premiums (Step 3+)
      2. cost_basis      — raw assignment basis (pre-Step 3 lots)
      3. assigned_strike — last resort if neither dollar figure is present
    """
    shares = max(int(float(lot.get("shares") or 100)), 1)

    net = lot.get("net_cost_basis") or ""
    if net and net not in ("", "0", "0.0", "0.00"):
        try:
            return f"{float(net) / shares:.4f}"
        except Exception:
            pass

    raw = lot.get("cost_basis") or ""
    if raw and raw not in ("", "0", "0.0", "0.00"):
        try:
            return f"{float(raw) / shares:.4f}"
        except Exception:
            pass

    strike = lot.get("assigned_strike") or ""
    if strike:
        try:
            return f"{float(strike):.4f}"
        except Exception:
            pass

    return ""


def plan_ccs_from_open_lots() -> List[dict]:
    """
    Generate CC candidates only for OPEN lots with no active CC.

    Each lot is checked independently — two lots for the same ticker (two
    separate CSP assignment cycles) are both eligible and each gets its own
    CC idea, keyed by lot_id rather than ticker so the linking step can attach
    the right CC to the right lot.
    """
    ideas: List[dict] = []
    lots = get_open_lots()
    if not lots:
        return ideas

    open_cc_lot_ids = strat.load_open_cc_lot_ids()

    assigned_rows = []
    for lot in lots:
        if (lot.get("has_open_cc") or "").strip().lower() in ("1", "true"):
            continue
        lot_id = (lot.get("lot_id") or "").strip()
        # Skip this lot if it already has an open CC recorded against it.
        if lot_id and lot_id in open_cc_lot_ids:
            continue
        tkr = (lot.get("ticker") or "").strip().upper()
        if not tkr:
            continue
        assigned_rows.append({
            "ticker":                  tkr,
            "lot_id":                  lot_id,           # carries through to CC record
            "shares_if_assigned":      lot.get("shares") or "100",
            "strike":                  lot.get("assigned_strike") or "",
            # Prefer net_cost_basis (cost_basis reduced by all CC premiums collected so far).
            # Fall back to raw cost_basis for lots created before Step 3, then to assigned_strike.
            # Divide by shares to get per-share figure for decide_cc_strike.
            "net_cost_basis_per_share": _net_basis_per_share(lot),
            # Carry the lot's account through so plan_covered_calls can tag each idea.
            "account": (lot.get("account") or "INDIVIDUAL").strip().upper(),
        })

    if not assigned_rows:
        return ideas

    try:
        ideas = strat.plan_covered_calls(dt.date.today(), assigned_rows, open_cc_lot_ids)
    except Exception as e:
        log.warning("plan_covered_calls failed: %s", e)
        ideas = []

    return ideas
