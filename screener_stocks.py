"""screener_stocks.py

Stock scan (pullback / breakout signals) and paper trade execution.

Extracted from screener.py.  Returns structured results;
all printing stays in screener_display.py.
"""

from __future__ import annotations

import datetime as dt
from typing import Dict, List, Optional, Set, Tuple

import strategies as strat
from utils import get_logger
from config import (
    STOCKS,
    INDIVIDUAL, IRA, ROTH,
)

log = get_logger(__name__)

# ── Behaviour toggles (edit here to change cross-account dedup rules) ──
# Prevents the same ticker appearing in both IRA and ROTH simultaneously.
PREVENT_DUPLICATE_RETIREMENT_TICKERS = True
# When False: also blocks the same ticker across INDIVIDUAL vs retirement.
ALLOW_DUPLICATE_TICKERS_ACROSS_ACCOUNTS = True


# ============================================================
# Signal scan
# ============================================================

def scan_stock_entries_and_watchlist() -> Tuple[List[dict], List[dict]]:
    """
    Scan STOCKS for pullback/breakout entry signals and watchlist candidates.

    Returns (entries, watchlist).  Each entry carries '_last' (the indicator
    row) for use by the planning step.
    """
    entries: List[dict] = []
    watch:   List[dict] = []

    for tkr in STOCKS:
        try:
            df   = strat.add_indicators(strat.download_ohlcv(tkr))
            last = df.iloc[-1]

            if not strat.is_eligible(last):
                continue

            pb = strat.pullback_signal(last)
            bo = strat.breakout_signal(last)

            if pb or bo:
                signal = "PULLBACK" if pb else "BREAKOUT"
                entries.append({
                    "ticker": tkr,
                    "signal": signal,
                    "close":  float(last["Close"]),
                    "rsi2":   float(last["RSI_2"]),
                    "_last":  last,
                })
                continue

            # Watchlist: near EMA21, near 20D high, or deeply oversold RSI(2).
            close  = float(last["Close"])
            ema21  = float(last["EMA_21"])
            rsi2   = float(last["RSI_2"])
            high20 = float(last["HIGH_20"])

            near_ema      = abs(close - ema21) / max(ema21, 1e-9) <= 0.012
            near_breakout = (high20 > 0) and (close / high20 >= 0.985)
            oversold      = rsi2 <= 10

            if near_ema or near_breakout or oversold:
                watch.append({
                    "ticker": tkr,
                    "close":  close,
                    "rsi2":   rsi2,
                    "note":   ("near EMA21" if near_ema
                               else ("near 20D high" if near_breakout
                                     else "RSI2 oversold")),
                })
        except Exception as e:
            log.warning("stock scan failed for %s: %s", tkr, e)
            continue

    entries = _dedupe(sorted(entries, key=lambda x: (x["signal"], x["rsi2"])))
    watch   = _dedupe(sorted(watch,   key=lambda x: (x["note"],   x["rsi2"])))
    return entries, watch


def _dedupe(rows: List[dict]) -> List[dict]:
    seen: Set[str] = set()
    out: List[dict] = []
    for r in rows:
        t = (r.get("ticker") or "").strip().upper()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(r)
    return out


# ============================================================
# Planning + execution loop
# ============================================================

def plan_and_execute_stocks(
    today: dt.date,
    entries: List[dict],
    mkt: Dict,
    trading_on: bool,
    retire_on: bool,
    acct_mv: Dict[str, float],
    ret_by_key: dict,
) -> Tuple[List[str], List[dict]]:
    """
    For each signal, attempt to plan a trade in INDIVIDUAL then IRA/ROTH.

    Returns:
        stock_opened   — list of "ACCT:TICKER" strings for post-run reporting
        planned_stocks — list of dicts for Discord alert / display

    Modifies acct_mv in-place as positions are opened so each subsequent
    trade sees the updated utilization.  The dict comes from the caller
    (screener) and is intentionally mutated here.
    """
    if not entries:
        return [], []

    stock_rows = strat.load_stock_positions()
    open_rows  = [r for r in stock_rows if (r.get("status") or "").upper() == "OPEN"]

    open_by_acct: Dict[str, Set[str]] = {INDIVIDUAL: set(), IRA: set(), ROTH: set()}
    for r in open_rows:
        acct = (r.get("account") or "").strip().upper()
        tkr  = (r.get("ticker")  or "").strip().upper()
        if acct in open_by_acct and tkr:
            open_by_acct[acct].add(tkr)

    open_any        = set().union(*open_by_acct.values())
    open_retirement = set().union(open_by_acct.get(IRA, set()), open_by_acct.get(ROTH, set()))

    print("\n📈 STOCK ENTRIES (planned)")
    planned        = 0
    stock_opened:  List[str]  = []
    planned_stocks: List[dict] = []

    for e in entries:
        if planned >= 3:
            break

        tkr  = e["ticker"]
        sig  = e["signal"]
        last = e["_last"]

        acct_order: List[str] = []
        if trading_on:
            acct_order.append(INDIVIDUAL)
        if retire_on:
            acct_order.extend([IRA, ROTH])

        picked_plan = None
        picked_acct = None

        for acct in acct_order:
            be_only = (
                f"{acct}:{tkr}" in ret_by_key
                and ret_by_key[f"{acct}:{tkr}"].get("flag_breakeven_only") == "1"
            )

            existing_set = open_by_acct.get(acct, set())
            if PREVENT_DUPLICATE_RETIREMENT_TICKERS and acct in (IRA, ROTH):
                existing_set = open_retirement
            elif not ALLOW_DUPLICATE_TICKERS_ACROSS_ACCOUNTS:
                existing_set = open_any

            plan = strat.plan_stock_trade(
                account=acct,
                ticker=tkr,
                signal=sig,
                last=last,
                mkt=mkt,
                existing_open_tickers=existing_set,
                acct_current_mv=float(acct_mv.get(acct, 0.0)),
                retirement_breakeven_only=be_only,
            )
            if not plan:
                continue
            picked_plan = plan
            picked_acct = acct
            break

        if not picked_plan or not picked_acct:
            continue

        strat.execute_stock_plan(today, picked_plan)
        stock_opened.append(f"{picked_acct}:{tkr}")
        open_by_acct[picked_acct].add(tkr)
        open_any.add(tkr)
        if picked_acct in (IRA, ROTH):
            open_retirement.add(tkr)

        # Update in-memory utilization so the next candidate sees current exposure.
        acct_mv[picked_acct] = (
            float(acct_mv.get(picked_acct, 0.0))
            + float(picked_plan["entry_price"]) * int(picked_plan["shares"])
        )

        risk = (picked_plan["entry_price"] - picked_plan["stop_price"]) * int(picked_plan["shares"])
        print(
            f"  {picked_acct:<5} {tkr:<6} {sig:<9} "
            f"Entry {picked_plan['entry_price']:.2f} | Shares {picked_plan['shares']:<5} "
            f"Stop {picked_plan['stop_price']:.2f} | Target {picked_plan['target_price']:.2f} | Risk ${risk:,.0f}"
        )

        planned_stocks.append({
            "ticker":      tkr,
            "signal":      sig,
            "account":     picked_acct,
            "entry_price": float(picked_plan["entry_price"]),
        })
        planned += 1

    return stock_opened, planned_stocks


# ============================================================
# Watchlist print (used when no entries, or market is off)
# ============================================================

def print_watchlist(watch: List[dict]) -> None:
    print("\n📋 WATCHLIST")
    for w in watch[:20]:
        print(f"  {w['ticker']:<6} {w['note']:<13} Close {w['close']:.2f} | RSI2 {w['rsi2']:.1f}")
