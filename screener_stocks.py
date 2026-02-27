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
    RETIREMENT_STOCKS,
    RETIREMENT_MAX_STOCK_POSITIONS,
    RETIREMENT_STOP_LOSS_PCT,
    RETIREMENT_DIVERSIFY_SECTORS,
    CSP_TICKER_SECTOR,
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
    Plan and execute stock trades across all accounts.

    INDIVIDUAL: swing trades — up to 3 per run, any signal, full STOCKS universe.
    IRA/ROTH:   buy-and-hold — up to RETIREMENT_MAX_STOCK_POSITIONS open at once,
                pullback only, RETIREMENT_STOCKS universe, separate per-account counter.

    Modifies acct_mv in-place so each subsequent trade sees updated utilisation.
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

    # Count existing open positions per retirement account for the per-account cap.
    ret_open_count: Dict[str, int] = {
        IRA:  len(open_by_acct.get(IRA,  set())),
        ROTH: len(open_by_acct.get(ROTH, set())),
    }

    print("\n📈 STOCK ENTRIES (planned)")
    indiv_planned  = 0          # INDIVIDUAL cap: 3 new trades per run
    stock_opened:   List[str]  = []
    planned_stocks: List[dict] = []

    retirement_set = set(RETIREMENT_STOCKS)

    for e in entries:
        tkr  = e["ticker"]
        sig  = e["signal"]
        last = e["_last"]

        # Build the ordered list of accounts to try for this entry.
        acct_order: List[str] = []
        if trading_on and indiv_planned < 3:
            acct_order.append(INDIVIDUAL)
        if retire_on:
            # Only add retirement accounts that have room and where this ticker
            # is in the eligible universe.
            if tkr in retirement_set:
                for acct in (IRA, ROTH):
                    if ret_open_count.get(acct, 0) < RETIREMENT_MAX_STOCK_POSITIONS:
                        acct_order.append(acct)

        if not acct_order:
            continue

        picked_plan = None
        picked_acct = None

        for acct in acct_order:
            be_only = (
                f"{acct}:{tkr}" in ret_by_key
                and ret_by_key[f"{acct}:{tkr}"].get("flag_breakeven_only") == "1"
            )

            # Dedup logic per account type.
            if acct == INDIVIDUAL:
                existing_set = open_by_acct.get(INDIVIDUAL, set())
                if not ALLOW_DUPLICATE_TICKERS_ACROSS_ACCOUNTS:
                    existing_set = open_any
            else:
                # Retirement: block same ticker across both IRA and ROTH.
                existing_set = open_retirement if PREVENT_DUPLICATE_RETIREMENT_TICKERS else open_by_acct.get(acct, set())

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
            ret_open_count[picked_acct] = ret_open_count.get(picked_acct, 0) + 1

            # Soft cross-sector diversification check.
            # If we already hold one name in this account and the new name is in
            # the same sector, warn — but proceed. Human makes the final call.
            if RETIREMENT_DIVERSIFY_SECTORS and ret_open_count[picked_acct] >= 2:
                existing_sectors = {
                    CSP_TICKER_SECTOR.get(t.upper(), "OTHER")
                    for t in open_by_acct.get(picked_acct, set())
                    if t.upper() != tkr.upper()
                }
                new_sector = CSP_TICKER_SECTOR.get(tkr.upper(), "OTHER")
                if new_sector != "OTHER" and new_sector in existing_sectors:
                    print(
                        f"  ⚠️  SECTOR CONCENTRATION: [{picked_acct}] {tkr} ({new_sector}) joins "
                        f"another {new_sector} holding — consider diversifying across sectors."
                    )
        else:
            indiv_planned += 1

        acct_mv[picked_acct] = (
            float(acct_mv.get(picked_acct, 0.0))
            + float(picked_plan["entry_price"]) * int(picked_plan["shares"])
        )

        # Display differs by strategy type.
        if picked_acct in (IRA, ROTH):
            print(
                f"  {picked_acct:<10} {tkr:<6} BUY-HOLD   "
                f"Entry {picked_plan['entry_price']:.2f} | "
                f"Shares {picked_plan['shares']:<4} | "
                f"Stop {picked_plan['stop_price']:.2f} ({int(RETIREMENT_STOP_LOSS_PCT*100)}% below)"
            )
        else:
            risk = (picked_plan["entry_price"] - picked_plan["stop_price"]) * int(picked_plan["shares"])
            print(
                f"  {picked_acct:<10} {tkr:<6} {sig:<9} "
                f"Entry {picked_plan['entry_price']:.2f} | Shares {picked_plan['shares']:<5} "
                f"Stop {picked_plan['stop_price']:.2f} | Target {picked_plan['target_price']:.2f} | Risk ${risk:,.0f}"
            )

        planned_stocks.append({
            "ticker":      tkr,
            "signal":      sig,
            "account":     picked_acct,
            "entry_price": float(picked_plan["entry_price"]),
        })

    return stock_opened, planned_stocks


# ============================================================
# Watchlist print (used when no entries, or market is off)
# ============================================================

def print_watchlist(watch: List[dict]) -> None:
    print("\n📋 WATCHLIST")
    for w in watch[:20]:
        print(f"  {w['ticker']:<6} {w['note']:<13} Close {w['close']:.2f} | RSI2 {w['rsi2']:.1f}")
