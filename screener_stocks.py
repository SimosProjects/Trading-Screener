"""screener_stocks.py

Stock scan (pullback / breakout / EMA8 signals) and paper trade execution.
All printing stays in screener_display.py.

CHANGES vs original:
  - Added EMA8_PULLBACK signal (primary for MOMENTUM/STRONG_BULL regimes)
  - Added per-ticker rejection logging so you can see exactly why names fail
  - Added STOCK_MAX_OPEN_POSITIONS regime-dynamic cap on simultaneous trades
  - plan_and_execute_stocks respects the new signal type for retirement path
"""

from __future__ import annotations

import datetime as dt
from typing import Dict, List, Set, Tuple

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

PREVENT_DUPLICATE_RETIREMENT_TICKERS = True
ALLOW_DUPLICATE_TICKERS_ACROSS_ACCOUNTS = True


# ============================================================
# Signal scan
# ============================================================

def scan_stock_entries_and_watchlist(regime: str = "BULL") -> Tuple[List[dict], List[dict]]:
    """
    Scan STOCKS for entry signals and watchlist candidates.

    Three signal types (checked in priority order):
      1. EMA8_PULLBACK — primary in trending/momentum markets
      2. PULLBACK      — RSI(2) oversold near EMA21
      3. BREAKOUT      — new 20D high with volume

    Logs rejection reasons per ticker so you can see exactly what's failing.
    """
    entries: List[dict] = []
    watch:   List[dict] = []

    rejects: Dict[str, str] = {}   # ticker -> reason string for logging

    for tkr in STOCKS:
        try:
            df   = strat.add_indicators(strat.download_ohlcv(tkr))
            if df is None or df.empty:
                rejects[tkr] = "no data"
                continue
            last = df.iloc[-1]

            # ── Eligibility gate ────────────────────────────────────────────
            if not strat.is_eligible(last, regime=regime):
                try:
                    close = float(last["Close"])
                    sma50 = float(last["SMA_50"])
                    ema21 = float(last["EMA_21"])
                    adx   = float(last["ADX_14"])
                    if close <= sma50:
                        rejects[tkr] = f"close {close:.2f} <= SMA50 {sma50:.2f}"
                    elif ema21 <= sma50:
                        rejects[tkr] = f"EMA21 {ema21:.2f} <= SMA50 {sma50:.2f} (trend not stacked)"
                    else:
                        rejects[tkr] = f"ADX {adx:.1f} below floor"
                except Exception:
                    rejects[tkr] = "is_eligible failed"
                continue

            # ── Signal detection (priority order) ───────────────────────────
            ema8_pb = strat.ema8_pullback_signal(last, regime=regime)
            pb      = strat.pullback_signal(last, regime=regime)
            bo      = strat.breakout_signal(last, regime=regime)

            if ema8_pb or pb or bo:
                if ema8_pb:
                    signal = "EMA8_PULLBACK"
                elif pb:
                    signal = "PULLBACK"
                else:
                    signal = "BREAKOUT"

                entries.append({
                    "ticker": tkr,
                    "signal": signal,
                    "close":  float(last["Close"]),
                    "rsi2":   float(last["RSI_2"]),
                    "rsi14":  float(last.get("RSI_14", 50)),
                    "_last":  last,
                })
                continue

            # ── No signal: log why and add to watchlist if close ─────────────
            try:
                close  = float(last["Close"])
                ema8   = float(last.get("EMA_8", close))
                ema21  = float(last["EMA_21"])
                rsi2   = float(last["RSI_2"])
                rsi14  = float(last.get("RSI_14", 50))
                high20 = float(last["HIGH_20"])
                vol    = float(last["Volume"])
                vol_sma = float(last.get("VOL_SMA_10", 0) or 0)

                # Explain why no signal fired
                from config import (
                    STOCK_EMA8_BAND, STOCK_EMA8_PULLBACK_RSI14_MIN,
                    STOCK_EMA8_PULLBACK_RSI14_MAX, STOCK_PULLBACK_RSI2_MAX,
                    STOCK_PULLBACK_EMA_BAND, STOCK_BREAKOUT_VOL_MULT,
                )
                ema8_band  = float(strat.regime_val(STOCK_EMA8_BAND, regime, 0.018))
                rsi2_max   = float(strat.regime_val(STOCK_PULLBACK_RSI2_MAX, regime, 8.0))
                ema21_band = float(strat.regime_val(STOCK_PULLBACK_EMA_BAND, regime, 0.025))
                vol_mult   = float(strat.regime_val(STOCK_BREAKOUT_VOL_MULT, regime, 1.2))
                rsi14_min  = float(strat.regime_val(STOCK_EMA8_PULLBACK_RSI14_MIN, regime, 40.0))
                rsi14_max  = float(strat.regime_val(STOCK_EMA8_PULLBACK_RSI14_MAX, regime, 70.0))

                pct_from_ema8 = abs(close - ema8) / max(ema8, 1e-9)
                reasons = []
                if pct_from_ema8 > ema8_band:
                    reasons.append(f"EMA8: {pct_from_ema8*100:.1f}% away (need <{ema8_band*100:.1f}%)")
                if not (rsi14_min <= rsi14 <= rsi14_max):
                    reasons.append(f"RSI14={rsi14:.1f} (need {rsi14_min:.0f}-{rsi14_max:.0f} for EMA8)")
                if rsi2 >= rsi2_max:
                    reasons.append(f"RSI2={rsi2:.1f} (need <{rsi2_max:.1f} for pullback)")
                if close < high20:
                    vol_ok = vol_sma > 0 and vol >= vol_mult * vol_sma
                    if not vol_ok:
                        reasons.append(f"no breakout (below 20D high {high20:.2f}) + low vol")
                    else:
                        reasons.append(f"below 20D high {high20:.2f}")
                else:
                    vol_ok = vol_sma > 0 and vol >= vol_mult * vol_sma
                    if not vol_ok:
                        reasons.append(f"breakout vol too low ({vol:.0f} < {vol_mult:.1f}x {vol_sma:.0f})")

                rejects[tkr] = "; ".join(reasons) if reasons else "no signal fired"

                # Watchlist: near EMA8, near EMA21, near 20D high, or oversold
                near_ema8     = pct_from_ema8 <= ema8_band * 1.5
                near_ema21    = abs(close - ema21) / max(ema21, 1e-9) <= 0.015
                near_breakout = high20 > 0 and close / high20 >= 0.985
                oversold      = rsi2 <= 10

                if near_ema8 or near_ema21 or near_breakout or oversold:
                    if near_ema8:
                        note = "near EMA8"
                    elif near_ema21:
                        note = "near EMA21"
                    elif near_breakout:
                        note = "near 20D high"
                    else:
                        note = "RSI2 oversold"
                    watch.append({
                        "ticker": tkr,
                        "close":  close,
                        "rsi2":   rsi2,
                        "note":   note,
                    })
            except Exception as e:
                log.warning("stock watchlist eval failed for %s: %s", tkr, e)

        except Exception as e:
            log.warning("stock scan failed for %s: %s", tkr, e)
            continue

    # Log rejections at DEBUG (too noisy for daily terminal output).
    # Print a single summary line so you can see the scan worked.
    n_rejected = len(rejects)
    n_signals  = len(entries)
    n_watch    = len(watch)
    if rejects:
        for tkr, reason in sorted(rejects.items()):
            log.debug("  REJECT %s: %s", tkr, reason)
    log.info(
        "Stock scan: %d signals, %d watchlist, %d rejected (of %d tickers)",
        n_signals, n_watch, n_rejected, len(STOCKS),
    )

    entries = _dedupe(sorted(entries, key=lambda x: (
        # Priority: EMA8_PULLBACK first, then PULLBACK, then BREAKOUT
        {"EMA8_PULLBACK": 0, "PULLBACK": 1, "BREAKOUT": 2}.get(x["signal"], 3),
        x["rsi2"]
    )))
    watch = _dedupe(sorted(watch, key=lambda x: (x["note"], x["rsi2"])))
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
    regime: str = "BULL",
) -> Tuple[List[str], List[dict]]:
    """
    Plan and execute stock trades across all accounts.

    INDIVIDUAL: swing trades — no position count cap; every signal gets a plan.
                You decide which to take. Risk-based sizing keeps each trade safe.
    IRA/ROTH:   buy-and-hold — pullback/EMA8_PULLBACK only, RETIREMENT_STOCKS only.
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

    try:
        ret_pos_rows = strat.load_retirement_positions()
        for r in ret_pos_rows:
            acct = (r.get("account") or "").strip().upper()
            tkr  = (r.get("ticker")  or "").strip().upper()
            if acct in open_by_acct and tkr:
                open_by_acct[acct].add(tkr)
                open_retirement.add(tkr)
                open_any.add(tkr)
    except Exception as e:
        log.warning("plan_and_execute_stocks: could not load retirement positions: %s", e)

    ret_open_count: Dict[str, int] = {
        IRA:  len(open_by_acct.get(IRA,  set())),
        ROTH: len(open_by_acct.get(ROTH, set())),
    }

    print(f"\n📈 STOCK ENTRIES  (regime: {regime})")
    print(f"   {'Ticker':<6}  {'Signal':<14}  {'Entry':>7}  {'Stop':>13}  {'Target':>7}  "
          f"{'Shares':>6}  {'Value':>7}  {'Risk':>6}")
    print(f"   {'─'*6}  {'─'*14}  {'─'*7}  {'─'*13}  {'─'*7}  {'─'*6}  {'─'*7}  {'─'*6}")

    stock_opened:   List[str]  = []
    planned_stocks: List[dict] = []
    retirement_set = set(RETIREMENT_STOCKS)

    for e in entries:
        tkr  = e["ticker"]
        sig  = e["signal"]
        last = e["_last"]

        acct_order: List[str] = []
        if trading_on:
            acct_order.append(INDIVIDUAL)
        if retire_on:
            if tkr in retirement_set and sig in ("PULLBACK", "EMA8_PULLBACK"):
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

            if acct == INDIVIDUAL:
                existing_set = open_by_acct.get(INDIVIDUAL, set())
                if not ALLOW_DUPLICATE_TICKERS_ACROSS_ACCOUNTS:
                    existing_set = open_any
            else:
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
                regime=regime,
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

            if RETIREMENT_DIVERSIFY_SECTORS and ret_open_count[picked_acct] >= 2:
                existing_sectors = {
                    CSP_TICKER_SECTOR.get(t.upper(), "OTHER")
                    for t in open_by_acct.get(picked_acct, set())
                    if t.upper() != tkr.upper()
                }
                new_sector = CSP_TICKER_SECTOR.get(tkr.upper(), "OTHER")
                if new_sector != "OTHER" and new_sector in existing_sectors:
                    log.warning("Sector concentration: [%s] %s (%s) — consider diversifying.",
                                picked_acct, tkr, new_sector)
        else:
            acct_mv[picked_acct] = (
                float(acct_mv.get(picked_acct, 0.0))
                + float(picked_plan["entry_price"]) * int(picked_plan["shares"])
            )

        risk_dollars = (picked_plan["entry_price"] - picked_plan["stop_price"]) * int(picked_plan["shares"])
        pos_value    = picked_plan["entry_price"] * int(picked_plan["shares"])

        if picked_acct in (IRA, ROTH):
            print(
                f"   {tkr:<6}  {'BUY-HOLD':<14}  "
                f"{picked_plan['entry_price']:>7.2f}  "
                f"{picked_plan['stop_price']:>7.2f}  "
                f"{'—':>7}  "
                f"{picked_plan['shares']:>6}  "
                f"${pos_value:>6,.0f}  "
                f"{'35% stop'}"
            )
        else:
            upside_pct = (picked_plan['target_price'] - picked_plan['entry_price']) / picked_plan['entry_price'] * 100
            stop_type  = picked_plan.get("stop_type", "FIXED")
            trail_dist = picked_plan['entry_price'] - picked_plan['stop_price']
            # Compact stop label: "263.39 T-1.97" for trailing, "263.39" for fixed
            if stop_type == "TRAIL_EMA8":
                stop_str = f"{picked_plan['stop_price']:>7.2f} T-{trail_dist:.2f}"
            else:
                stop_str = f"{picked_plan['stop_price']:>7.2f}      "
            print(
                f"   {tkr:<6}  {sig:<14}  "
                f"{picked_plan['entry_price']:>7.2f}  "
                f"{stop_str}  "
                f"{picked_plan['target_price']:>7.2f}  "
                f"{picked_plan['shares']:>6}  "
                f"${pos_value:>6,.0f}  "
                f"${risk_dollars:>5,.0f}"
                f"  (+{upside_pct:.1f}% tgt)"
            )

        planned_stocks.append({
            "ticker":       tkr,
            "signal":       sig,
            "account":      picked_acct,
            "entry_price":  float(picked_plan["entry_price"]),
            "stop_price":   float(picked_plan["stop_price"]),
            "target_price": float(picked_plan["target_price"]),
            "shares":       int(picked_plan["shares"]),
            "pos_value":    float(pos_value),
            "risk_dollars": float(risk_dollars),
            "stop_type":    picked_plan.get("stop_type", "FIXED"),
        })

    return stock_opened, planned_stocks


# ============================================================
# Watchlist display
# ============================================================

def print_watchlist(watch: List[dict]) -> None:
    print("\n📋 WATCHLIST")
    if not watch:
        print("  (none)")
        return
    for w in watch[:20]:
        print(f"  {w['ticker']:<6} {w['note']:<14} Close {w['close']:.2f} | RSI2 {w['rsi2']:.1f}")
