# screener.py
import datetime as dt
from typing import Dict, List, Tuple

from config import (
    WEBHOOK_URL,
    STOCKS,
    CSP_STOCKS,
    ENABLE_CSP,
    INDIVIDUAL, IRA, ROTH,
    ACCOUNT_SIZES,
    INDIVIDUAL_STOCK_CAP,
    CSP_LEDGER_FILE,
)

import strategies as strat
from wheel import (
    ensure_wheel_files,
    compute_wheel_exposure,
    compute_week_remaining,
    record_event,
    create_lots_from_new_assignments,
    link_new_ccs_to_lots,
    process_cc_expirations,
    rebuild_monthly_from_events,
    get_open_lots,
    should_backfill_events,
    backfill_open_events_from_positions,
)

# ============================================================
# Discord (alerts only; avoid intimate account details)
# ============================================================

def send_discord(msg: str) -> None:
    if not WEBHOOK_URL or not WEBHOOK_URL.strip():
        print("Discord webhook not set (WEBHOOK_URL). Skipping Discord.")
        return

    url = WEBHOOK_URL.strip()
    if not url.startswith("http"):
        print("Discord webhook invalid (WEBHOOK_URL). Skipping Discord.")
        return

    try:
        import requests
        requests.post(url, json={"content": msg[:1900]}, timeout=10)
    except Exception as e:
        print(f"Discord error: {e}")


# ============================================================
# Market filter + watchlist / entries
# ============================================================
def allow_swing_trades(mkt: Dict) -> bool:
    """Strict gate for INDIVIDUAL swing trades."""
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

def allow_conservative_premium(mkt: Dict) -> bool:
    """Conservative income strategies (CSPs)."""
    return bool(mkt.get("spy_above_200") and mkt.get("vix_below_25"))

def scan_stock_entries_and_watchlist() -> Tuple[List[dict], List[dict]]:
    """
    Scans STOCKS for:
      - entries: pullback/breakout signals
      - watchlist: technically healthy, close to signal
    """
    entries: List[dict] = []
    watch: List[dict] = []

    for tkr in STOCKS:
        try:
            df = strat.add_indicators(strat.download_ohlcv(tkr))
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
                    "close": float(last["Close"]),
                    "ema21": float(last["EMA_21"]),
                    "sma50": float(last["SMA_50"]),
                    "sma200": float(last["SMA_200"]),
                    "rsi2": float(last["RSI_2"]),
                    "atr": float(last["ATR_14"]),
                    "high20": float(last["HIGH_20"]),
                    "_last": last,  # keep row for planning
                })
                continue

            # Watchlist heuristics (cheap & stable)
            close = float(last["Close"])
            ema21 = float(last["EMA_21"])
            rsi2 = float(last["RSI_2"])
            high20 = float(last["HIGH_20"])

            near_ema = abs(close - ema21) / max(ema21, 1e-9) <= 0.012  # within ~1.2%
            near_breakout = (high20 > 0) and (close / high20 >= 0.985)  # within ~1.5% of 20d high
            oversold = rsi2 <= 10

            if near_ema or near_breakout or oversold:
                watch.append({
                    "ticker": tkr,
                    "close": close,
                    "ema21": ema21,
                    "high20": high20,
                    "rsi2": rsi2,
                    "note": "near EMA21" if near_ema else ("near 20D high" if near_breakout else "RSI2 oversold"),
                })
        except Exception:
            continue

    # prioritize entries, then watchlist
    entries = sorted(entries, key=lambda x: (x["signal"], x["rsi2"]))  # pullbacks first (more oversold)
    watch = sorted(watch, key=lambda x: (x["note"], x["rsi2"]))
    return entries, watch


# ============================================================
# CSP + CC planning (signals from strategies; exposure from wheel)
# ============================================================

def build_csp_candidates() -> List[dict]:
    candidates: List[dict] = []
    for tkr in CSP_STOCKS:
        try:
            df = strat.add_indicators(strat.download_ohlcv(tkr))
            last = df.iloc[-1]
            if not strat.is_eligible(last):
                continue
            c = strat.evaluate_csp_candidate(tkr, df)
            if c:
                candidates.append(c)
        except Exception:
            continue
    return candidates


def plan_ccs_from_open_lots() -> List[dict]:
    """
    CC ideas ONLY come from OPEN lots with no attached CC.
    Uses strategy helper 'decide_cc_strike' and option chain selection from strategies.
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
            "ticker": tkr,
            "shares_if_assigned": lot.get("shares") or "100",
            "strike": lot.get("assigned_strike") or "",
        })

    if not assigned_rows:
        return ideas

    try:
        ideas = strat.plan_covered_calls(dt.date.today(), assigned_rows, open_cc_tickers)
    except Exception:
        ideas = []
    return ideas


# ============================================================
# Formatting
# ============================================================

def print_market_context(mkt: Dict, trading_on: bool, retire_on: bool) -> None:
    print("\n==============================")
    print(f"📅 RUN DATE: {dt.date.today().isoformat()}")
    print("==============================\n")

    print("📊 MARKET CONTEXT")
    print("SPY:")
    print(f"  Close: {mkt['spy_close']:.2f}")
    print(f"  Above 200 SMA: {mkt['spy_above_200']}")
    print(f"  Above 50 SMA:  {mkt['spy_above_50']}")
    print(f"  Above 21 EMA:  {mkt['spy_above_21']}\n")

    print("QQQ:")
    print(f"  Close: {mkt['qqq_close']:.2f}")
    print(f"  Above 50 SMA: {mkt['qqq_above_50']}\n")

    print("VIX:")
    print(f"  Close: {mkt['vix_close']:.2f}")
    print(f"  < 18 (low): {mkt['vix_below_18']}")
    print(f"  < 25 (ok):  {mkt['vix_below_25']}\n")

    print(("🟢 INDIVIDUAL Trading ON" if trading_on else "🔴 INDIVIDUAL Trading OFF") +
          (" | 🟢 Retirement tactical ON" if retire_on else " | 🔴 Retirement tactical OFF"))


def build_discord_alert(
    mkt: Dict,
    trading_on: bool,
    new_csps: List[dict],
    new_ccs: List[dict],
    entries: List[dict],
    watch: List[dict],
    csp_exp: List[str],
    csp_asn: List[str],
    cc_exp: List[str],
    cc_call: List[str],
    stock_opens: List[str],
    stock_closes: List[str],
) -> str:
    """
    Keep it subscriber-safe:
    - include market conditions, entries/watchlist, and maintenance outcomes
    - exclude account sizes / caps / exposures
    """
    lines = []
    lines.append(f"📅 {dt.date.today().isoformat()} Screener")
    lines.append(f"Market: {'ON' if trading_on else 'OFF'} | SPY {mkt['spy_close']:.2f} | QQQ {mkt['qqq_close']:.2f} | VIX {mkt['vix_close']:.2f}")

    if csp_exp or csp_asn or cc_exp or cc_call or stock_opens or stock_closes:
        lines.append("— Maintenance —")
        if csp_exp:  lines.append(f"CSP expired: {', '.join(csp_exp[:8])}{'…' if len(csp_exp)>8 else ''}")
        if csp_asn:  lines.append(f"CSP assigned: {', '.join(csp_asn[:8])}{'…' if len(csp_asn)>8 else ''}")
        if cc_exp:   lines.append(f"CC expired: {', '.join(cc_exp[:8])}{'…' if len(cc_exp)>8 else ''}")
        if cc_call:  lines.append(f"Called away: {', '.join(cc_call[:8])}{'…' if len(cc_call)>8 else ''}")
        if stock_closes: lines.append(f"Stocks closed: {', '.join(stock_closes[:10])}{'…' if len(stock_closes)>10 else ''}")
        if stock_opens:  lines.append(f"Stocks opened: {', '.join(stock_opens[:10])}{'…' if len(stock_opens)>10 else ''}")

    if new_csps:
        lines.append("— New CSP ideas —")
        for x in new_csps[:10]:
            lines.append(f"{x['ticker']} {x['strike']:.0f}P {x['expiry']} ~${x['est_premium']:.0f}")

    if new_ccs:
        lines.append("— New CC ideas —")
        for x in new_ccs[:10]:
            lines.append(f"{x['ticker']} {x['strike']:.0f}C {x['expiry']} ~${x['credit_mid']*100:.0f}")

    if entries:
        lines.append("— Stock entries —")
        for e in entries[:10]:
            lines.append(f"{e['ticker']} {e['signal']} @ {e['close']:.2f}")

    if watch and not entries:
        lines.append("— Watchlist —")
        for w in watch[:12]:
            lines.append(f"{w['ticker']} ({w['note']}) @ {w['close']:.2f}")

    return "\n".join(lines)


# ============================================================
# MAIN
# ============================================================

def run_screener() -> None:
    today = dt.date.today()

    # --- Market context (always) ---
    mkt = strat.market_context(today)
    trading_on = allow_swing_trades(mkt)
    retire_on = allow_retirement_tactical(mkt)
    print_market_context(mkt, trading_on, retire_on)

    # --- Ensure files + maintenance ALWAYS ---
    ensure_wheel_files()
    strat.ensure_positions_files()
    strat.ensure_stock_files()
    strat.ensure_retirement_file()

    # Retirement marks (MV + breakeven-only flags)
    ret_by_key, ret_flagged = strat.update_retirement_marks()

    # ============================================================
    # Print OPEN equity holdings (stocks only) across all accounts
    # Excludes CSP/CC option positions; includes assigned Wheel lots
    # ============================================================

    # 1) Swing stock positions (OPEN)
    stock_rows = strat.load_stock_positions()
    open_swing = [r for r in stock_rows if (r.get("status") or "").upper() == "OPEN"]

    # 2) Retirement inventory (all rows are effectively "open holdings")
    ret_rows = strat.load_retirement_positions()

    # 3) Wheel assigned lots (OPEN lots => stock exposure)
    open_lots = get_open_lots()  # from wheel import get_open_lots

    # Build ticker universe for pricing
    tickers = []
    tickers += [r.get("ticker") for r in open_swing]
    tickers += [r.get("ticker") for r in ret_rows]
    tickers += [r.get("ticker") for r in open_lots]

    px = strat.last_close_prices(tickers)

    def _f(x, default=0.0) -> float:
        try:
            return float(x)
        except Exception:
            return float(default)

    def _i(x, default=0) -> int:
        try:
            return int(float(x))
        except Exception:
            return int(default)

    holdings = []

    # Swing holdings
    for r in open_swing:
        acct = (r.get("account") or "").strip().upper()
        tkr = (r.get("ticker") or "").strip().upper()
        sh  = _i(r.get("shares"), 0)
        entry = _f(r.get("entry_price"), 0.0)
        cur = _f(px.get(tkr), _f(r.get("current_price"), 0.0))
        pnl = (cur - entry) * sh if (entry > 0 and sh > 0 and cur > 0) else 0.0
        pnl_pct = ((cur - entry) / entry) if (entry > 0 and cur > 0) else 0.0
        holdings.append({
            "account": acct,
            "ticker": tkr,
            "shares": sh,
            "entry": entry,
            "cur": cur,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "source": "SWING",
        })

    # Retirement holdings
    for r in ret_rows:
        acct = (r.get("account") or "").strip().upper()
        tkr = (r.get("ticker") or "").strip().upper()
        sh  = _i(r.get("shares"), 0)
        entry = _f(r.get("entry_price"), 0.0)
        cur = _f(px.get(tkr), _f(r.get("current_price"), 0.0))
        pnl = (cur - entry) * sh if (entry > 0 and sh > 0 and cur > 0) else 0.0
        pnl_pct = ((cur - entry) / entry) if (entry > 0 and cur > 0) else 0.0
        holdings.append({
            "account": acct,
            "ticker": tkr,
            "shares": sh,
            "entry": entry,
            "cur": cur,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "source": "RETIRE",
        })

    # Wheel lots (assigned stock exposure)
    for r in open_lots:
        acct = INDIVIDUAL  # wheel is only in INDIVIDUAL per config
        tkr = (r.get("ticker") or "").strip().upper()
        sh  = _i(r.get("shares"), 0)
        # cost_basis in your lots is TOTAL dollars (strike*shares - premium)
        cost_basis = _f(r.get("cost_basis"), 0.0)
        entry = (cost_basis / sh) if (sh > 0 and cost_basis > 0) else 0.0
        cur = _f(px.get(tkr), 0.0)
        pnl = (cur * sh - cost_basis) if (cur > 0 and sh > 0 and cost_basis > 0) else 0.0
        pnl_pct = ((cur * sh - cost_basis) / cost_basis) if (cost_basis > 0 and cur > 0) else 0.0
        holdings.append({
            "account": acct,
            "ticker": tkr,
            "shares": sh,
            "entry": entry,
            "cur": cur,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "source": "WHEEL",
        })

    # Print
    if holdings:
        print("\n📌 OPEN STOCK HOLDINGS (all accounts) — Unrealized P/L")
        print("    (Excludes CSP/CC options; includes assigned Wheel lots)\n")

        holdings.sort(key=lambda x: (x["account"], x["ticker"]))

        by_acct = {}
        for h in holdings:
            by_acct.setdefault(h["account"], []).append(h)

        for acct, rows in by_acct.items():
            acct_pnl = sum(r["pnl"] for r in rows)
            acct_mv = sum((r["cur"] * r["shares"]) for r in rows if r["cur"] > 0)
            print(f"  {acct}  |  MV ${acct_mv:,.0f}  |  P/L ${acct_pnl:,.0f}")
            for r in rows:
                print(
                    f"    {r['ticker']:<6} "
                    f"{r['shares']:>5} sh  "
                    f"Entry {r['entry']:>8.2f}  "
                    f"Now {r['cur']:>8.2f}  "
                    f"P/L {r['pnl']:>10.0f}  "
                    f"({r['pnl_pct']*100:>6.1f}%)  "
                    f"[{r['source']}]"
                )
            print("")
    else:
        print("\n📌 OPEN STOCK HOLDINGS: none\n")

    # Close any stock swing positions first (stop/target hits, paper)
    closes = strat.update_and_close_stock_positions(today, mkt)
    stock_closed = closes.get("stops", []) + closes.get("targets", [])

    # Wheel maintenance (always)
    csp_out = strat.process_csp_expirations(today)  # updates csp_positions.csv
    cc_out = process_cc_expirations(today)          # updates cc_positions.csv + lots

    create_lots_from_new_assignments(today)         # turns ASSIGNED CSPs into wheel lots
    link_new_ccs_to_lots(today)                     # attaches OPEN CCs to lots + logs wheel events

    # --- Wheel exposure ---
    exposure = compute_wheel_exposure(today)
    week_remaining = compute_week_remaining(today)

    print("\n💼 WHEEL EXPOSURE (INDIVIDUAL options)")
    print(f"  Total exposure: ${exposure['total_exposure']:,.0f} / ${exposure['cap']:,.0f}")
    print(f"  Weekly target:  ${exposure['weekly_target']:,.0f}")
    print(f"  Weekly remaining: ${week_remaining:,.0f}")

    # --- Retirement exposure (market value) ---
    mv_ret = strat.retirement_market_value_by_account(ret_by_key)
    print("\n🏦 RETIREMENT EXPOSURE (stock market value)")
    for acct in (IRA, ROTH):
        mv = mv_ret.get(acct, 0.0)
        cap = float(ACCOUNT_SIZES.get(acct, 0))
        remaining = max(cap - mv, 0.0)
        print(f"  {acct:<5} MV ${mv:,.0f} / ${cap:,.0f} | Remaining ${remaining:,.0f}")
    if ret_flagged:
        print(f"  ⚠️ Breakeven-only flagged: {', '.join(ret_flagged)}")

    # --- INDIVIDUAL stock cap (separate from wheel) ---
    stock_pos_rows = strat.load_stock_positions()
    # get latest prices for open positions for MV calc
    open_stock = [r for r in stock_pos_rows if (r.get("status") or "").upper() == "OPEN"]
    tickers = sorted({(r.get("ticker") or "").strip().upper() for r in open_stock})
    prices = {}
    if tickers:
        import yfinance as yf
        import pandas as pd
        for tkr in tickers:
            try:
                df = yf.download(tkr, period="7d", interval="1d", auto_adjust=False, progress=False)
                df.dropna(inplace=True)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                if not df.empty:
                    prices[tkr] = float(df["Close"].iloc[-1])
            except Exception:
                continue

    mv_stock = strat.stock_market_value_by_account(stock_pos_rows, prices)
    indiv_stock_mv = mv_stock.get(INDIVIDUAL, 0.0)
    print("\n📦 INDIVIDUAL STOCK CAP (non-wheel)")
    print(f"  MV ${indiv_stock_mv:,.0f} / ${float(INDIVIDUAL_STOCK_CAP):,.0f} | Remaining ${max(float(INDIVIDUAL_STOCK_CAP)-indiv_stock_mv, 0.0):,.0f}")

    # --- Stock scan (always) ---
    entries, watch = scan_stock_entries_and_watchlist()

    if entries:
        print("\n📈 STOCK ENTRIES (signals)")
        for e in entries[:15]:
            print(f"  {e['ticker']:<6} {e['signal']:<9} Close {e['close']:.2f} | RSI2 {e['rsi2']:.1f}")
    else:
        print("\n📋 WATCHLIST")
        for w in watch[:20]:
            print(f"  {w['ticker']:<6} {w['note']:<13} Close {w['close']:.2f} | RSI2 {w['rsi2']:.1f}")

    # --- Stock planning + paper execution ---
    stock_opened: List[str] = []
    if entries:
        # existing open tickers per account
        open_rows = [r for r in stock_pos_rows if (r.get("status") or "").upper() == "OPEN"]
        open_by_acct = {INDIVIDUAL: set(), IRA: set(), ROTH: set()}
        for r in open_rows:
            acct = (r.get("account") or "").strip().upper()
            tkr = (r.get("ticker") or "").strip().upper()
            if acct in open_by_acct and tkr:
                open_by_acct[acct].add(tkr)

        # Current MV per account for sizing
        acct_mv = {
            INDIVIDUAL: float(indiv_stock_mv),
            IRA: float(mv_ret.get(IRA, 0.0)) + float(mv_stock.get(IRA, 0.0)),
            ROTH: float(mv_ret.get(ROTH, 0.0)) + float(mv_stock.get(ROTH, 0.0)),
        }

        print("\n🧠 STOCK PLANS (paper execution)")
        planned = 0
        for e in entries:
            if planned >= 3:
                break

            tkr = e["ticker"]
            sig = e["signal"]
            last = e["_last"]

            # Decide which account to use
            acct_order = []
            if trading_on:
                acct_order.append(INDIVIDUAL)
            if retire_on:
                acct_order.extend([IRA, ROTH])

            planned_this = False
            for acct in acct_order:
                # Retirement "breakeven-only" flag for that ticker blocks new adds
                be_only = (f"{acct}:{tkr}" in ret_by_key and (ret_by_key[f"{acct}:{tkr}"].get("flag_breakeven_only") == "1"))
                plan = strat.plan_stock_trade(
                    account=acct,
                    ticker=tkr,
                    signal=sig,
                    last=last,
                    mkt=mkt,
                    existing_open_tickers=open_by_acct.get(acct, set()),
                    acct_current_mv=acct_mv.get(acct, 0.0),
                    retirement_breakeven_only=be_only,
                )
                if not plan:
                    continue

                pos_id = strat.execute_stock_plan(today, plan)
                stock_opened.append(f"{acct}:{tkr}")
                open_by_acct[acct].add(tkr)
                acct_mv[acct] = acct_mv.get(acct, 0.0) + float(plan["entry_price"]) * int(plan["shares"])

                # Print actionable details
                risk = (plan["entry_price"] - plan["stop_price"]) * int(plan["shares"])
                print(
                    f"  {acct:<10} {tkr:<6} {sig:<9} "
                    f"Entry {plan['entry_price']:.2f} | Shares {plan['shares']:<5} "
                    f"Stop {plan['stop_price']:.2f} | Target {plan['target_price']:.2f} | Risk ${risk:,.0f}"
                )
                planned += 1
                planned_this = True
                break

            if not planned_this:
                # Still show why it didn't get a plan (high-level)
                if sig and not strat.nextday_valid_for_entry(sig, last):
                    print(f"  {tkr:<6} {sig:<9} skipped: next-day validity filter")
                else:
                    print(f"  {tkr:<6} {sig:<9} skipped: cap/risk/duplicate/breakeven-only")

    # --- CSP planning ---
    new_csp_orders: List[dict] = []
    if ENABLE_CSP and allow_conservative_premium(mkt) and week_remaining > 0:
        candidates = build_csp_candidates()

        total_remaining = max(float(exposure["cap"]) - float(exposure["total_exposure"]), 0.0)

        plan = strat.plan_weekly_csp_orders(
            candidates,
            today=today,
            vix_close=float(mkt["vix_close"]),
            total_remaining_cap=total_remaining,
            week_remaining_cap=float(week_remaining),
            aggressive_total=int(exposure.get("aggressive_total", 0)),
            aggressive_week=int(exposure.get("aggressive_week", 0)),
        )

        orders = plan.get("selected", [])
        if orders:
            new_csp_orders = orders
            print("\n🧾 NEW CSP IDEAS (paper execution)")
            for o in orders:
                print(f"  {o['ticker']:<6} {o['strike']:.0f}P {o['expiry']} | est prem ${o['est_premium']:.0f} | cash ${o['cash_reserved']:,.0f} | {o.get('tier','')}")

            for o in orders:
                csp_id = strat.add_csp_position_from_selected(today.isoformat(), exposure["week_id"], o)

                # CSP ledger (idempotent)
                try:
                    ledger_rows = strat.load_csv_rows(CSP_LEDGER_FILE)
                    if not strat.csp_already_logged(ledger_rows, exposure["week_id"], o["ticker"], o["expiry"], float(o["strike"])):
                        strat.append_csp_ledger_row({
                            "date": today.isoformat(),
                            "week_id": exposure["week_id"],
                            "ticker": o["ticker"],
                            "expiry": o["expiry"],
                            "strike": f"{float(o['strike']):.2f}",
                            "contracts": int(o.get("contracts", 1)),
                            "credit_mid": float(o.get("mid", 0.0)),
                            "cash_reserved": float(o.get("cash_reserved", 0.0)),
                            "est_premium": float(o.get("est_premium", 0.0)),
                            "tier": o.get("tier", ""),
                        })
                except Exception:
                    pass

                record_event(
                    date=today.isoformat(),
                    ticker=o["ticker"],
                    event_type="CSP_OPEN",
                    ref_id=csp_id,
                    expiry=o["expiry"],
                    strike=float(o["strike"]),
                    contracts=int(o.get("contracts", 1)),
                    shares=int(o.get("contracts", 1)) * 100,
                    premium=float(o.get("est_premium", 0.0)),
                    wheel_value=float(o.get("cash_reserved", 0.0)),
                    notes="CSP opened (planned by screener)",
                )
        else:
            print("\n🧾 CSP: No new entries today.")
    else:
        print("\n🧾 CSP scanning skipped (market filter or allocation).")

    # --- CC planning (ALWAYS runs; inventory management) ---
    new_cc_orders: List[dict] = []
    new_cc_orders = plan_ccs_from_open_lots()

    if new_cc_orders:
        print("\n📞 NEW CC IDEAS (from lots) (paper execution)")
        for o in new_cc_orders[:15]:
            credit_est = float(o.get("credit_mid", 0.0)) * 100.0
            print(f"  {o['ticker']:<6} {float(o['strike']):.0f}C {o['expiry']} | est credit ${credit_est:.0f}")

        for o in new_cc_orders:
            strat.add_cc_position_from_candidate(today.isoformat(), o)

        link_new_ccs_to_lots(today)
    else:
        print("\n📞 CC: No calls triggered today.")

    # Backfill ONLY if missing events
    if should_backfill_events():
        backfill_open_events_from_positions(today)

    rebuild_monthly_from_events()

    # --- Discord (alerts only) ---
    alert = build_discord_alert(
        mkt=mkt,
        trading_on=trading_on,
        new_csps=new_csp_orders,
        new_ccs=new_cc_orders,
        entries=entries,
        watch=watch,
        csp_exp=csp_out.get("expired", []),
        csp_asn=csp_out.get("assigned", []),
        cc_exp=cc_out.get("expired", []),
        cc_call=cc_out.get("called_away", []),
        stock_opens=stock_opened,
        stock_closes=stock_closed,
    )
    send_discord(alert)

    print("\n✅ Screener run complete.")


if __name__ == "__main__":
    run_screener()
