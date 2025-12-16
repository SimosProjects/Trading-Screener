# screener.py
import datetime as dt
from typing import Dict, List, Tuple

from config import (
    WEBHOOK_URL,
    STOCKS,
    CSP_STOCKS,
    ENABLE_CSP,
    WHEEL_CAP,
    WHEEL_WEEKLY_TARGET,
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
    backfill_open_events_from_positions
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

def trading_allowed(mkt: Dict) -> bool:
    """
    Keep the exact vibe you liked:
    - SPY above 200 & 50 & 21
    - QQQ above 50
    - VIX < 25 (and <18 is "low")
    """
    return bool(
        mkt.get("spy_above_200")
        and mkt.get("spy_above_50")
        and mkt.get("spy_above_21")
        and mkt.get("qqq_above_50")
        and mkt.get("vix_below_25")
    )


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
                entries.append({
                    "ticker": tkr,
                    "signal": "PULLBACK" if pb else "BREAKOUT",
                    "close": float(last["Close"]),
                    "sma50": float(last["SMA_50"]),
                    "sma200": float(last["SMA_200"]),
                    "rsi2": float(last["RSI_2"]),
                    "atr": float(last["ATR_14"]),
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
    entries = sorted(entries, key=lambda x: (x["signal"], -x["rsi2"]))
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
    Uses the strategy helper 'decide_cc_strike' and option chain selection from strategies.
    """
    ideas: List[dict] = []
    lots = get_open_lots()
    if not lots:
        return ideas

    # Build a quick set of tickers that already have OPEN CCs in cc_positions.csv
    open_cc_rows = strat.load_csv_rows(strat.CC_POSITIONS_FILE)
    open_cc_tickers = { (r.get("ticker") or "").strip().upper()
                        for r in open_cc_rows
                        if (r.get("status") or "").upper() == "OPEN" }

    # Treat each lot like an assigned row for the existing planner
    assigned_rows = []
    for lot in lots:
        if lot.get("has_open_cc"):
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

def print_market_context(mkt: Dict, trading_on: bool) -> None:
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

    print(("🟢 Trading ON — Market conditions favorable." if trading_on else "🔴 Trading OFF — Market conditions not favorable."))


def build_discord_alert(mkt: Dict, trading_on: bool, new_csps: List[dict], new_ccs: List[dict],
                        entries: List[dict], watch: List[dict],
                        csp_exp: List[str], csp_asn: List[str], cc_exp: List[str], cc_call: List[str]) -> str:
    """
    Keep it subscriber-safe:
    - include market conditions, entries/watchlist, and position maintenance outcomes
    - exclude account size, caps, wheel exposure amounts, etc.
    """
    lines = []
    lines.append(f"📅 {dt.date.today().isoformat()} Screener")
    lines.append(f"Market: {'ON' if trading_on else 'OFF'} | SPY {mkt['spy_close']:.2f} | QQQ {mkt['qqq_close']:.2f} | VIX {mkt['vix_close']:.2f}")

    if csp_exp or csp_asn or cc_exp or cc_call:
        lines.append("— Maintenance —")
        if csp_exp:  lines.append(f"CSP expired: {', '.join(csp_exp[:8])}{'…' if len(csp_exp)>8 else ''}")
        if csp_asn:  lines.append(f"CSP assigned: {', '.join(csp_asn[:8])}{'…' if len(csp_asn)>8 else ''}")
        if cc_exp:   lines.append(f"CC expired: {', '.join(cc_exp[:8])}{'…' if len(cc_exp)>8 else ''}")
        if cc_call:  lines.append(f"Called away: {', '.join(cc_call[:8])}{'…' if len(cc_call)>8 else ''}")

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
    trading_on = trading_allowed(mkt)
    print_market_context(mkt, trading_on)

    # --- Ensure files + maintenance ALWAYS (even when trading OFF) ---
    ensure_wheel_files()
    strat.ensure_positions_files()

    csp_out = strat.process_csp_expirations(today)  # updates csp_positions.csv
    cc_out = process_cc_expirations(today)          # updates cc_positions.csv + lots

    create_lots_from_new_assignments(today)         # turns ASSIGNED CSPs into wheel lots
    link_new_ccs_to_lots(today)                     # attaches OPEN CCs to lots + logs wheel events

    # --- Exposure (screen-only) ---
    exposure = compute_wheel_exposure(today)
    week_remaining = compute_week_remaining(today)
    print("\n💼 WHEEL EXPOSURE")
    print(f"  Total exposure: ${exposure['total_exposure']:,.0f} / ${exposure['cap']:,.0f}")
    print(f"  Weekly target:  ${exposure['weekly_target']:,.0f}")
    print(f"  Weekly remaining: ${week_remaining:,.0f}")

    # Allocation (screen-only)
    stock_alloc = max(int(exposure["cap"]) - int(exposure["total_exposure"]), 0)
    print("\n🧮 ALLOCATION")
    print(f"  Wheel cap: ${WHEEL_CAP:,.0f} | Weekly target: ${WHEEL_WEEKLY_TARGET:,.0f}")
    print(f"  Remaining under wheel cap: ${stock_alloc:,.0f}")

    # --- Stock scan (always, so you see watchlist even if trading OFF) ---
    entries, watch = scan_stock_entries_and_watchlist()

    if entries:
        print("\n📈 STOCK ENTRIES")
        for e in entries[:15]:
            print(f"  {e['ticker']:<6} {e['signal']:<9} Close {e['close']:.2f} | RSI2 {e['rsi2']:.1f}")
    else:
        print("\n📋 WATCHLIST")
        for w in watch[:20]:
            print(f"  {w['ticker']:<6} {w['note']:<13} Close {w['close']:.2f} | RSI2 {w['rsi2']:.1f}")

    # --- CSP planning (only if market ON and cap allows) ---
    new_csp_orders: List[dict] = []
    if ENABLE_CSP and trading_on and week_remaining > 0:
        candidates = build_csp_candidates()

        # Total remaining under cap is computed from current exposure
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

        orders = plan.get('selected', [])
        if orders:
            new_csp_orders = orders
            print("\n🧾 NEW CSP IDEAS")
            for o in orders:
                print(f"  {o['ticker']:<6} {o['strike']:.0f}P {o['expiry']} | est prem ${o['est_premium']:.0f} | cash ${o['cash_reserved']:,.0f}")

            # Persist CSP positions + log wheel events
            for o in orders:
                csp_id = strat.add_csp_position_from_selected(today.isoformat(), exposure['week_id'], o)
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

    # --- CC planning (only if market ON; from lots only) ---
    new_cc_orders: List[dict] = []
    if trading_on:
        new_cc_orders = plan_ccs_from_open_lots()
        if new_cc_orders:
            print("\n📞 NEW CC IDEAS (from lots)")
            for o in new_cc_orders[:15]:
                credit_est = float(o.get("credit_mid", 0.0)) * 100.0
                print(f"  {o['ticker']:<6} {float(o['strike']):.0f}C {o['expiry']} | est credit ${credit_est:.0f}")

            # persist CC positions (wheel will link + record CC_OPEN event)
            for o in new_cc_orders:
                strat.add_cc_position_from_candidate(today.isoformat(), o)
            link_new_ccs_to_lots(today)
        else:
            print("\n📞 CC: No new calls today.")
    else:
        print("\n📞 CC scanning skipped (market filter).")

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
    )
    send_discord(alert)

    print("\n✅ Screener run complete.")


if __name__ == "__main__":
    run_screener()
