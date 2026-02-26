"""screener_display.py

All terminal output and Discord alert formatting.

Print functions are extracted verbatim from screener.py so the output
format is byte-for-byte identical.  No trading logic lives here.
"""

from __future__ import annotations

import datetime as dt
from typing import Dict, List

import strategies as strat
from utils import get_logger
from config import (
    INDIVIDUAL, IRA, ROTH,
    INDIVIDUAL_STOCK_CAP,
    WEBHOOK_URL,
    CSP_RISK_OFF_VIX,
    CSP_STRIKE_BASE_NORMAL, CSP_STRIKE_BASE_RISK_OFF,
    CSP_RISK_OFF_MIN_OTM_PCT_DEFENSIVE,
    CSP_NORMAL_MIN_OTM_PCT,
    CSP_POSITIONS_FILE,
    CC_POSITIONS_FILE,
    CC_ROLL_SIGNAL_THRESHOLD,
    ENABLE_CSP,
)
from wheel import compute_wheel_exposure, compute_week_remaining

log = get_logger(__name__)


# ============================================================
# Discord
# ============================================================

def send_discord(msg: str) -> None:
    if not WEBHOOK_URL or not WEBHOOK_URL.strip():
        return
    url = WEBHOOK_URL.strip()
    if not url.startswith("http"):
        return
    try:
        import requests
        requests.post(url, json={"content": msg[:1900]}, timeout=10)
    except Exception as e:
        log.warning("Discord send failed: %s", e)


def build_discord_alert(
    mkt: Dict,
    trading_on: bool,
    new_csps: List[dict],
    new_ccs: List[dict],
    planned_stocks: List[dict],
    watch: List[dict],
    csp_tp: List[str],
    csp_exp: List[str],
    csp_asn: List[str],
    cc_exp: List[str],
    cc_call: List[str],
    stock_opens: List[str],
    stock_closes: List[str],
    ret_stopped: List[str] = [],
) -> str:
    lines: List[str] = []
    lines.append(f"📅 {dt.date.today().isoformat()} Screener")
    lines.append(
        f"Market: {'ON' if trading_on else 'OFF'} | SPY {mkt['spy_close']:.2f} | "
        f"QQQ {mkt['qqq_close']:.2f} | VIX {mkt['vix_close']:.2f}"
    )

    if csp_tp or csp_exp or csp_asn or cc_exp or cc_call or stock_opens or stock_closes or ret_stopped:
        lines.append("— Maintenance —")
        if ret_stopped:  lines.append(f"🛑 Retirement stops: {', '.join(ret_stopped[:8])}{'…' if len(ret_stopped)>8 else ''}")
        if csp_tp:       lines.append(f"CSP closed (TP): {', '.join(csp_tp[:8])}{'…' if len(csp_tp)>8 else ''}")
        if csp_exp:      lines.append(f"CSP expired: {', '.join(csp_exp[:8])}{'…' if len(csp_exp)>8 else ''}")
        if csp_asn:      lines.append(f"CSP assigned: {', '.join(csp_asn[:8])}{'…' if len(csp_asn)>8 else ''}")
        if cc_exp:       lines.append(f"CC expired: {', '.join(cc_exp[:8])}{'…' if len(cc_exp)>8 else ''}")
        if cc_call:      lines.append(f"Called away: {', '.join(cc_call[:8])}{'…' if len(cc_call)>8 else ''}")
        if stock_closes: lines.append(f"Stocks closed: {', '.join(stock_closes[:10])}{'…' if len(stock_closes)>10 else ''}")
        if stock_opens:  lines.append(f"Stocks opened: {', '.join(stock_opens[:10])}{'…' if len(stock_opens)>10 else ''}")

    if new_csps:
        lines.append("— New CSP ideas —")
        for x in new_csps[:10]:
            acct_tag = f"[{x['account']}] " if x.get("account") else ""
            lines.append(f"{acct_tag}{x['ticker']} {x['strike']:.0f}P {x['expiry']} ~${x['est_premium']:.0f}")

    if new_ccs:
        lines.append("— New CC ideas —")
        for x in new_ccs[:10]:
            acct_tag = f"[{x['account']}] " if x.get("account") else ""
            lines.append(f"{acct_tag}{x['ticker']} {x['strike']:.0f}C {x['expiry']} ~${x['credit_mid']*100:.0f}")

    if planned_stocks:
        lines.append("— Stock entries (planned) —")
        for p in planned_stocks[:10]:
            lines.append(f"{p['ticker']} {p['signal']} {p['account']} @ {p['entry_price']:.2f}")

    if watch and not planned_stocks:
        lines.append("— Watchlist —")
        for w in watch[:12]:
            lines.append(f"{w['ticker']} ({w['note']}) @ {w['close']:.2f}")

    return "\n".join(lines)


# ============================================================
# Market context header
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


# ============================================================
# Holdings table
# ============================================================

def print_open_holdings(holdings: List[dict]) -> None:
    if not holdings:
        print("\n📌 OPEN STOCK HOLDINGS: none\n")
        return

    print("\n📌 OPEN STOCK HOLDINGS (all accounts) — Unrealized P/L")
    print("    (Excludes CSP/CC options; includes assigned Wheel lots)\n")

    holdings_sorted = sorted(holdings, key=lambda x: (x["account"], x["ticker"]))
    by_acct: Dict[str, List[dict]] = {}
    for h in holdings_sorted:
        by_acct.setdefault(h["account"], []).append(h)

    for acct, rows in by_acct.items():
        acct_pnl    = sum(r["pnl"] for r in rows)
        acct_mv     = sum((r["cur"] * r["shares"]) for r in rows if r["cur"] > 0)
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


# ============================================================
# Open CSP positions summary
# ============================================================

def print_open_csps(today: dt.date) -> None:
    try:
        csp_rows = strat.load_csv_rows(CSP_POSITIONS_FILE)
        open_csps = []
        for r in csp_rows:
            if (r.get("status") or "").upper() != "OPEN":
                continue
            exp = (r.get("expiry") or "").strip()
            try:
                if exp and dt.date.fromisoformat(exp) < today:
                    continue
            except Exception:
                pass
            open_csps.append(r)

        if open_csps:
            print("\n🧾 OPEN CSP POSITIONS")
            for r in open_csps[:10]:
                tkr      = (r.get("ticker") or "").strip().upper()
                acct     = (r.get("account") or "").strip().upper()
                exp      = (r.get("expiry") or "").strip()
                strike   = r.get("strike") or ""
                prem     = r.get("premium") or r.get("est_premium") or ""
                dte_open = r.get("dte_open") or ""
                print(f"  [{acct}] {tkr:<6} {exp} {strike}P | prem {prem} | dte_open {dte_open}")
    except Exception as e:
        log.warning("print_open_csps failed: %s", e)



# ============================================================
# Open CC roll-candidate signals
# ============================================================

def print_open_cc_roll_candidates(px: Dict[str, float]) -> None:
    """
    Flag open CCs where the stock has recovered close to the strike.

    When current_price / strike >= CC_ROLL_SIGNAL_THRESHOLD the CC is at
    risk of assignment before expiry on a continued rally.  This is purely
    informational — no automated roll is executed.  The human decides whether
    to buy-to-close and re-sell at a higher strike / later expiry.
    """
    try:
        cc_rows = strat.load_csv_rows(CC_POSITIONS_FILE)
        candidates = []
        for r in cc_rows:
            if (r.get("status") or "").upper() != "OPEN":
                continue
            tkr = (r.get("ticker") or "").strip().upper()
            if not tkr:
                continue
            try:
                strike = float(r.get("strike") or 0)
                if strike <= 0:
                    continue
            except Exception:
                continue

            cur = px.get(tkr)
            if cur is None or cur <= 0:
                continue

            # How close is the stock to being called away?
            pct_to_strike = (strike - cur) / strike  # positive = OTM, negative = ITM
            proximity = cur / strike  # >= threshold means "getting close"

            if proximity >= float(CC_ROLL_SIGNAL_THRESHOLD):
                exp = (r.get("expiry") or "").strip()
                candidates.append((tkr, strike, cur, pct_to_strike * 100, exp))

        if candidates:
            print("\n⚠️  CC ROLL CANDIDATES (within {:.0f}% of strike — consider rolling up/out)".format(
                (1.0 - float(CC_ROLL_SIGNAL_THRESHOLD)) * 100
            ))
            for tkr, strike, cur, pct_otm, exp in candidates:
                flag = "🔴 ITM" if pct_otm < 0 else "🟡 near"
                direction = "ITM" if pct_otm < 0 else "OTM"
                print(f"  {flag} {tkr:<6} {strike:.0f}C {exp} | Now {cur:.2f} ({abs(pct_otm):.1f}% {direction})")
    except Exception as e:
        log.warning("print_open_cc_roll_candidates failed: %s", e)


# ============================================================
# Final exposure summary
# ============================================================

def print_final_exposure_summary(
    today: dt.date,
    ret_by_key: dict,
    ret_flagged: list,
    mv_stock: Dict[str, float],
    wheel_mv: float,
) -> None:
    print("\n💼 WHEEL EXPOSURE (all accounts)")
    for acct in (INDIVIDUAL, IRA, ROTH):
        exp = compute_wheel_exposure(today, acct)
        rem = compute_week_remaining(today, acct)
        print(f"  {acct:<10} "
              f"${exp['total_exposure']:>8,.0f} / ${exp['cap']:>8,.0f}  "
              f"| wk target ${exp['weekly_target']:>7,.0f}  "
              f"| wk rem ${rem:>7,.0f}")

    # Retirement MV
    mv_ret = strat.retirement_market_value_by_account(ret_by_key)

    total_ret_mv = sum(mv_ret.get(a, 0.0) for a in (IRA, ROTH))
    total_ret_mv += sum(mv_stock.get(a, 0.0) for a in (IRA, ROTH))

    if total_ret_mv > 0 or ret_flagged:
        print(f"\n🏦 RETIREMENT MV: ${total_ret_mv:,.0f}")
        for acct in (IRA, ROTH):
            mv = mv_ret.get(acct, 0.0) + mv_stock.get(acct, 0.0)
            if mv > 0:
                print(f"  {acct}: ${mv:,.0f}")
    if ret_flagged:
        print(f"  ⚠️ Breakeven-only flagged: {', '.join(ret_flagged)}")

    indiv_stock_mv = float(mv_stock.get(INDIVIDUAL, 0.0))
    print("\n📦 INDIVIDUAL STOCK CAP (non-wheel swing trades)")
    print(
        f"  MV ${indiv_stock_mv:,.0f} / ${float(INDIVIDUAL_STOCK_CAP):,.0f} | "
        f"Remaining ${max(float(INDIVIDUAL_STOCK_CAP)-indiv_stock_mv, 0.0):,.0f}"
    )
