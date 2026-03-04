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
    RETIREMENT_STOCK_CAPS,
    RETIREMENT_STOCK_YIELDS,
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
    cc_tp: List[str],
    csp_exp: List[str],
    csp_asn: List[str],
    cc_exp: List[str],
    cc_call: List[str],
    stock_opens: List[str],
    stock_closes: List[str],
    ret_stopped: List[str] = [],
    early_asn: List[str] = [],
    csp_roll: List[dict] = [],
) -> str:
    lines: List[str] = []
    lines.append(f"📅 {dt.date.today().isoformat()} Screener")
    lines.append(
        f"Market: {'ON' if trading_on else 'OFF'} | SPY {mkt['spy_close']:.2f} | "
        f"QQQ {mkt['qqq_close']:.2f} | VIX {mkt['vix_close']:.2f}"
    )

    if csp_tp or cc_tp or csp_exp or csp_asn or cc_exp or cc_call or stock_opens or stock_closes or ret_stopped or early_asn:
        lines.append("— Maintenance —")
        if ret_stopped:  lines.append(f"🛑 Retirement stops: {', '.join(ret_stopped[:8])}{'…' if len(ret_stopped)>8 else ''}")
        if early_asn:    lines.append(f"⚠️ Early assigned: {', '.join(early_asn[:8])}{'…' if len(early_asn)>8 else ''}")
        if csp_tp:       lines.append(f"✅ CSP closed (TP): {', '.join(csp_tp[:8])}{'…' if len(csp_tp)>8 else ''}")
        if cc_tp:        lines.append(f"✅ CC closed (TP): {', '.join(cc_tp[:8])}{'…' if len(cc_tp)>8 else ''}")
        if csp_exp:      lines.append(f"CSP expired: {', '.join(csp_exp[:8])}{'…' if len(csp_exp)>8 else ''}")
        if csp_asn:      lines.append(f"CSP assigned: {', '.join(csp_asn[:8])}{'…' if len(csp_asn)>8 else ''}")
        if cc_exp:       lines.append(f"CC expired: {', '.join(cc_exp[:8])}{'…' if len(cc_exp)>8 else ''}")
        if cc_call:      lines.append(f"Called away: {', '.join(cc_call[:8])}{'…' if len(cc_call)>8 else ''}")
        if stock_closes: lines.append(f"Stocks closed: {', '.join(stock_closes[:10])}{'…' if len(stock_closes)>10 else ''}")
        if stock_opens:  lines.append(f"Stocks opened: {', '.join(stock_opens[:10])}{'…' if len(stock_opens)>10 else ''}")

    if csp_roll:
        lines.append("— CSP Roll Candidates —")
        for r in csp_roll[:6]:
            lines.append(
                f"🔄 [{r['account']}] {r['ticker']} {r['strike']:.0f}P {r['expiry']} "
                f"| {r['pct_itm']:.1f}% ITM | {r['dte']}d | px {r['current_price']:.2f}"
            )

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
# Open CC positions summary
# ============================================================

def print_open_ccs(today: dt.date, px: Dict[str, float]) -> None:
    """Print a summary table of all open covered calls with live ITM/OTM status."""
    try:
        cc_rows = strat.load_csv_rows(CC_POSITIONS_FILE)
        open_ccs = [
            r for r in cc_rows
            if (r.get("status") or "").upper() == "OPEN"
        ]
        if not open_ccs:
            return

        print("\n📞 OPEN CC POSITIONS")
        for r in open_ccs:
            tkr      = (r.get("ticker")   or "").strip().upper()
            acct     = (r.get("account")  or "").strip().upper()
            exp      = (r.get("expiry")   or "").strip()
            strike   = r.get("strike")    or ""
            prem     = r.get("premium")   or ""
            contracts = r.get("contracts") or ""

            # Days to expiry
            dte_str = ""
            try:
                if exp:
                    dte = (dt.date.fromisoformat(exp) - today).days
                    dte_str = f"{dte}d"
            except Exception:
                pass

            # ITM / OTM status vs current price
            status_str = ""
            cur = px.get(tkr)
            try:
                if cur and cur > 0 and strike:
                    s = float(strike)
                    if s > 0:
                        pct = (cur - s) / s * 100.0
                        if cur >= s:
                            status_str = f"🔴 ITM {pct:+.1f}%"
                        else:
                            status_str = f"🟢 OTM {pct:+.1f}%"
            except Exception:
                pass

            print(
                f"  [{acct}] {tkr:<6} {exp} {strike}C "
                f"| prem ${prem} | {contracts}x "
                f"| {dte_str} {status_str}"
            )
    except Exception as e:
        log.warning("print_open_ccs failed: %s", e)

def print_open_cc_roll_candidates(px: Dict[str, float], today: "dt.date | None" = None) -> None:
    """
    Flag open CCs where the stock has recovered close to or through the strike,
    then prompt the user to act on each one interactively.

    Options presented per candidate:
      1  Roll up & out  — buy-to-close, sell higher strike on later expiry
                          (strike chosen by ATR-tier logic, same as fresh CC open)
      2  Roll out       — buy-to-close, re-sell SAME strike on later expiry
      3  Close only     — buy-to-close, no replacement (lot stays open, new CC
                          can be planned on next run)
      4  Skip           — do nothing for this candidate this run

    Debit rolls are blocked by policy — if the net of closing+opening would cost
    money the system explains why and treats the candidate as skipped.

    The function is intentionally defensive: any failure in the interactive path
    logs a warning and falls through to the next candidate rather than crashing
    the screener run.
    """
    import datetime as _dt
    if today is None:
        today = _dt.date.today()

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

            pct_to_strike = (strike - cur) / strike
            proximity     = cur / strike

            if proximity >= float(CC_ROLL_SIGNAL_THRESHOLD):
                exp = (r.get("expiry") or "").strip()
                candidates.append({
                    "row":    r,
                    "tkr":    tkr,
                    "strike": strike,
                    "cur":    cur,
                    "pct":    pct_to_strike * 100,   # negative = ITM
                    "exp":    exp,
                })

        if not candidates:
            return

        threshold_pct = (1.0 - float(CC_ROLL_SIGNAL_THRESHOLD)) * 100
        print(f"\n⚠️  CC ROLL CANDIDATES (within {threshold_pct:.0f}% of strike — action required?)")

        for cand in candidates:
            tkr    = cand["tkr"]
            strike = cand["strike"]
            cur    = cand["cur"]
            pct    = cand["pct"]        # negative means ITM
            exp    = cand["exp"]
            r      = cand["row"]

            flag      = "🔴 ITM " if pct < 0 else "🟡 near"
            direction = "ITM"         if pct < 0 else "OTM"

            print(f"\n  {flag} {tkr:<6} {strike:.0f}C {exp} | Now {cur:.2f} ({abs(pct):.1f}% {direction})")
            print(f"    1  Roll up & out  (new strike via ATR tier, later expiry)")
            print(f"    2  Roll out only  (same strike {strike:.0f}C, later expiry)")
            print(f"    3  Close CC only  (buy to close, keep shares, fresh CC next run)")
            print(f"    4  Close CC + exit shares  (unwind entire position, book P&L)")
            print(f"    5  Skip           (do nothing)")

            # Read user choice with a 30-second timeout.
            # If no input arrives (background run, piped, or user away) the
            # default is always Skip — the screener never hangs.
            choice = "5"
            try:
                import sys as _sys
                import select as _select
                print("    Choice [1/2/3/4/5, default=5, 30s timeout]: ", end="", flush=True)
                ready, _, _ = _select.select([_sys.stdin], [], [], 30)
                if ready:
                    raw = _sys.stdin.readline().strip()
                    if raw in ("1", "2", "3", "4", "5"):
                        choice = raw
                    elif raw == "":
                        choice = "5"
                    else:
                        print(f"\n    ⚠️  Unrecognised input '{raw}' — defaulting to Skip.")
                        choice = "5"
                else:
                    print("\n    [no response in 30s — defaulting to Skip]")
                    choice = "5"
            except (EOFError, KeyboardInterrupt):
                print("\n    [non-interactive — skipping]")
                choice = "5"
            except Exception as e:
                log.warning("CC roll prompt error for %s: %s", tkr, e)
                choice = "5"

            if choice == "5":
                print(f"    → Skipped {tkr}.")
                continue

            if choice == "3":
                # Close CC only — mark CC closed, leave shares, fresh CC next run
                print(f"    Closing CC for {tkr} {exp} {strike:.0f}C …", end=" ", flush=True)
                try:
                    rows_all = strat.load_csv_rows(CC_POSITIONS_FILE)
                    old_id   = (r.get("id") or "").strip()
                    changed  = False
                    for row in rows_all:
                        if (row.get("id") or "").strip() == old_id:
                            row["status"]     = "CLOSED_MANUAL"
                            row["close_date"] = today.isoformat()
                            row["close_type"] = "CLOSED_MANUAL"
                            row["notes"]      = f"Manually closed via roll-prompt on {today}"
                            changed = True
                            break
                    if changed:
                        strat.write_csv_rows(CC_POSITIONS_FILE, rows_all,
                                             strat.CC_POSITIONS_COLUMNS)
                        import sys
                        _whl = sys.modules.get("wheel")
                        src_lot = (r.get("source_lot_id") or "").strip()
                        if _whl and src_lot:
                            lots = _whl._read_rows(_whl.WHEEL_LOTS_FILE)
                            for lot in lots:
                                if (lot.get("lot_id") or "").strip() == src_lot:
                                    lot["has_open_cc"] = "0"
                                    lot["cc_id"]       = ""
                                    break
                            _whl._write_rows(_whl.WHEEL_LOTS_FILE, lots, _whl.LOT_FIELDS)
                        print("✅  CC closed. Shares kept — new CC will be planned next run.")
                    else:
                        print("⚠️  CC row not found — nothing changed.")
                except Exception as e:
                    log.warning("CC close-only failed for %s: %s", tkr, e)
                    print(f"❌  Failed: {e}")
                continue

            if choice == "4":
                # Close CC + sell shares — full position exit
                print(f"    Closing CC and exiting {tkr} shares …", end=" ", flush=True)
                try:
                    result = strat.execute_cc_close_and_exit(today, r)
                    if result["ok"]:
                        print(f"✅  {result['reason']}")
                    else:
                        print(f"❌  Failed: {result['reason']}")
                except Exception as e:
                    log.warning("execute_cc_close_and_exit failed for %s: %s", tkr, e)
                    print(f"❌  Error: {e}")
                continue

            # Choice 1 or 2 — execute a roll
            roll_up = (choice == "1")
            roll_label = "up & out" if roll_up else "out only"
            print(f"    Rolling {tkr} {roll_label} …", end=" ", flush=True)
            try:
                result = strat.execute_cc_roll(today, r, roll_up=roll_up)
                if result["ok"]:
                    print(f"✅  {result['reason']}")
                else:
                    print(f"❌  Blocked: {result['reason']}")
                    print(f"    Position unchanged — choose Skip or Close only to proceed manually.")
            except Exception as e:
                log.warning("execute_cc_roll failed for %s: %s", tkr, e)
                print(f"❌  Error: {e}")

    except Exception as e:
        log.warning("print_open_cc_roll_candidates failed: %s", e)


def print_csp_roll_candidates(candidates: List[dict]) -> None:
    """Display CSP roll candidates flagged by scan_csp_roll_candidates.

    These are open CSPs that are >=10% ITM with >10 DTE remaining — enough
    time to buy-to-close and re-open at a lower strike / further expiry for
    a potential net credit.  Display-only — no automated action.
    """
    if not candidates:
        return
    print(f"\n🔄 CSP ROLL CANDIDATES ({len(candidates)} position{'s' if len(candidates) != 1 else ''})")
    print("   (>=10% ITM, >10 DTE — consider rolling down/out for net credit)")
    for c in candidates:
        print(
            f"  [{c['account']}] {c['ticker']:<6} {c['strike']:.0f}P {c['expiry']} "
            f"| {c['pct_itm']:.1f}% ITM | {c['dte']}d left "
            f"| px {c['current_price']:.2f} "
            f"| orig prem ${c['orig_premium']:.0f} "
            f"| {c['contracts']}x"
        )


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

        # Estimated annual dividends per holding
        try:
            ret_rows = strat.load_retirement_positions()
            total_est_div = 0.0
            div_lines = []
            for r in ret_rows:
                tkr   = (r.get("ticker") or "").strip().upper()
                acct  = (r.get("account") or "").strip().upper()
                if acct not in (IRA, ROTH) or not tkr:
                    continue
                try:
                    sh  = float(r.get("shares") or 0)
                    px  = float(r.get("current_price") or 0)
                    yld = float(RETIREMENT_STOCK_YIELDS.get(tkr, 0.0))
                    if sh > 0 and px > 0 and yld > 0:
                        est_div = sh * px * yld
                        total_est_div += est_div
                        div_lines.append(f"    {acct} {tkr}: ~${est_div:,.0f}/yr ({yld*100:.1f}% yield)")
                except Exception:
                    pass
            if div_lines:
                print(f"  📆 Est. annual dividends: ~${total_est_div:,.0f}/yr")
                for dl in div_lines:
                    print(dl)
        except Exception as e:
            log.warning("Retirement dividend display failed: %s", e)
    if ret_flagged:
        print(f"  ⚠️ Breakeven-only flagged: {', '.join(ret_flagged)}")

    indiv_stock_mv = float(mv_stock.get(INDIVIDUAL, 0.0))
    print("\n📦 STOCK CAPS (non-wheel positions)")
    print(
        f"  INDIVIDUAL   MV ${indiv_stock_mv:>8,.0f} / ${float(INDIVIDUAL_STOCK_CAP):>8,.0f}"
        f"  | Remaining ${max(float(INDIVIDUAL_STOCK_CAP) - indiv_stock_mv, 0.0):>8,.0f}"
    )
    for acct in (IRA, ROTH):
        acct_stock_mv = float(mv_ret.get(acct, 0.0)) + float(mv_stock.get(acct, 0.0))
        cap = float(RETIREMENT_STOCK_CAPS.get(acct, 0))
        print(
            f"  {acct:<12} MV ${acct_stock_mv:>8,.0f} / ${cap:>8,.0f}"
            f"  | Remaining ${max(cap - acct_stock_mv, 0.0):>8,.0f}"
        )
