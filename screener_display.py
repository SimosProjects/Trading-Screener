"""screener_display.py

Terminal output and Discord alert formatting.
No trading logic lives here — pure display and notification.
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
    WEBHOOK_MARKET_URL,
    WEBHOOK_STOCKS_URL,
    WEBHOOK_OPTIONS_URL,
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

_ACCT_LABEL = {INDIVIDUAL: "INDV", IRA: "IRA", ROTH: "ROTH"}


# ============================================================
# Discord helpers
# ============================================================

def _send(url: str, msg: str) -> None:
    """Post msg to a Discord webhook URL. Silently skips if no URL is set."""
    url = (url or "").strip()
    if not url or not url.startswith("http"):
        return
    try:
        import requests
        requests.post(url, json={"content": msg[:1990]}, timeout=10)
    except Exception as e:
        log.warning("Discord send failed (%s): %s", url[:40], e)


def send_discord(msg: str) -> None:
    """Legacy single-channel send — posts to WEBHOOK_URL if set."""
    _send(WEBHOOK_URL, msg)


def _spy_ma_flags(mkt: Dict) -> str:
    flags = []
    if mkt.get("spy_above_200"):
        flags.append("200")
    if mkt.get("spy_above_50"):
        flags.append("50")
    if mkt.get("spy_above_21"):
        flags.append("21")
    if not flags:
        return "❌ below all MAs"
    return "✅" + "/".join(flags)


def _vix_emoji(mkt: Dict) -> str:
    vix = float(mkt.get("vix_close") or 99.0)
    if vix < 18:
        return "🟢"
    if vix < 25:
        return "🟡"
    return "🔴"


# ============================================================
# Three-channel Discord alert builders
# ============================================================

def build_market_alert(mkt: Dict, trading_on: bool, retire_on: bool,
                       ret_by_key: dict, mv_stock: Dict[str, float],
                       wheel_mv: float) -> str:
    """
    #screener-market — regime, holdings P/L, wheel exposure.
    Posts every run regardless of trading activity.
    """
    today = dt.date.today()
    lines: List[str] = []

    # Header
    spy_flags = _spy_ma_flags(mkt)
    qqq_flag  = "✅50" if mkt.get("qqq_above_50") else "❌50"
    vix_emoji = _vix_emoji(mkt)
    regime_emoji = {"MOMENTUM": "🚀", "STRONG_BULL": "🐂", "BULL": "📈",
                    "NEUTRAL": "😐", "RISK_OFF": "🛡️"}.get(
                        mkt.get("regime", ""), "📊")
    lines.append(f"📅 {today.isoformat()}  {regime_emoji} {mkt.get('regime', 'UNKNOWN')}")
    lines.append(
        f"SPY {mkt['spy_close']:.2f} {spy_flags} | "
        f"QQQ {mkt['qqq_close']:.2f} {qqq_flag} | "
        f"VIX {mkt['vix_close']:.2f} {vix_emoji}"
    )
    t_flag = "🟢" if trading_on else "🔴"
    r_flag = "🟢" if retire_on else "🔴"
    lines.append(f"{t_flag} Swing {'ON' if trading_on else 'OFF'}  {r_flag} Retire {'ON' if retire_on else 'OFF'}")

    # Holdings P/L
    try:
        stock_rows = strat.load_csv_rows(strat.STOCK_POSITIONS_FILE)
        open_rows  = [r for r in stock_rows if (r.get("status") or "").upper() == "OPEN"]
        import sys as _sys
        _whl     = _sys.modules.get("wheel")
        lot_rows = _whl.get_open_lots() if _whl else []
        ret_rows = strat.load_retirement_positions()

        all_tkrs = list({
            (r.get("ticker") or "").strip().upper()
            for r in open_rows + lot_rows + ret_rows
            if (r.get("ticker") or "").strip()
        })
        px_live = strat.live_prices(all_tkrs) if all_tkrs else {}

        # Build per-account buckets
        buckets: Dict[str, list] = {}
        for r in open_rows:
            tkr   = (r.get("ticker") or "").strip().upper()
            acct  = (r.get("account") or INDIVIDUAL).strip().upper()
            sh    = float(r.get("shares") or 0)
            entry = float(r.get("entry_price") or 0)
            cur   = px_live.get(tkr, 0.0)
            stop  = float(r.get("stop_price") or 0)
            stop_type = (r.get("stop_type") or "FIXED").upper()
            trail_tag = " 🔄" if stop_type == "TRAIL_EMA8" else ""
            pnl   = (cur - entry) * sh if cur > 0 else 0.0
            pct   = (cur - entry) / entry * 100 if entry > 0 and cur > 0 else 0.0
            buckets.setdefault(acct, []).append(
                {"tkr": tkr, "sh": sh, "entry": entry, "cur": cur,
                 "stop": stop, "pnl": pnl, "pct": pct,
                 "src": "SWING", "trail": trail_tag})
        for lot in lot_rows:
            tkr   = (lot.get("ticker") or "").strip().upper()
            acct  = (lot.get("account") or INDIVIDUAL).strip().upper()
            sh    = float(lot.get("shares") or 0)
            cb    = float(lot.get("cost_basis") or 0)
            entry = cb / sh if sh > 0 else 0.0
            cur   = px_live.get(tkr, 0.0)
            pnl   = (cur * sh - cb) if cur > 0 and sh > 0 and cb > 0 else 0.0
            pct   = (cur - entry) / entry * 100 if entry > 0 and cur > 0 else 0.0
            buckets.setdefault(acct, []).append(
                {"tkr": tkr, "sh": sh, "entry": entry, "cur": cur,
                 "stop": 0.0, "pnl": pnl, "pct": pct, "src": "WHEEL", "trail": ""})
        for r in ret_rows:
            tkr   = (r.get("ticker") or "").strip().upper()
            acct  = (r.get("account") or "").strip().upper()
            sh    = float(r.get("shares") or 0)
            entry = float(r.get("entry_price") or 0)
            cur   = px_live.get(tkr, float(r.get("current_price") or 0))
            pnl   = (cur - entry) * sh if cur > 0 else 0.0
            pct   = (cur - entry) / entry * 100 if entry > 0 and cur > 0 else 0.0
            buckets.setdefault(acct, []).append(
                {"tkr": tkr, "sh": sh, "entry": entry, "cur": cur,
                 "stop": 0.0, "pnl": pnl, "pct": pct, "src": "RETIRE", "trail": ""})

        if buckets:
            lines.append("📌 HOLDINGS")
            for acct in (INDIVIDUAL, IRA, ROTH):
                rows = buckets.get(acct)
                if not rows:
                    continue
                acct_mv  = sum(r["cur"] * r["sh"] for r in rows if r["cur"] > 0)
                acct_pnl = sum(r["pnl"] for r in rows)
                sign     = "+" if acct_pnl >= 0 else ""
                lines.append(f"{_ACCT_LABEL[acct]} MV ${acct_mv:,.0f} | P/L {sign}${acct_pnl:,.0f}")
                for r in rows:
                    tag  = f"[{r['src'][0]}]"   # [S]wing [W]heel [R]etire
                    if r["cur"] <= 0:
                        lines.append(f"  {r['tkr']:<5} {r['sh']:.0f}sh @{r['entry']:.2f} — n/a {tag}")
                    else:
                        sign2 = "+" if r["pnl"] >= 0 else ""
                        stop_str = f" stop ${r['stop']:.2f}{r['trail']}" if r["stop"] > 0 else ""
                        lines.append(
                            f"  {r['tkr']:<5} {r['sh']:.0f}sh "
                            f"@{r['entry']:.2f}→{r['cur']:.2f} "
                            f"{sign2}${r['pnl']:,.0f} ({sign2}{r['pct']:.1f}%)"
                            f"{stop_str} {tag}"
                        )
    except Exception as e:
        log.warning("Market alert holdings block failed: %s", e)

    # Wheel exposure
    lines.append("💼 EXPOSURE")
    try:
        for acct in (INDIVIDUAL, IRA, ROTH):
            label = _ACCT_LABEL[acct]
            exp   = compute_wheel_exposure(today, acct)
            rem   = compute_week_remaining(today, acct)
            lines.append(
                f"  {label} Whl ${exp['total_exposure']:,.0f}/${exp['cap']:,.0f} "
                f"wk rem ${rem:,.0f}"
            )
    except Exception as e:
        log.warning("Market alert exposure block failed: %s", e)

    return "\n".join(lines)


def build_stocks_alert(
    planned_stocks: List[dict],
    stock_closes: List[str],
    ret_stopped: List[str],
    ret_targets: List[str],
    watch: List[dict],
    regime: str = "",
) -> str:
    """
    #screener-stocks — new trades, closes, watchlist.
    Returns empty string if nothing to report.
    """
    today = dt.date.today()
    lines: List[str] = []

    has_content = bool(planned_stocks or stock_closes or ret_stopped or ret_targets)
    if not has_content and not watch:
        return ""

    lines.append(f"📈 STOCKS — {today.isoformat()}" + (f"  [{regime}]" if regime else ""))

    # Closes / stops first (most urgent)
    if ret_stopped:
        lines.append(f"🛑 Retirement stops: {', '.join(ret_stopped)}")
    if ret_targets:
        lines.append(f"✅ Retirement targets: {', '.join(ret_targets)}")
    if stock_closes:
        closed_fmt = []
        for s in stock_closes:
            if ":" in s:
                acct_raw, tkr = s.split(":", 1)
                closed_fmt.append(f"{tkr} [{_ACCT_LABEL.get(acct_raw.strip().upper(), acct_raw)}]")
            else:
                closed_fmt.append(s)
        lines.append(f"📉 Closed: {', '.join(closed_fmt)}")

    # New trades
    if planned_stocks:
        lines.append("🆕 NEW TRADES")
        for p in planned_stocks:
            acct      = _ACCT_LABEL.get((p.get("account") or INDIVIDUAL).strip().upper(), "?")
            tkr       = p.get("ticker", "?")
            sig       = p.get("signal", "")
            entry     = float(p.get("entry_price", 0))
            stop      = float(p.get("stop_price", 0))
            tgt       = float(p.get("target_price", 0))
            shares    = int(p.get("shares", 0))
            pos_val   = float(p.get("pos_value", 0))
            risk      = float(p.get("risk_dollars", 0))
            stop_type = p.get("stop_type", "FIXED")
            upside    = (tgt - entry) / entry * 100 if entry > 0 and tgt > 0 else 0.0
            stop_pct  = (entry - stop) / entry * 100 if entry > 0 and stop > 0 else 0.0
            sig_short = {"EMA8_PULLBACK": "EMA8pb", "PULLBACK": "PB", "BREAKOUT": "BO"}.get(sig, sig)
            trail_tag = " 🔄trail" if stop_type == "TRAIL_EMA8" else ""
            lines.append(
                f"  [{acct}] {tkr} {sig_short}{trail_tag}\n"
                f"    {shares}sh @${entry:.2f} = ${pos_val:,.0f} | "
                f"stop ${stop:.2f} (-{stop_pct:.1f}%) | "
                f"tgt ${tgt:.2f} (+{upside:.1f}%) | "
                f"risk ${risk:,.0f}"
            )

    # Watchlist (condensed)
    if watch:
        watch_items = [f"{w['ticker']} {w['note']}" for w in watch[:8]]
        lines.append(f"👀 Watch: {', '.join(watch_items)}")

    return "\n".join(lines)


def build_options_alert(
    new_csps: List[dict],
    new_ccs: List[dict],
    csp_tp: List[str],
    cc_tp: List[str],
    csp_exp: List[str],
    csp_asn: List[str],
    cc_exp: List[str],
    cc_call: List[str],
    early_asn: List[str],
    csp_roll: List[dict],
) -> str:
    """
    #screener-options — CSP/CC positions, new orders, maintenance events.
    Returns empty string if nothing to report.
    """
    today = dt.date.today()
    lines: List[str] = []

    maint_items = (csp_tp or cc_tp or csp_exp or csp_asn or
                   cc_exp or cc_call or early_asn or csp_roll)
    has_content = bool(new_csps or new_ccs or maint_items)
    if not has_content:
        return ""

    lines.append(f"🧾 OPTIONS — {today.isoformat()}")

    # Maintenance events
    maint: List[str] = []
    if early_asn:   maint.append(f"⚠️ Early asn: {', '.join(early_asn[:4])}")
    if csp_asn:     maint.append(f"📥 Assigned: {', '.join(csp_asn[:4])}")
    if csp_tp:      maint.append(f"✅ CSP TP: {', '.join(csp_tp[:4])}")
    if cc_tp:       maint.append(f"✅ CC TP: {', '.join(cc_tp[:4])}")
    if csp_exp:     maint.append(f"CSP exp: {', '.join(csp_exp[:4])}")
    if cc_exp:      maint.append(f"CC exp: {', '.join(cc_exp[:4])}")
    if cc_call:     maint.append(f"📤 Called away: {', '.join(cc_call[:4])}")
    if maint:
        lines.append("— Events —")
        lines.extend(maint)

    # Open CSPs
    try:
        csp_rows  = strat.load_csv_rows(CSP_POSITIONS_FILE)
        open_csps = [
            r for r in csp_rows
            if (r.get("status") or "").upper() == "OPEN"
            and not (
                (r.get("expiry") or "").strip()
                and dt.date.fromisoformat((r.get("expiry") or "").strip()) < today
            )
        ]
        if open_csps:
            lines.append("🧾 OPEN CSPs")
            for r in open_csps[:10]:
                acct   = _ACCT_LABEL.get((r.get("account") or INDIVIDUAL).strip().upper(), "?")
                tkr    = (r.get("ticker") or "").strip().upper()
                strike = r.get("strike") or ""
                exp    = (r.get("expiry") or "").replace("2026-", "").replace("2025-", "")
                prem   = r.get("premium") or r.get("est_premium") or ""
                try:
                    prem_f = f"${float(prem):,.0f}" if prem else ""
                except Exception:
                    prem_f = str(prem)
                dte_str = ""
                try:
                    if r.get("expiry"):
                        dte = (dt.date.fromisoformat(r["expiry"]) - today).days
                        dte_str = f"{dte}d"
                except Exception:
                    pass
                lines.append(f"  [{acct}] {tkr} {strike}P {exp} {prem_f} {dte_str}")
    except Exception as e:
        log.warning("Options alert CSP block failed: %s", e)

    # Open CCs
    try:
        cc_rows  = strat.load_csv_rows(CC_POSITIONS_FILE)
        open_ccs = [r for r in cc_rows if (r.get("status") or "").upper() == "OPEN"]
        if open_ccs:
            lines.append("📞 OPEN CCs")
            for r in open_ccs[:10]:
                acct      = _ACCT_LABEL.get((r.get("account") or INDIVIDUAL).strip().upper(), "?")
                tkr       = (r.get("ticker") or "").strip().upper()
                strike    = r.get("strike") or ""
                exp       = (r.get("expiry") or "").replace("2026-", "").replace("2025-", "")
                prem      = r.get("premium") or ""
                contracts = r.get("contracts") or ""
                try:
                    prem_f = f"${float(prem):,.0f}" if prem else ""
                except Exception:
                    prem_f = str(prem)
                dte_str = itm_str = ""
                try:
                    if r.get("expiry"):
                        dte = (dt.date.fromisoformat(r["expiry"]) - today).days
                        dte_str = f"{dte}d"
                except Exception:
                    pass
                try:
                    px_map = strat.live_prices([tkr])
                    cur    = px_map.get(tkr, 0.0)
                    s      = float(strike) if strike else 0.0
                    if cur > 0 and s > 0:
                        pct = (cur - s) / s * 100.0
                        itm_str = f"🔴{pct:+.1f}%" if cur >= s else f"🟢{pct:+.1f}%"
                except Exception:
                    pass
                lines.append(
                    f"  [{acct}] {tkr} {strike}C {exp} {prem_f} {contracts}x {dte_str} {itm_str}"
                )
    except Exception as e:
        log.warning("Options alert CC block failed: %s", e)

    # Roll candidates
    if csp_roll:
        lines.append("🔄 CSP ROLLS")
        for c in csp_roll[:4]:
            lines.append(
                f"  [{c['account']}] {c['ticker']} {c['strike']:.0f}P "
                f"{c['expiry']} | {c['pct_itm']:.1f}% ITM | {c['dte']}d"
            )

    # New CSPs
    if new_csps:
        lines.append("🆕 NEW CSPs")
        for x in new_csps[:8]:
            acct = _ACCT_LABEL.get((x.get("account") or INDIVIDUAL).strip().upper(), "?")
            exp  = (x.get("expiry") or "").replace("2026-", "").replace("2025-", "")
            lines.append(
                f"  [{acct}] {x['ticker']} {x['strike']:.0f}P {exp} "
                f"~${x['est_premium']:.0f} | ${x['cash_reserved']:,.0f} cash"
            )

    # New CCs
    if new_ccs:
        lines.append("📞 NEW CCs")
        for x in new_ccs[:8]:
            acct = _ACCT_LABEL.get((x.get("account") or INDIVIDUAL).strip().upper(), "?")
            exp  = (x.get("expiry") or "").replace("2026-", "").replace("2025-", "")
            lines.append(
                f"  [{acct}] {x['ticker']} {x['strike']:.0f}C {exp} "
                f"~${float(x.get('credit_mid', 0)) * 100:.0f}"
            )

    return "\n".join(lines)


def send_market_alert(msg: str) -> None:
    _send(WEBHOOK_MARKET_URL or WEBHOOK_URL, msg)


def send_stocks_alert(msg: str) -> None:
    _send(WEBHOOK_STOCKS_URL or WEBHOOK_URL, msg)


def send_options_alert(msg: str) -> None:
    _send(WEBHOOK_OPTIONS_URL or WEBHOOK_URL, msg)


# ── Legacy single-message builder (kept for backward compat) ─────────────────
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
    ret_targets: List[str] = [],
    early_asn: List[str] = [],
    csp_roll: List[dict] = [],
) -> str:
    """Kept for backward compatibility — screener.py now calls the three
    channel-specific builders directly."""
    return build_market_alert(mkt, trading_on, True, {}, {}, 0.0)


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
    ret_targets: List[str] = [],
    early_asn: List[str] = [],
    csp_roll: List[dict] = [],
) -> str:
    today = dt.date.today()
    lines: List[str] = []

    # --- Header ---
    lines.append(f"📅 {today.isoformat()} Screener")

    spy_flags = _spy_ma_flags(mkt)
    qqq_flag  = "✅50" if mkt.get("qqq_above_50") else "❌50"
    vix_emoji = _vix_emoji(mkt)
    lines.append(
        f"SPY {mkt['spy_close']:.2f} {spy_flags} | "
        f"QQQ {mkt['qqq_close']:.2f} {qqq_flag} | "
        f"VIX {mkt['vix_close']:.2f} {vix_emoji}"
    )

    t_flag  = "🟢" if trading_on else "🔴"
    lines.append(f"{t_flag} Trading {'ON' if trading_on else 'OFF'}")

    # --- Open holdings ---
    try:
        stock_rows = strat.load_csv_rows(strat.STOCK_POSITIONS_FILE)
        open_rows  = [r for r in stock_rows if (r.get("status") or "").upper() == "OPEN"]

        import sys as _sys
        _whl = _sys.modules.get("wheel")
        lot_rows = _whl.get_open_lots() if _whl else []

        holdings: List[dict] = []

        # Use live_prices for all open positions — fast_info gives ~15min
        # delayed intraday price rather than yesterday's close.
        all_tkrs = list({
            (r.get("ticker") or "").strip().upper() for r in open_rows
            if (r.get("ticker") or "").strip()
        } | {
            (lot.get("ticker") or "").strip().upper() for lot in lot_rows
            if (lot.get("ticker") or "").strip()
        })
        px_live = strat.live_prices(all_tkrs) if all_tkrs else {}

        for r in open_rows:
            tkr   = (r.get("ticker") or "").strip().upper()
            acct  = (r.get("account") or INDIVIDUAL).strip().upper()
            sh    = float(r.get("shares") or 0)
            entry = float(r.get("entry_price") or r.get("cost_basis") or 0)
            cur   = px_live.get(tkr, 0.0)
            pnl   = (cur - entry) * sh if cur > 0 else 0.0
            pct   = (cur - entry) / entry * 100 if (entry > 0 and cur > 0) else 0.0
            holdings.append({"acct": acct, "tkr": tkr, "sh": sh, "entry": entry,
                              "cur": cur, "pnl": pnl, "pct": pct, "src": "STOCK"})

        for lot in lot_rows:
            tkr   = (lot.get("ticker") or "").strip().upper()
            acct  = (lot.get("account") or INDIVIDUAL).strip().upper()
            sh    = float(lot.get("shares") or 0)
            cb    = float(lot.get("cost_basis") or 0)
            entry = cb / sh if sh > 0 else 0.0
            cur   = px_live.get(tkr, 0.0)
            pnl   = (cur - entry) * sh if cur > 0 else 0.0
            pct   = (cur - entry) / entry * 100 if (entry > 0 and cur > 0) else 0.0
            holdings.append({"acct": acct, "tkr": tkr, "sh": sh, "entry": entry,
                              "cur": cur, "pnl": pnl, "pct": pct, "src": "WHEEL"})

        if holdings:
            lines.append("📌 HOLDINGS")
            by_acct: Dict[str, list] = {}
            for h in holdings:
                by_acct.setdefault(h["acct"], []).append(h)
            for acct in (INDIVIDUAL, IRA, ROTH):
                rows = by_acct.get(acct)
                if not rows:
                    continue
                acct_mv  = sum(r["cur"] * r["sh"] for r in rows if r["cur"] > 0)
                acct_pnl = sum(r["pnl"] for r in rows)
                sign     = "+" if acct_pnl >= 0 else ""
                lines.append(
                    f"{_ACCT_LABEL[acct]} MV ${acct_mv:,.0f} P/L {sign}${acct_pnl:,.0f}"
                )
                for r in rows:
                    w_tag = "[W]" if r["src"] == "WHEEL" else ""
                    if r["cur"] <= 0:
                        lines.append(
                            f"  {r['tkr']:<5} {r['sh']:.0f}sh "
                            f"@{r['entry']:.2f}→n/a {w_tag}"
                        )
                    else:
                        sign2 = "+" if r["pnl"] >= 0 else ""
                        lines.append(
                            f"  {r['tkr']:<5} {r['sh']:.0f}sh "
                            f"@{r['entry']:.2f}→{r['cur']:.2f} "
                            f"{sign2}${r['pnl']:,.0f} ({sign2}{r['pct']:.1f}%) {w_tag}"
                        )
    except Exception as e:
        log.warning("Discord holdings block failed: %s", e)

    # --- Open CSPs ---
    try:
        csp_rows  = strat.load_csv_rows(CSP_POSITIONS_FILE)
        open_csps = [
            r for r in csp_rows
            if (r.get("status") or "").upper() == "OPEN"
            and not (
                (r.get("expiry") or "").strip()
                and dt.date.fromisoformat((r.get("expiry") or "").strip()) < today
            )
        ]
        if open_csps:
            lines.append("🧾 CSPs")
            for r in open_csps[:12]:
                acct   = _ACCT_LABEL.get((r.get("account") or INDIVIDUAL).strip().upper(), "?")
                tkr    = (r.get("ticker") or "").strip().upper()
                strike = r.get("strike") or ""
                exp    = (r.get("expiry") or "").replace("2026-", "").replace("2025-", "")
                prem   = r.get("premium") or r.get("est_premium") or ""
                try:
                    prem_f = f"${float(prem):,.0f}" if prem else ""
                except Exception:
                    prem_f = str(prem)
                dte_str = ""
                try:
                    if r.get("expiry"):
                        dte = (dt.date.fromisoformat(r["expiry"]) - today).days
                        dte_str = f"{dte}d"
                except Exception:
                    pass
                lines.append(f"  [{acct}] {tkr} {strike}P {exp} {prem_f} {dte_str}")
    except Exception as e:
        log.warning("Discord CSP block failed: %s", e)

    # --- Open CCs ---
    try:
        cc_rows  = strat.load_csv_rows(CC_POSITIONS_FILE)
        open_ccs = [r for r in cc_rows if (r.get("status") or "").upper() == "OPEN"]
        if open_ccs:
            lines.append("📞 CCs")
            for r in open_ccs[:12]:
                acct      = _ACCT_LABEL.get((r.get("account") or INDIVIDUAL).strip().upper(), "?")
                tkr       = (r.get("ticker") or "").strip().upper()
                strike    = r.get("strike") or ""
                exp       = (r.get("expiry") or "").replace("2026-", "").replace("2025-", "")
                prem      = r.get("premium") or ""
                contracts = r.get("contracts") or ""
                try:
                    prem_f = f"${float(prem):,.0f}" if prem else ""
                except Exception:
                    prem_f = str(prem)
                dte_str = ""
                try:
                    if r.get("expiry"):
                        dte = (dt.date.fromisoformat(r["expiry"]) - today).days
                        dte_str = f"{dte}d"
                except Exception:
                    pass
                # ITM/OTM — use live price for accuracy
                itm_str = ""
                try:
                    px_map = strat.live_prices([tkr])
                    cur    = px_map.get(tkr, 0.0)
                    s      = float(strike) if strike else 0.0
                    if cur > 0 and s > 0:
                        pct = (cur - s) / s * 100.0
                        itm_str = (f"🔴{pct:+.1f}%" if cur >= s else f"🟢{pct:+.1f}%")
                except Exception:
                    pass
                lines.append(
                    f"  [{acct}] {tkr} {strike}C {exp} {prem_f} {contracts}x {dte_str} {itm_str}"
                )
    except Exception as e:
        log.warning("Discord CC block failed: %s", e)

    # --- CC roll alerts ---
    try:
        cc_rows_all = strat.load_csv_rows(CC_POSITIONS_FILE)
        roll_flags  = []
        for r in cc_rows_all:
            if (r.get("status") or "").upper() != "OPEN":
                continue
            tkr = (r.get("ticker") or "").strip().upper()
            try:
                strike = float(r.get("strike") or 0)
                if strike <= 0:
                    continue
                px_map = strat.live_prices([tkr])
                cur    = px_map.get(tkr, 0.0)
                if cur <= 0:
                    continue
                if cur / strike >= float(CC_ROLL_SIGNAL_THRESHOLD):
                    pct = (cur - strike) / strike * 100.0
                    flag = "🔴 ITM" if pct > 0 else "🟡 near"
                    roll_flags.append(f"  ⚠️ {flag} {tkr} {strike:.0f}C {pct:+.1f}% — review at terminal")
            except Exception:
                continue
        for line in roll_flags:
            lines.append(line)
    except Exception as e:
        log.warning("Discord CC roll block failed: %s", e)

    # --- Maintenance events ---
    maint: List[str] = []
    if ret_stopped:  maint.append(f"🛑 Ret stops: {', '.join(ret_stopped[:6])}")
    if ret_targets:  maint.append(f"✅ Ret targets: {', '.join(ret_targets[:6])}")
    if early_asn:    maint.append(f"⚠️ Early asn: {', '.join(early_asn[:6])}")
    if csp_asn:      maint.append(f"📥 CSP asn: {', '.join(csp_asn[:6])}")
    if csp_tp:       maint.append(f"✅ CSP TP: {', '.join(csp_tp[:6])}")
    if csp_exp:      maint.append(f"CSP exp: {', '.join(csp_exp[:6])}")
    if cc_tp:        maint.append(f"✅ CC TP: {', '.join(cc_tp[:6])}")
    if cc_exp:       maint.append(f"CC exp: {', '.join(cc_exp[:6])}")
    if cc_call:      maint.append(f"📤 Called away: {', '.join(cc_call[:6])}")
    if stock_closes:
        closed_fmt = []
        for s in stock_closes[:6]:
            if ":" in s:
                acct_raw, tkr = s.split(":", 1)
                closed_fmt.append(f"{tkr} [{_ACCT_LABEL.get(acct_raw.strip().upper(), acct_raw)}]")
            else:
                closed_fmt.append(s)
        maint.append(f"📉 Closed: {', '.join(closed_fmt)}")
    if maint:
        lines.append("— Events —")
        lines.extend(maint)

    # --- New stock trades (detailed, separate from maint) ---
    if planned_stocks:
        lines.append("📈 NEW TRADES")
        for p in planned_stocks[:12]:
            acct     = _ACCT_LABEL.get((p.get("account") or INDIVIDUAL).strip().upper(), "?")
            tkr      = p.get("ticker", "?")
            sig      = p.get("signal", "")
            entry    = float(p.get("entry_price", 0))
            stop     = float(p.get("stop_price", 0))
            tgt      = float(p.get("target_price", 0))
            shares   = int(p.get("shares", 0))
            pos_val  = float(p.get("pos_value", 0))
            risk     = float(p.get("risk_dollars", 0))
            upside   = (tgt - entry) / entry * 100 if entry > 0 and tgt > 0 else 0.0
            stop_pct = (entry - stop) / entry * 100 if entry > 0 and stop > 0 else 0.0
            sig_short = {"EMA8_PULLBACK": "EMA8pb", "PULLBACK": "PB", "BREAKOUT": "BO"}.get(sig, sig)
            lines.append(
                f"  [{acct}] {tkr} {sig_short} | "
                f"{shares}sh @${entry:.2f} = ${pos_val:,.0f} | "
                f"stop ${stop:.2f} (-{stop_pct:.1f}%) | "
                f"tgt ${tgt:.2f} (+{upside:.1f}%) | "
                f"risk ${risk:,.0f}"
            )

    # --- New CSPs ---
    if new_csps:
        lines.append("🆕 CSPs")
        for x in new_csps[:10]:
            acct = _ACCT_LABEL.get((x.get("account") or INDIVIDUAL).strip().upper(), "?")
            exp  = (x.get("expiry") or "").replace("2026-", "").replace("2025-", "")
            lines.append(
                f"  [{acct}] {x['ticker']} {x['strike']:.0f}P {exp} "
                f"~${x['est_premium']:.0f} ${x['cash_reserved']:,.0f}"
            )
    else:
        lines.append("🆕 CSPs: none")

    # --- New CCs ---
    if new_ccs:
        lines.append("📞 New CCs")
        for x in new_ccs[:10]:
            acct = _ACCT_LABEL.get((x.get("account") or INDIVIDUAL).strip().upper(), "?")
            exp  = (x.get("expiry") or "").replace("2026-", "").replace("2025-", "")
            lines.append(
                f"  [{acct}] {x['ticker']} {x['strike']:.0f}C {exp} "
                f"~${float(x.get('credit_mid', 0)) * 100:.0f}"
            )
    else:
        lines.append("📞 New CCs: none")

    # --- Wheel exposure + stock caps ---
    lines.append("💼 Exposure")
    try:
        for acct in (INDIVIDUAL, IRA, ROTH):
            label = _ACCT_LABEL[acct]
            exp   = compute_wheel_exposure(today, acct)
            rem   = compute_week_remaining(today, acct)
            cap   = float(RETIREMENT_STOCK_CAPS.get(acct, INDIVIDUAL_STOCK_CAP))
            lines.append(
                f"  {label} Whl ${exp['total_exposure']:,.0f}/${exp['cap']:,.0f} "
                f"wk rem ${rem:,.0f} | Stk cap ${cap:,.0f}"
            )
    except Exception as e:
        log.warning("Discord exposure block failed: %s", e)

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
        acct_pnl = sum(r["pnl"] for r in rows)
        acct_mv  = sum((r["cur"] * r["shares"]) for r in rows if r["cur"] > 0)
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
        csp_rows  = strat.load_csv_rows(CSP_POSITIONS_FILE)
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
    """Print all open covered calls with live ITM/OTM status."""
    try:
        cc_rows  = strat.load_csv_rows(CC_POSITIONS_FILE)
        open_ccs = [r for r in cc_rows if (r.get("status") or "").upper() == "OPEN"]
        if not open_ccs:
            return

        print("\n📞 OPEN CC POSITIONS")
        for r in open_ccs:
            tkr       = (r.get("ticker")    or "").strip().upper()
            acct      = (r.get("account")   or "").strip().upper()
            exp       = (r.get("expiry")    or "").strip()
            strike    = r.get("strike")     or ""
            prem      = r.get("premium")    or ""
            contracts = r.get("contracts")  or ""

            dte_str = ""
            try:
                if exp:
                    dte = (dt.date.fromisoformat(exp) - today).days
                    dte_str = f"{dte}d"
            except Exception:
                pass

            status_str = ""
            cur = px.get(tkr)
            try:
                if cur and cur > 0 and strike:
                    s   = float(strike)
                    pct = (cur - s) / s * 100.0 if s > 0 else 0.0
                    status_str = (f"🔴 ITM {pct:+.1f}%" if cur >= s else f"🟢 OTM {pct:+.1f}%")
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
    Flag open CCs near or through their strike and prompt for action.
    """
    import datetime as _dt
    if today is None:
        today = _dt.date.today()

    try:
        cc_rows    = strat.load_csv_rows(CC_POSITIONS_FILE)
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
                    "pct":    pct_to_strike * 100,
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
            pct    = cand["pct"]
            exp    = cand["exp"]
            r      = cand["row"]

            flag      = "🔴 ITM " if pct < 0 else "🟡 near"
            direction = "ITM"      if pct < 0 else "OTM"

            print(f"\n  {flag} {tkr:<6} {strike:.0f}C {exp} | Now {cur:.2f} ({abs(pct):.1f}% {direction})")
            print(f"    1  Roll up & out  (new strike via ATR tier, later expiry)")
            print(f"    2  Roll out only  (same strike {strike:.0f}C, later expiry)")
            print(f"    3  Close CC only  (buy to close, keep shares, fresh CC next run)")
            print(f"    4  Close CC + exit shares  (unwind entire position, book P&L)")
            print(f"    5  Skip           (do nothing)")

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
                        _whl   = sys.modules.get("wheel")
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

            roll_up   = (choice == "1")
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
        print(
            f"  {acct:<10} "
            f"${exp['total_exposure']:>8,.0f} / ${exp['cap']:>8,.0f}  "
            f"| wk target ${exp['weekly_target']:>7,.0f}  "
            f"| wk rem ${rem:>7,.0f}"
        )

    mv_ret       = strat.retirement_market_value_by_account(ret_by_key)
    total_ret_mv = sum(mv_ret.get(a, 0.0) for a in (IRA, ROTH))
    total_ret_mv += sum(mv_stock.get(a, 0.0) for a in (IRA, ROTH))

    if total_ret_mv > 0 or ret_flagged:
        print(f"\n🏦 RETIREMENT MV: ${total_ret_mv:,.0f}")
        for acct in (IRA, ROTH):
            mv = mv_ret.get(acct, 0.0) + mv_stock.get(acct, 0.0)
            if mv > 0:
                print(f"  {acct}: ${mv:,.0f}")

        try:
            ret_rows      = strat.load_retirement_positions()
            total_est_div = 0.0
            div_lines     = []
            for r in ret_rows:
                tkr  = (r.get("ticker")  or "").strip().upper()
                acct = (r.get("account") or "").strip().upper()
                if acct not in (IRA, ROTH) or not tkr:
                    continue
                try:
                    sh  = float(r.get("shares") or 0)
                    px  = float(r.get("current_price") or 0)
                    yld = float(RETIREMENT_STOCK_YIELDS.get(tkr, 0.0))
                    if sh > 0 and px > 0 and yld > 0:
                        est_div = sh * px * yld
                        total_est_div += est_div
                        div_lines.append(
                            f"    {acct} {tkr}: ~${est_div:,.0f}/yr ({yld*100:.1f}% yield)"
                        )
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

    # Swing position summary — risk-based sizing, no hard cap
    try:
        swing_rows = [r for r in strat.load_stock_positions()
                      if (r.get("status") or "").upper() == "OPEN"
                      and (r.get("account") or "").upper() == INDIVIDUAL]
        swing_count  = len(swing_rows)
        swing_mv     = float(mv_stock.get(INDIVIDUAL, 0.0))
        total_risk   = sum(
            (float(r.get("entry_price") or 0) - float(r.get("stop_price") or 0))
            * float(r.get("shares") or 0)
            for r in swing_rows
        )
        trail_count = sum(1 for r in swing_rows
                          if (r.get("stop_type") or "").upper() == "TRAIL_EMA8")
        fixed_count = swing_count - trail_count
        print(f"\n📦 SWING POSITIONS  ({swing_count} open)")
        print(f"  MV ${swing_mv:,.0f}  |  Total risk deployed ${total_risk:,.0f}"
              f"  |  {trail_count} trailing  {fixed_count} fixed")
    except Exception as e:
        log.warning("Swing summary failed: %s", e)

    for acct in (IRA, ROTH):
        acct_stock_mv = float(mv_ret.get(acct, 0.0)) + float(mv_stock.get(acct, 0.0))
        cap           = float(RETIREMENT_STOCK_CAPS.get(acct, 0))
        print(
            f"  {acct:<12} MV ${acct_stock_mv:>8,.0f} / ${cap:>8,.0f}"
            f"  | Remaining ${max(cap - acct_stock_mv, 0.0):>8,.0f}"
        )
