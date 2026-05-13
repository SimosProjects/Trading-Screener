"""screener.py

Daily screener orchestrator.

Sequence:
  1. Warm data cache (one batch download)
  2. Market context + regime flags
  3. File / position maintenance (expirations, lots, links)
  4. Print open holdings
  5. Close stock positions at stop/target
  6. Scan for new stock entries → plan + execute
  7. Scan for new CSP candidates → plan + execute
  8. Plan CC ideas from open lots → execute
  9. Backfill wheel events if needed
 10. Rebuild monthly reports
 11. Print final exposure summary
 12. Send Discord alert
"""

from __future__ import annotations

import datetime as dt
from typing import Dict, List

import yfinance as yf

from utils import get_logger, iso_week_id as _iso_week_id
from discord_trades import alert_stock_open, alert_stock_closes
from config import (
    ENABLE_CSP,
    INDIVIDUAL, IRA, ROTH,
    STOCKS, CSP_STOCKS,
    CSP_LEDGER_FILE,
)

import strategies as strat
import wheel as _wheel_mod
from data_cache import DataCache
from market import fetch_market_context, allow_swing_trades, allow_retirement_tactical, csp_mode, market_regime, is_market_hours
from screener_display import (
    print_market_context,
    print_open_holdings,
    print_open_csps,
    print_open_ccs,
    print_open_cc_roll_candidates,
    print_csp_roll_candidates,
    print_final_exposure_summary,
    build_market_alert,
    build_stocks_alert,
    build_options_alert,
    send_market_alert,
    send_stocks_alert,
    send_options_alert,
    send_discord,
)
from screener_positions import (
    build_holdings_and_mv,
    compute_acct_mv,
    collect_tickers_for_price_fetch,
)
from screener_stocks import (
    scan_stock_entries_and_watchlist,
    plan_and_execute_stocks,
    print_watchlist,
)
from screener_options import build_csp_candidates, plan_ccs_from_open_lots
from wheel import (
    ensure_wheel_files,
    compute_wheel_exposure,
    compute_week_remaining,
    record_event,
    create_lots_from_new_assignments,
    link_new_ccs_to_lots,
    process_cc_expirations,
    rebuild_monthly_from_events,
    should_backfill_events,
    backfill_open_events_from_positions,
)

log = get_logger(__name__)


def _run_integrity_check(today: dt.date) -> None:
    """Warn on CSP/lot/CC state inconsistencies.  Warn-and-continue — never aborts the run."""
    try:
        csp_rows = strat.load_csv_rows(strat.CSP_POSITIONS_FILE)
        cc_rows  = strat.load_csv_rows(strat.CC_POSITIONS_FILE)

        import sys as _sys
        _whl  = _sys.modules.get("wheel") or __import__("wheel")
        lots  = _whl._read_rows(_whl.WHEEL_LOTS_FILE)

        lot_by_csp_id = {(r.get("source_csp_id") or "").strip(): r for r in lots}

        open_cc_rows = [r for r in cc_rows if (r.get("status") or "").upper() == "OPEN"]
        open_by_source_lot = {
            (r.get("source_lot_id") or "").strip()
            for r in open_cc_rows
            if (r.get("source_lot_id") or "").strip()
        }
        open_by_cc_id = {
            (r.get("id") or "").strip()
            for r in open_cc_rows
        }

        for r in csp_rows:
            if (r.get("status") or "").upper() != "ASSIGNED":
                continue
            csp_id = (r.get("id") or "").strip()
            if csp_id and csp_id not in lot_by_csp_id:
                log.warning(
                    "INTEGRITY: ASSIGNED CSP %s (%s %s) has no matching wheel lot — "
                    "create_lots_from_new_assignments may not have run yet.",
                    csp_id, r.get("ticker", "?"), r.get("expiry", "?"),
                )

        for lot in lots:
            if (lot.get("has_open_cc") or "").strip() not in ("1", "true", "TRUE"):
                continue
            lot_id = (lot.get("lot_id") or "").strip()
            cc_id  = (lot.get("cc_id")  or "").strip()
            linked = (
                (lot_id and lot_id in open_by_source_lot)
                or (cc_id  and cc_id  in open_by_cc_id)
            )
            if not linked:
                log.warning(
                    "INTEGRITY: lot %s (%s) has_open_cc=1 but no matching OPEN CC "
                    "(checked source_lot_id and cc_id=%s).",
                    lot_id, lot.get("ticker", "?"), cc_id,
                )
    except Exception as e:
        log.warning("Pre-run integrity check failed (non-fatal): %s", e)


def run_screener() -> None:
    today = dt.date.today()

    # ── 1. Warm data cache ────────────────────────────────────────
    ret_tickers = [
        (r.get("ticker") or "").strip().upper()
        for r in strat.load_retirement_positions()
        if (r.get("ticker") or "").strip()
    ]
    lot_tickers = [
        (r.get("ticker") or "").strip().upper()
        for r in _wheel_mod.get_open_lots()
        if (r.get("ticker") or "").strip()
    ]
    all_equity_tickers = list({
        "SPY", "QQQ", "^VIX",
        *STOCKS,
        *CSP_STOCKS,
        *ret_tickers,
        *lot_tickers,
    })
    cache = DataCache(all_equity_tickers)
    cache.warm()
    strat.set_data_cache(cache)
    strat.reset_chain_cache()
    _wheel_mod.set_data_cache(cache)

    # ── 2. Market context + regime ────────────────────────────────
    mkt         = fetch_market_context(cache)
    trading_on  = allow_swing_trades(mkt)
    retire_on   = allow_retirement_tactical(mkt)
    csp_regime  = csp_mode(mkt)
    regime      = market_regime(mkt)
    market_open = is_market_hours()

    print_market_context(mkt, trading_on, retire_on)
    if not market_open:
        print("\n⏰ AFTER HOURS — position maintenance only (no new entries)")
    if ENABLE_CSP:
        print(f"\n🧾 CSP engine: ENABLED | Regime: {csp_regime} | Market: {regime}")
    else:
        print("\n🧾 CSP engine: DISABLED")

    # ── 3. Ensure files + maintenance ─────────────────────────────
    ensure_wheel_files()
    strat.ensure_positions_files()
    strat.ensure_stock_files()
    strat.ensure_retirement_file()

    ret_by_key, ret_flagged = strat.update_retirement_marks()

    # ── 4. Build + print open holdings ───────────────────────────
    holding_tickers = collect_tickers_for_price_fetch(ret_by_key)
    # live_prices: fast_info per ticker for small sets (open positions),
    # batch daily close for large universe — single source of truth for
    # all intraday price decisions.
    px = strat.live_prices(holding_tickers)

    holdings, wheel_mv, mv_stock = build_holdings_and_mv(px)
    acct_mv = compute_acct_mv(ret_by_key, mv_stock, wheel_mv)

    print_open_holdings(holdings)
    print_open_csps(today)
    print_open_ccs(today, px)

    # ── 5. Close stock positions at stop / target ─────────────────
    closes       = strat.update_and_close_stock_positions(today, mkt)
    ret_stops    = strat.close_retirement_stops(today)
    strat.rebuild_stock_monthly_from_trades()
    stock_closed = closes.get("stops", []) + closes.get("targets", [])
    alert_stock_closes(today)

    if ret_stops.get("stopped"):
        print("\n🛑 RETIREMENT STOPS TRIGGERED")
        for s in ret_stops["stopped"]:
            print(f"  {s}")

    if ret_stops.get("targets"):
        print("\n✅ RETIREMENT TARGETS HIT")
        for s in ret_stops["targets"]:
            print(f"  {s}")

    # ── 6. Wheel maintenance ──────────────────────────────────────
    _run_integrity_check(today)

    csp_tp_out    = strat.process_csp_take_profits(today, regime=regime)
    cc_tp_out     = strat.scan_cc_take_profits(today, regime=regime)
    csp_out       = strat.process_csp_expirations(today)
    cc_out        = process_cc_expirations(today)
    early_asn_out = strat.scan_early_assignments(today)
    create_lots_from_new_assignments(today)
    link_new_ccs_to_lots(today)

    csp_roll_candidates = strat.scan_csp_roll_candidates(today)

    if early_asn_out.get("assigned"):
        print("\n⚠️  EARLY ASSIGNMENTS AUTO-MARKED")
        for s in early_asn_out["assigned"]:
            print(f"  {s}")

    if early_asn_out.get("warned"):
        print("\n⚠️  EARLY ASSIGNMENT CANDIDATES (warn-only)")
        for s in early_asn_out["warned"]:
            print(f"  {s}")

    if csp_tp_out.get("closed"):
        print("\n✅ CSP TAKE-PROFITS CLOSED")
        for c in csp_tp_out["closed"]:
            print(f"  {c['summary']}")
            record_event(
                date=today.isoformat(),
                account=c.get("account", INDIVIDUAL),
                ticker=c["ticker"],
                event_type="CSP_CLOSE_TP",
                ref_id=c["ref_id"],
                expiry=c["expiry"],
                strike=float(c["strike"]),
                contracts=int(c["contracts"]),
                shares=int(c["contracts"]) * 100,
                premium=-float(c["buyback"]),
                wheel_value=0.0,
                notes=f"CSP take-profit close, profit ${c['profit']:.0f}",
            )

    if cc_tp_out.get("closed"):
        print("\n✅ CC TAKE-PROFITS CLOSED")
        for c in cc_tp_out["closed"]:
            print(f"  {c['summary']}")
            record_event(
                date=today.isoformat(),
                account=c.get("account", INDIVIDUAL),
                ticker=c["ticker"],
                event_type="CC_CLOSE_TP",
                ref_id=c["ref_id"],
                expiry=c["expiry"],
                strike=float(c["strike"]),
                contracts=int(c["contracts"]),
                shares=int(c["contracts"]) * 100,
                premium=-float(c["buyback"]),
                wheel_value=0.0,
                notes=f"CC take-profit close, profit ${c['profit']:.0f}",
            )

    # ── 7. Stock scan + execution ─────────────────────────────────
    entries, watch = (scan_stock_entries_and_watchlist(regime=regime)
                      if market_open else ([], []))

    if not market_open:
        print("\n📈 STOCK SCAN: skipped (after hours)")
        stock_opened, planned_stocks = [], []
    elif entries:
        stock_opened, planned_stocks = plan_and_execute_stocks(
            today, entries, mkt, trading_on, retire_on, acct_mv, ret_by_key,
            regime=regime,
        )
        alert_stock_open(stock_opened)
        if not stock_opened:
            print_watchlist(watch)
    else:
        stock_opened, planned_stocks = [], []
        print_watchlist(watch)

    # ── 8. CSP planning + execution ───────────────────────────────
    new_csp_orders: List[dict] = []
    if not market_open:
        print("\n🧾 CSP SCAN: skipped (after hours)")
    elif ENABLE_CSP and csp_regime in ("NORMAL", "RISK_OFF"):

        live_vix: float | None = None
        try:
            live_vix = float(yf.Ticker("^VIX").fast_info.get("last_price") or 0) or None
            if live_vix:
                log.info("Live intraday VIX: %.2f (EOD close: %.2f)", live_vix, float(mkt["vix_close"]))
        except Exception as e:
            log.warning("Could not fetch live VIX for spike guard: %s", e)

        candidates = build_csp_candidates(mkt, csp_regime, regime=regime)

        open_csp_tickers = strat.load_open_csp_tickers(today)
        open_cc_tickers  = strat.load_open_cc_tickers()
        candidates = [
            c for c in candidates
            if (c.get("ticker") or "").strip().upper() not in open_csp_tickers
            and (c.get("ticker") or "").strip().upper() not in open_cc_tickers
        ]

        _open_csp_rows = strat.load_csv_rows(strat.CSP_POSITIONS_FILE)
        _open_csp_by_acct: Dict[str, set] = {INDIVIDUAL: set(), IRA: set(), ROTH: set()}
        for _r in _open_csp_rows:
            if (_r.get("status") or "").upper() != "OPEN":
                continue
            _acct = (_r.get("account") or INDIVIDUAL).strip().upper()
            _tkr  = (_r.get("ticker") or "").strip().upper()
            if _acct in _open_csp_by_acct and _tkr:
                _open_csp_by_acct[_acct].add(_tkr)

        def _sector_counts_for_account(acct: str) -> dict:
            counts: dict = {}
            for tkr in _open_csp_by_acct.get(acct, set()):
                sec = strat.get_ticker_sector(tkr)
                if sec != "OTHER":
                    counts[sec] = counts.get(sec, 0) + 1
            return counts

        exp_ira  = compute_wheel_exposure(today, IRA)
        exp_roth = compute_wheel_exposure(today, ROTH)
        rem_ira  = compute_week_remaining(today, IRA)
        rem_roth = compute_week_remaining(today, ROTH)
        retirement_order = (
            [IRA, ROTH] if rem_ira >= rem_roth else [ROTH, IRA]
        )
        account_order = [INDIVIDUAL] + retirement_order

        used_tickers: set = set()
        any_week_cap_hit = True

        for acct in account_order:
            exp            = compute_wheel_exposure(today, acct)
            week_remaining = compute_week_remaining(today, acct)

            if week_remaining <= 0:
                print(f"\n🧾 CSP [{acct}]: scanning skipped (weekly cap reached).")
                continue

            any_week_cap_hit = False

            acct_candidates = [
                c for c in candidates
                if (c.get("ticker") or "").strip().upper() not in used_tickers
            ]

            if not acct_candidates:
                print(f"\n🧾 CSP [{acct}]: no candidates remaining after dedup.")
                continue

            total_remaining = max(float(exp["cap"]) - float(exp["total_exposure"]), 0.0)
            plan = strat.plan_weekly_csp_orders(
                acct_candidates,
                today=today,
                vix_close=float(mkt["vix_close"]),
                total_remaining_cap=total_remaining,
                week_remaining_cap=float(week_remaining),
                aggressive_total=int(exp.get("aggressive_total", 0)),
                aggressive_week=int(exp.get("aggressive_week", 0)),
                open_sector_counts=_sector_counts_for_account(acct),
                live_vix=live_vix,
                regime=regime,
            )

            orders = plan.get("selected", [])
            if not orders:
                print(f"\n🧾 CSP [{acct}]: No new entries today.")
                continue

            for o in orders:
                o["account"] = acct
                used_tickers.add((o.get("ticker") or "").strip().upper())

            new_csp_orders.extend(orders)

            print(f"\n🧾 NEW CSP IDEAS [{acct}] (paper execution)")
            for o in orders:
                print(
                    f"  {o['ticker']:<6} {o['strike']:.0f}P {o['expiry']} | "
                    f"est prem ${o['est_premium']:.0f} | cash ${o['cash_reserved']:,.0f} | {o.get('tier','')}"
                )

        if any_week_cap_hit and not new_csp_orders:
            print("\n🧾 CSP scanning skipped (weekly cap reached for all accounts).")

        week_id = _iso_week_id(today)
        for o in new_csp_orders:
            csp_id, created = strat.add_csp_position_from_selected(
                today.isoformat(), week_id, o
            )
            if not created:
                continue

            try:
                ledger_rows = strat.load_csv_rows(CSP_LEDGER_FILE)
                if not strat.csp_already_logged(
                    ledger_rows, week_id,
                    o["ticker"], o["expiry"], float(o["strike"]),
                    account=(o.get("account") or INDIVIDUAL),
                ):
                    strat.append_csp_ledger_row({
                        "date":          today.isoformat(),
                        "week_id":       week_id,
                        "account":       (o.get("account") or INDIVIDUAL),
                        "ticker":        o["ticker"],
                        "expiry":        o["expiry"],
                        "strike":        f"{float(o['strike']):.2f}",
                        "contracts":     int(o.get("contracts", 1)),
                        "premium":       round(float(o.get("est_premium", 0.0)), 2),
                        "cash_reserved": round(float(o.get("cash_reserved", 0.0)), 2),
                        "tier":          o.get("tier", ""),
                    })
            except Exception as e:
                log.warning("CSP ledger append failed for %s: %s", o.get("ticker"), e)

            record_event(
                date=today.isoformat(),
                account=(o.get("account") or INDIVIDUAL),
                ticker=o["ticker"],
                event_type="CSP_OPEN",
                ref_id=csp_id,
                expiry=o["expiry"],
                strike=float(o["strike"]),
                contracts=int(o.get("contracts", 1)),
                shares=int(o.get("contracts", 1)) * 100,
                premium=float(o.get("premium") or o.get("est_premium") or 0.0),
                wheel_value=float(o.get("cash_reserved", 0.0)),
                notes="CSP opened (planned by screener)",
            )

    else:
        print("\n🧾 CSP scanning skipped (market filter or ENABLE_CSP=False).")

    # ── 9. CC planning + execution ────────────────────────────────
    new_cc_orders: List[dict] = []
    if not market_open:
        print("\n📞 CC SCAN: skipped (after hours)")
    else:
        new_cc_orders = plan_ccs_from_open_lots()

        if new_cc_orders:
            print("\n📞 NEW CC IDEAS (from lots) (paper execution)")
            for o in new_cc_orders[:15]:
                credit_est = float(o.get("credit_mid", 0.0)) * 100.0
                print(f"  {o['ticker']:<6} {float(o['strike']):.0f}C {o['expiry']} | est credit ${credit_est:.0f} | {o.get('reason','')}")

            for o in new_cc_orders:
                strat.add_cc_position_from_candidate(today.isoformat(), o)

            link_new_ccs_to_lots(today)
        else:
            print("\n📞 CC: No calls triggered today.")

    if market_open:
        print_open_cc_roll_candidates(px, today=today)
        print_csp_roll_candidates(csp_roll_candidates)

    # ── 10. Backfill + monthly rebuild ───────────────────────────
    if should_backfill_events():
        backfill_open_events_from_positions(today)

    rebuild_monthly_from_events()

    # ── 11. Final exposure + refresh marks ───────────────────────
    ret_by_key, ret_flagged = strat.update_retirement_marks()
    _, wheel_mv_final, mv_stock_final = build_holdings_and_mv(
        strat.live_prices(collect_tickers_for_price_fetch(ret_by_key))
    )
    print_final_exposure_summary(today, ret_by_key, ret_flagged, mv_stock_final, wheel_mv_final)

    # ── 12. Discord alerts (three channels) ─────────────────────────────────
    # #screener-market — always posts
    send_market_alert(build_market_alert(
        mkt=mkt,
        trading_on=trading_on,
        retire_on=retire_on,
        ret_by_key=ret_by_key,
        mv_stock=mv_stock_final,
        wheel_mv=wheel_mv_final,
    ))

    # #screener-stocks — only posts when there is something to show
    stocks_msg = build_stocks_alert(
        planned_stocks=planned_stocks,
        stock_closes=stock_closed,
        ret_stopped=ret_stops.get("stopped", []),
        ret_targets=ret_stops.get("targets", []),
        watch=watch,
        regime=regime,
    )
    if stocks_msg:
        send_stocks_alert(stocks_msg)

    # #screener-options — only posts when there is something to show
    options_msg = build_options_alert(
        new_csps=new_csp_orders,
        new_ccs=new_cc_orders,
        csp_tp=[c["summary"] for c in csp_tp_out.get("closed", [])],
        cc_tp=[c["summary"] for c in cc_tp_out.get("closed", [])],
        csp_exp=csp_out.get("expired", []),
        csp_asn=csp_out.get("assigned", []),
        cc_exp=cc_out.get("expired", []),
        cc_call=cc_out.get("called_away", []),
        early_asn=early_asn_out.get("assigned", []) + early_asn_out.get("warned", []),
        csp_roll=csp_roll_candidates,
    )
    if options_msg:
        send_options_alert(options_msg)

    print("\n✅ Screener run complete.")


if __name__ == "__main__":
    run_screener()
