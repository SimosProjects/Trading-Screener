# screener.py
import datetime as dt

from config import (
    ENABLE_CSP,
    CSP_STOCKS,
    WEBHOOK_URL,
)

from strategies import (
    market_context,
    print_market_context,
    find_csp_candidates,
    build_discord_summary,
)

from wheel import (
    ensure_wheel_files,
    compute_wheel_exposure,
    compute_week_remaining,
    create_lots_from_new_assignments,
    link_new_ccs_to_lots,
    process_cc_expirations,
    rebuild_monthly_from_events,
)

import requests


# ------------------------------------------------------------
# Discord helper
# ------------------------------------------------------------

def send_discord(message: str):
    if not WEBHOOK_URL or WEBHOOK_URL == "https://discord.com/api/webhooks/1445480294500270081/pBeMhblXLTybjfht9YPOuC8YshLxXD52BKb-IL7TR9YMt1i4fcqteMcbG9sqrzRYnlr_":
        return

    try:
        requests.post(WEBHOOK_URL, json={"content": message}, timeout=10)
    except Exception as e:
        print(f"Discord error: {e}")


# ------------------------------------------------------------
# Main screener
# ------------------------------------------------------------

def run_screener():
    today = dt.date.today()
    today_str = today.isoformat()

    print(f"\n==============================")
    print(f"📅 RUN DATE: {today_str}")
    print(f"==============================\n")

    # --------------------------------------------------------
    # 1️⃣ Market context (RESTORED)
    # --------------------------------------------------------
    ctx = market_context()
    print_market_context(ctx)

    trading_allowed = (
        ctx["spy_above_200"]
        and ctx["qqq_above_50"]
        and ctx["vix_lt_25"]
    )

    if not trading_allowed:
        print("🔴 Trading OFF — Market conditions not favorable.\n")
    else:
        print("🟢 Trading ON — Scanning for CSPs.\n")

    # --------------------------------------------------------
    # 2️⃣ Wheel exposure (institutional risk control)
    # --------------------------------------------------------
    ensure_wheel_files()

    exposure = compute_wheel_exposure(today)
    week_remaining = compute_week_remaining(today)

    print("💼 WHEEL EXPOSURE")
    print(f"  Total exposure: ${exposure['total_exposure']:,.0f}")
    print(f"  Weekly exposure: ${exposure['week_exposure']:,.0f}")
    print(f"  Weekly remaining: ${week_remaining:,.0f}")
    print()

    # --------------------------------------------------------
    # 3️⃣ Find CSP strategy candidates
    # --------------------------------------------------------
    csp_ideas = []
    watchlist = []

    # Always scan for strategy signals
    if ENABLE_CSP:
        csp_ideas, watchlist = find_csp_candidates(CSP_STOCKS)

    # But only ALLOW execution if rules pass
    if not trading_allowed:
        print("⚠️ Market gate OFF — CSPs are informational only.")

    if week_remaining <= 0:
        print("⚠️ Weekly allocation exhausted — CSPs are informational only.")

    # --------------------------------------------------------
    # 4️⃣ Process wheel lifecycle (ASSIGN → LOT → CC → CLOSE)
    # --------------------------------------------------------
    print("\n🔁 WHEEL MAINTENANCE")

    assigned_lots = create_lots_from_new_assignments(today)
    if assigned_lots:
        print(f"  Created {len(assigned_lots)} new stock lots from assignments")

    linked = link_new_ccs_to_lots(today)
    if linked:
        print(f"  Linked {len(linked)} CCs to stock lots")

    cc_results = process_cc_expirations(today)
    if cc_results["expired"] or cc_results["called_away"]:
        print(f"  CC expired: {len(cc_results['expired'])}")
        print(f"  CC called away: {len(cc_results['called_away'])}")

    rebuild_monthly_from_events()

    # --------------------------------------------------------
    # 5️⃣ Output strategy summary
    # --------------------------------------------------------
    print("\n📈 CSP STRATEGY RESULTS")

    if csp_ideas:
        for idea in csp_ideas:
            print(
                f"  {idea['ticker']} {idea['expiry']} "
                f"{idea['strike']:.0f}P | "
                f"${idea['premium']:.0f}"
            )
    else:
        print("  No CSP entries today.")

    if watchlist:
        print("\n👀 WATCHLIST")
        print(" ", ", ".join(watchlist))

    # --------------------------------------------------------
    # 6️⃣ Discord summary (RESTORED)
    # --------------------------------------------------------
    summary = build_discord_summary(
        date=today_str,
        market_ctx=ctx,
        csp_ideas=csp_ideas,
        watch=watchlist,
    )

    send_discord(summary)

    print("\n✅ Screener run complete.\n")


# ------------------------------------------------------------
# Entry point
# ------------------------------------------------------------

if __name__ == "__main__":
    run_screener()
