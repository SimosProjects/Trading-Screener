"""discord_trades.py

Sends stock trade alerts formatted for XTrades XCapture bot compatibility.

Alert format (per https://docs.xtrades.net/documentation/alert-capture-bot-xcapture/alert-capture-guide):

    BTO AAPL @ 172.50 SL 165.00 PT 185.00
    STC AAPL @ 181.00

Notes:
- SL and PT are included on BTO so XCapture auto-tracks and auto-closes the alert.
- STC is only sent on manual closes (STOP hit before SL triggers, TARGET hit, etc).
  If XCapture already auto-closed via SL/PT, the STC is a no-op on their end.
- Only INDIVIDUAL account swing trades are sent.
- IRA/ROTH and wheel-sourced closes (CC_CALLED_AWAY) are excluded.

Environment variable:
    DISCORD_TRADES_WEBHOOK_URL  — webhook URL for the #paper-trades channel.
                                  If unset, all functions silently no-op.

Usage in screener.py
--------------------
    from discord_trades import alert_stock_open, alert_stock_closes

    # After update_and_close_stock_positions() (step 5):
    alert_stock_closes(today)

    # After plan_and_execute_stocks() (step 7):
    alert_stock_open(stock_opened)
"""

from __future__ import annotations

import csv
import datetime as dt
import os
from typing import List

from utils import get_logger

log = get_logger(__name__)

_WHEEL_CLOSE_TYPES = {"CC_CALLED_AWAY", "CC_MANUAL_EXIT"}


def _trades_webhook_url() -> str:
    return os.environ.get("DISCORD_TRADES_WEBHOOK_URL", "").strip()


def _send(msg: str) -> None:
    url = _trades_webhook_url()
    if not url or not url.startswith("http"):
        return
    try:
        import requests
        resp = requests.post(url, json={"content": msg}, timeout=10)
        if resp.status_code not in (200, 204):
            log.warning("discord_trades: HTTP %s for: %s", resp.status_code, msg)
    except Exception as e:
        log.warning("discord_trades: send failed: %s", e)


def alert_stock_open(stock_opened: List[str]) -> None:
    """Send a BTO alert for each newly opened INDIVIDUAL swing trade.

    Format: BTO AAPL @ 172.50 SL 165.00 PT 185.00

    SL and PT are included so XCapture can auto-track and auto-close the
    alert when either level is hit.

    Args:
        stock_opened: list of "ACCOUNT:TICKER" strings from plan_and_execute_stocks.
    """
    if not stock_opened or not _trades_webhook_url():
        return

    import strategies as strat

    try:
        positions = strat.load_stock_positions()
    except Exception as e:
        log.warning("discord_trades.alert_stock_open: could not load positions: %s", e)
        return

    # Most recent OPEN INDIVIDUAL position per ticker.
    pos_by_ticker: dict = {}
    for row in positions:
        if (row.get("account") or "").strip().upper() != "INDIVIDUAL":
            continue
        if (row.get("status") or "").strip().upper() != "OPEN":
            continue
        tkr = (row.get("ticker") or "").strip().upper()
        if not tkr:
            continue
        existing = pos_by_ticker.get(tkr)
        if existing is None or row.get("entry_date", "") >= existing.get("entry_date", ""):
            pos_by_ticker[tkr] = row

    for entry in stock_opened:
        parts = entry.split(":", 1)
        if len(parts) != 2:
            continue
        account, tkr = parts[0].strip().upper(), parts[1].strip().upper()
        if account != "INDIVIDUAL":
            continue

        pos = pos_by_ticker.get(tkr)
        if pos is None:
            log.warning("discord_trades.alert_stock_open: no OPEN position for %s", tkr)
            continue

        try:
            price  = float(pos.get("entry_price") or 0.0)
            shares = int(float(pos.get("shares") or 0))
            stop   = float(pos.get("stop_price") or 0.0)
            target = float(pos.get("target_price") or 0.0)
        except Exception as e:
            log.warning("discord_trades.alert_stock_open: bad numeric for %s: %s", tkr, e)
            continue

        if price <= 0 or shares <= 0:
            continue

        msg = f"BTO {tkr} @ {price:.2f}"
        if stop > 0:
            msg += f" SL {stop:.2f}"
        if target > 0:
            msg += f" PT {target:.2f}"

        _send(msg)
        log.debug("discord_trades: %s", msg)


def alert_stock_closes(today: dt.date) -> None:
    """Send an STC alert for each INDIVIDUAL swing trade closed today.

    Format: STC AAPL @ 181.00

    Reads stock_trades.csv for today's closed rows directly — no changes
    needed to strategies.py. Wheel-sourced closes are excluded.

    Note: if XCapture already auto-closed the alert via SL/PT trigger,
    sending an STC is harmless — XCapture ignores closes on already-closed alerts.

    Args:
        today: the current run date.
    """
    if not _trades_webhook_url():
        return

    from config import STOCK_TRADES_FILE

    if not os.path.isfile(STOCK_TRADES_FILE):
        return

    today_str = today.isoformat()

    try:
        with open(STOCK_TRADES_FILE, "r", newline="") as f:
            rows = list(csv.DictReader(f))
    except Exception as e:
        log.warning("discord_trades.alert_stock_closes: could not read %s: %s",
                    STOCK_TRADES_FILE, e)
        return

    for row in rows:
        if (row.get("exit_date") or "").strip() != today_str:
            continue

        account    = (row.get("account")    or "").strip().upper()
        close_type = (row.get("close_type") or "").strip().upper()
        tkr        = (row.get("ticker")     or "").strip().upper()

        if account != "INDIVIDUAL":
            continue
        if close_type in _WHEEL_CLOSE_TYPES:
            continue

        try:
            exit_price = float(row.get("exit_price") or 0.0)
            shares     = int(float(row.get("shares") or 0))
        except Exception as e:
            log.warning("discord_trades.alert_stock_closes: bad numeric for %s: %s", tkr, e)
            continue

        if exit_price <= 0 or shares <= 0:
            continue

        msg = f"STC {tkr} @ {exit_price:.2f}"
        _send(msg)
        log.debug("discord_trades: %s", msg)
