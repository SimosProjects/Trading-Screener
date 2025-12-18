"""wheel.py

Lightweight 'institutional wheel' bookkeeping.

Goal: make screener.py runnable and keep state in a small number of CSVs.

Files (configured in config.py):
  - wheel_events.csv : append-only event log (CSP/CC opens, expiries, assignments, called-away)
  - wheel_lots.csv   : current stock lots created by CSP assignment (used to drive CC ideas)
  - wheel_monthly/   : one CSV per month, rebuilt from wheel_events.csv

This is intentionally conservative and simple. It is *not* broker-integrated.
"""

from __future__ import annotations

import csv
import datetime as dt
import os
from typing import Dict, List

from config import (
    WHEEL_EVENTS_FILE,
    WHEEL_LOTS_FILE,
    WHEEL_MONTHLY_DIR,
    WHEEL_CAP,
    WHEEL_WEEKLY_TARGET,
    CSP_POSITIONS_FILE,
    CC_POSITIONS_FILE,
)

EVENT_FIELDS = [
    "event_id",
    "date",
    "week_id",
    "ticker",
    "event_type",
    "ref_id",
    "expiry",
    "strike",
    "contracts",
    "shares",
    "premium",
    "wheel_value",
    "notes",
]

LOT_FIELDS = [
    "lot_id",
    "ticker",
    "open_date",
    "shares",
    "assigned_strike",
    "cost_basis",     # total cost basis dollars (strike*shares - premium)
    "source_csp_id",
    "has_open_cc",
    "cc_id",
    "status",         # OPEN / CLOSED
]

def _iso_week_id(d: dt.date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"

def _read_rows(path: str) -> List[dict]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", newline="") as f:
        return list(csv.DictReader(f))

def _write_rows(path: str, rows: List[dict], fieldnames: List[str]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

def ensure_wheel_files() -> None:
    # ensure directory exists
    base_dir = os.path.dirname(os.path.abspath(WHEEL_EVENTS_FILE))
    os.makedirs(base_dir, exist_ok=True)

    if not os.path.isfile(WHEEL_EVENTS_FILE):
        with open(WHEEL_EVENTS_FILE, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=EVENT_FIELDS)
            w.writeheader()

    if not os.path.isfile(WHEEL_LOTS_FILE):
        with open(WHEEL_LOTS_FILE, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=LOT_FIELDS)
            w.writeheader()

def record_event(**kwargs) -> None:
    ensure_wheel_files()

    date_str = (kwargs.get("date") or "").strip() or dt.date.today().isoformat()
    try:
        d = dt.date.fromisoformat(date_str)
    except Exception:
        d = dt.date.today()

    ticker_norm = (kwargs.get("ticker") or "").strip().upper()
    event_type_norm = (kwargs.get("event_type") or "").strip().upper()
    ref_norm = (kwargs.get("ref_id") or "").strip()

    row = {k: "" for k in EVENT_FIELDS}
    # Deterministic event_id so rerunning the screener is idempotent
    row["event_id"] = kwargs.get("event_id") or f"{date_str}-{ticker_norm}-{event_type_norm}-{ref_norm}"
    row["date"] = date_str
    row["week_id"] = kwargs.get("week_id") or _iso_week_id(d)
    row["ticker"] = ticker_norm
    row["event_type"] = event_type_norm
    row["ref_id"] = ref_norm
    row["expiry"] = (kwargs.get("expiry") or "").strip()
    row["strike"] = f"{float(kwargs.get('strike') or 0):.2f}"
    row["contracts"] = str(int(float(kwargs.get("contracts") or 0)))
    row["shares"] = str(int(float(kwargs.get("shares") or 0)))
    row["premium"] = f"{float(kwargs.get('premium') or 0):.2f}"
    row["wheel_value"] = f"{float(kwargs.get('wheel_value') or 0):.2f}"
    row["notes"] = (kwargs.get("notes") or "").strip()

    # Idempotency: don't append the same event twice
    try:
        existing = _read_rows(WHEEL_EVENTS_FILE)
        if any((r.get("event_id") or "") == row["event_id"] for r in existing):
            return
    except Exception:
        pass

    with open(WHEEL_EVENTS_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=EVENT_FIELDS, extrasaction="ignore")
        w.writerow(row)


# ----------------------------
# Lots (assigned stock)
# ----------------------------

def _make_lot_id(ticker: str, open_date: str, strike: float) -> str:
    return f"{ticker}-{open_date}-{float(strike):.2f}"

def get_open_lots() -> List[dict]:
    ensure_wheel_files()
    lots = _read_rows(WHEEL_LOTS_FILE)
    return [r for r in lots if (r.get("status") or "").upper() == "OPEN"]

def create_lots_from_new_assignments(today: dt.date) -> None:
    """Create lots for any CSP positions marked ASSIGNED that don't yet have a lot."""
    ensure_wheel_files()
    lots = _read_rows(WHEEL_LOTS_FILE)
    existing_by_csp = {(r.get("source_csp_id") or "") for r in lots}

    csp_rows = _read_rows(CSP_POSITIONS_FILE)
    for r in csp_rows:
        if (r.get("status") or "").upper() != "ASSIGNED":
            continue

        csp_id = (r.get("id") or "").strip()
        if not csp_id or csp_id in existing_by_csp:
            continue

        ticker = (r.get("ticker") or "").strip().upper()
        open_date = (r.get("close_date") or today.isoformat()).strip()

        try:
            strike = float(r.get("strike") or 0.0)
            shares = int(float(r.get("shares_if_assigned") or 0))
            basis = float(r.get("assignment_cost_basis") or 0.0)
        except Exception:
            continue

        if shares <= 0 or strike <= 0:
            continue

        lot_id = _make_lot_id(ticker, open_date, strike)
        lots.append({
            "lot_id": lot_id,
            "ticker": ticker,
            "open_date": open_date,
            "shares": str(shares),
            "assigned_strike": f"{strike:.2f}",
            "cost_basis": f"{basis:.2f}",
            "source_csp_id": csp_id,
            "has_open_cc": "0",
            "cc_id": "",
            "status": "OPEN",
        })

        record_event(
            date=open_date,
            ticker=ticker,
            event_type="CSP_ASSIGNED",
            ref_id=lot_id,
            expiry=(r.get("expiry") or ""),
            strike=strike,
            contracts=int(float(r.get("contracts") or 0)),
            shares=shares,
            premium=float(r.get("est_premium") or 0.0),
            wheel_value=strike * shares,
            notes=f"Assigned from CSP {csp_id}",
        )

    _write_rows(WHEEL_LOTS_FILE, lots, LOT_FIELDS)

def link_new_ccs_to_lots(today: dt.date) -> None:
    """Attach OPEN CCs in cc_positions.csv to OPEN lots of the same ticker if not already linked."""
    ensure_wheel_files()
    lots = _read_rows(WHEEL_LOTS_FILE)
    if not lots:
        return

    cc_rows = _read_rows(CC_POSITIONS_FILE)
    open_ccs = [r for r in cc_rows if (r.get("status") or "").upper() == "OPEN"]

    cc_by_ticker: Dict[str, dict] = {}
    for r in open_ccs:
        t = (r.get("ticker") or "").strip().upper()
        if t:
            cc_by_ticker[t] = r

    changed = False
    for lot in lots:
        if (lot.get("status") or "").upper() != "OPEN":
            continue

        t = (lot.get("ticker") or "").strip().upper()
        if not t:
            continue

        if (lot.get("has_open_cc") or "").strip() in ("1", "true", "TRUE"):
            continue

        cc = cc_by_ticker.get(t)
        if not cc:
            continue

        cc_id = (cc.get("id") or "").strip()
        if not cc_id:
            continue

        lot["has_open_cc"] = "1"
        lot["cc_id"] = cc_id
        changed = True

        # Log CC open event once
        try:
            prem = float(cc.get("credit_mid") or 0.0) * 100.0 * float(cc.get("contracts") or 0.0)
        except Exception:
            prem = 0.0

        record_event(
            date=(cc.get("open_date") or today.isoformat()),
            ticker=t,
            event_type="CC_OPEN",
            ref_id=cc_id,
            expiry=(cc.get("expiry") or ""),
            strike=float(cc.get("strike") or 0.0),
            contracts=int(float(cc.get("contracts") or 0)),
            shares=int(float(cc.get("contracts") or 0)) * 100,
            premium=prem,
            wheel_value=0.0,
            notes=f"Linked CC {cc_id} to lot {lot.get('lot_id','')}",
        )

    if changed:
        _write_rows(WHEEL_LOTS_FILE, lots, LOT_FIELDS)

def process_cc_expirations(today: dt.date) -> Dict[str, List[str]]:
    """Mark OPEN CCs as EXPIRED / CALLED_AWAY if expiry <= today."""
    ensure_wheel_files()
    cc_rows = _read_rows(CC_POSITIONS_FILE)
    if not cc_rows:
        return {"expired": [], "called_away": []}

    expired: List[str] = []
    called: List[str] = []

    import yfinance as yf
    import pandas as pd

    changed = False
    for r in cc_rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue

        exp_str = (r.get("expiry") or "").strip()
        try:
            exp = dt.date.fromisoformat(exp_str)
        except Exception:
            continue

        if exp > today:
            continue

        tkr = (r.get("ticker") or "").strip().upper()
        try:
            strike = float(r.get("strike") or 0.0)
        except Exception:
            strike = 0.0

        underlying_close = None
        try:
            start = (exp - dt.timedelta(days=7)).isoformat()
            end = (exp + dt.timedelta(days=1)).isoformat()
            df = yf.download(tkr, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
            df.dropna(inplace=True)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            if not df.empty:
                underlying_close = float(df["Close"].iloc[-1])
        except Exception:
            underlying_close = None

        r["close_date"] = exp.isoformat()
        if underlying_close is not None and strike > 0 and underlying_close >= strike:
            r["status"] = "CALLED_AWAY"
            r["close_type"] = "CALLED_AWAY_ITM"
            called.append(f"{tkr} {exp_str} {strike:.0f}C")
            record_event(
                date=exp.isoformat(),
                ticker=tkr,
                event_type="CC_CALLED_AWAY",
                ref_id=(r.get("id") or ""),
                expiry=exp_str,
                strike=strike,
                contracts=int(float(r.get("contracts") or 0)),
                shares=int(float(r.get("contracts") or 0)) * 100,
                premium=0.0,
                wheel_value=0.0,
                notes="CC called away (best-effort inference)",
            )
        else:
            r["status"] = "EXPIRED"
            r["close_type"] = "EXPIRED_OTM"
            expired.append(f"{tkr} {exp_str} {strike:.0f}C")
            record_event(
                date=exp.isoformat(),
                ticker=tkr,
                event_type="CC_EXPIRED",
                ref_id=(r.get("id") or ""),
                expiry=exp_str,
                strike=strike,
                contracts=int(float(r.get("contracts") or 0)),
                shares=int(float(r.get("contracts") or 0)) * 100,
                premium=0.0,
                wheel_value=0.0,
                notes="CC expired (best-effort inference)",
            )
        changed = True

    if changed:
        # keep original headers if present, else fall back
        fieldnames = list(cc_rows[0].keys()) if cc_rows else []
        _write_rows(CC_POSITIONS_FILE, cc_rows, fieldnames)

    # If called away, close the associated lot
    if called:
        lots = _read_rows(WHEEL_LOTS_FILE)
        lot_changed = False
        called_cc_ids = {(r.get("id") or "") for r in cc_rows if (r.get("status") or "").upper() == "CALLED_AWAY"}
        for lot in lots:
            if (lot.get("status") or "").upper() != "OPEN":
                continue
            if (lot.get("cc_id") or "") in called_cc_ids:
                lot["status"] = "CLOSED"
                lot["has_open_cc"] = "0"
                lot_changed = True
        if lot_changed:
            _write_rows(WHEEL_LOTS_FILE, lots, LOT_FIELDS)

    return {"expired": expired, "called_away": called}


# ----------------------------
# Exposure + weekly remaining
# ----------------------------

def compute_wheel_exposure(today: dt.date) -> Dict[str, float | int | str]:
    """Compute wheel exposure (INDIVIDUAL wheel account only)."""
    ensure_wheel_files()

    total = 0.0
    aggressive_total = 0
    aggressive_week = 0
    week_id = _iso_week_id(today)

    # CSP collateral
    csp_rows = _read_rows(CSP_POSITIONS_FILE)
    for r in csp_rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        try:
            exp = dt.date.fromisoformat((r.get("expiry") or "").strip())
            if exp < today:
                continue
        except Exception:
            pass

        try:
            total += float(r.get("cash_reserved") or 0.0)
        except Exception:
            pass

        tier = (r.get("tier") or "").upper()
        if tier == "AGGRESSIVE":
            aggressive_total += 1
            if (r.get("week_id") or "") == week_id:
                aggressive_week += 1

    # Assigned lots (notional)
    lots = _read_rows(WHEEL_LOTS_FILE)
    for lot in lots:
        if (lot.get("status") or "").upper() != "OPEN":
            continue
        try:
            strike = float(lot.get("assigned_strike") or 0.0)
            shares = float(lot.get("shares") or 0.0)
            total += strike * shares
        except Exception:
            continue

    return {
        "week_id": week_id,
        "cap": float(WHEEL_CAP),
        "weekly_target": float(WHEEL_WEEKLY_TARGET),
        "total_exposure": float(total),
        "aggressive_total": int(aggressive_total),
        "aggressive_week": int(aggressive_week),
    }

def compute_week_remaining(today: dt.date) -> float:
    """Weekly remaining based on OPEN CSP collateral entered this ISO week."""
    exp = compute_wheel_exposure(today)
    week_id = exp["week_id"]

    week_used = 0.0
    csp_rows = _read_rows(CSP_POSITIONS_FILE)
    for r in csp_rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        if (r.get("week_id") or "") != week_id:
            continue
        try:
            week_used += float(r.get("cash_reserved") or 0.0)
        except Exception:
            pass

    return max(float(WHEEL_WEEKLY_TARGET) - week_used, 0.0)


# ----------------------------
# Monthly rebuild
# ----------------------------

def rebuild_monthly_from_events() -> None:
    """Rebuild one CSV per month from wheel_events.csv.

    Focuses on premium credits (CSP_OPEN + CC_OPEN).
    """
    ensure_wheel_files()
    rows = _read_rows(WHEEL_EVENTS_FILE)
    if not rows:
        return

    os.makedirs(WHEEL_MONTHLY_DIR, exist_ok=True)

    by_month: Dict[str, List[dict]] = {}
    for r in rows:
        d = (r.get("date") or "").strip()
        if len(d) < 7:
            continue
        month = d[:7]
        by_month.setdefault(month, []).append(r)

    out_fields = ["date", "ticker", "event_type", "ref_id", "premium"]

    for month, evs in by_month.items():
        evs_sorted = sorted(evs, key=lambda x: ((x.get("date") or ""), (x.get("ticker") or "")))

        out_rows: List[dict] = []
        total = 0.0
        for e in evs_sorted:
            et = (e.get("event_type") or "").upper()
            if et not in ("CSP_OPEN", "CC_OPEN"):
                continue
            try:
                prem = float(e.get("premium") or 0.0)
            except Exception:
                prem = 0.0
            total += prem
            out_rows.append({
                "date": (e.get("date") or "").strip(),
                "ticker": (e.get("ticker") or "").strip(),
                "event_type": et,
                "ref_id": (e.get("ref_id") or "").strip(),
                "premium": f"{prem:.2f}",
            })

        out_rows.append({
            "date": "",
            "ticker": "TOTAL",
            "event_type": "",
            "ref_id": "",
            "premium": f"{total:.2f}",
        })

        path = os.path.join(WHEEL_MONTHLY_DIR, f"{month}.csv")
        _write_rows(path, out_rows, out_fields)

def should_backfill_events() -> bool:
    return (not os.path.isfile(WHEEL_EVENTS_FILE)) or (os.path.getsize(WHEEL_EVENTS_FILE) < 50)

def backfill_open_events_from_positions(today: dt.date) -> None:
    """If wheel_events.csv is empty but you already have OPEN CSP/CC positions, backfill open events."""
    ensure_wheel_files()

    events = _read_rows(WHEEL_EVENTS_FILE)
    existing = set()
    for e in events:
        et = (e.get("event_type") or "").upper()
        ref = (e.get("ref_id") or "").strip()
        if et in ("CSP_OPEN", "CC_OPEN") and ref:
            existing.add((et, ref))

    # CSP opens
    for r in _read_rows(CSP_POSITIONS_FILE):
        if (r.get("status") or "").upper() != "OPEN":
            continue
        ref = (r.get("id") or "").strip()
        if not ref or ("CSP_OPEN", ref) in existing:
            continue

        prem = 0.0
        try:
            prem = float(r.get("est_premium") or 0.0)
            if prem <= 0:
                prem = float(r.get("credit_mid") or 0.0) * 100.0 * float(r.get("contracts") or 0.0)
        except Exception:
            prem = 0.0

        record_event(
            date=(r.get("open_date") or today.isoformat()),
            ticker=(r.get("ticker") or ""),
            event_type="CSP_OPEN",
            ref_id=ref,
            expiry=(r.get("expiry") or ""),
            strike=float(r.get("strike") or 0.0),
            contracts=int(float(r.get("contracts") or 0.0)),
            shares=int(float(r.get("contracts") or 0.0)) * 100,
            premium=prem,
            wheel_value=float(r.get("cash_reserved") or 0.0),
            notes="Backfilled from csp_positions.csv",
        )

    # CC opens
    for r in _read_rows(CC_POSITIONS_FILE):
        if (r.get("status") or "").upper() != "OPEN":
            continue
        ref = (r.get("id") or "").strip()
        if not ref or ("CC_OPEN", ref) in existing:
            continue

        prem = 0.0
        try:
            prem = float(r.get("credit_mid") or 0.0) * 100.0 * float(r.get("contracts") or 0.0)
        except Exception:
            prem = 0.0

        record_event(
            date=(r.get("open_date") or today.isoformat()),
            ticker=(r.get("ticker") or ""),
            event_type="CC_OPEN",
            ref_id=ref,
            expiry=(r.get("expiry") or ""),
            strike=float(r.get("strike") or 0.0),
            contracts=int(float(r.get("contracts") or 0.0)),
            shares=int(float(r.get("contracts") or 0.0)) * 100,
            premium=prem,
            wheel_value=0.0,
            notes="Backfilled from cc_positions.csv",
        )
