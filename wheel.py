"""
Institutional wheel bookkeeping.

Goal:
- Track "wheel exposure" so CSP sizing respects BOTH:
  (a) open CSP collateral
  (b) assigned stock lots that are still held (even if a CC is open against them)

This lets your "1/4 of cap per week" rule work like an institutional risk book.
"""

from __future__ import annotations

import csv
import datetime as dt
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import yfinance as yf

from config import (
    CSP_POSITIONS_FILE,
    CC_POSITIONS_FILE,
    WHEEL_EVENTS_FILE,
    WHEEL_LOTS_FILE,
    WHEEL_MONTHLY_DIR,
    WHEEL_CAP,
    WHEEL_WEEKLY_TARGET,
)

# ----------------------------
# CSV schemas
# ----------------------------

WHEEL_EVENTS_COLUMNS = [
    "event_id",
    "date",
    "week_id",
    "ticker",
    "event_type",              # CSP_OPEN, CSP_EXPIRE, CSP_ASSIGN, CC_OPEN, CC_EXPIRE, CC_ASSIGN, STOCK_CALLED_AWAY
    "ref_id",                  # optional: csp_id / cc_id / lot_id
    "expiry",
    "strike",
    "contracts",
    "shares",
    "premium",                 # option premium (+ for credit)
    "wheel_value",             # capital tied up created by this event (collateral or assignment value)
    "notes",
]

WHEEL_LOTS_COLUMNS = [
    "lot_id",
    "ticker",
    "open_date",
    "shares",
    "assigned_strike",
    "assigned_value",          # strike * shares
    "csp_id",
    "cc_id",                   # active CC id if any
    "status",                  # OPEN / CALLED_AWAY
    "close_date",
    "called_away_strike",
    "called_away_value",
    "stock_pnl",
    "notes",
]

# ----------------------------
# small csv helpers
# ----------------------------

def _load_rows(path: str) -> List[dict]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", newline="") as f:
        return list(csv.DictReader(f))

def _write_rows(path: str, rows: List[dict], fieldnames: List[str]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def _append_row(path: str, row: dict, fieldnames: List[str]) -> None:
    file_exists = os.path.isfile(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        w.writerow(row)

def _iso_week_id(d: dt.date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


# ----------------------------
# Public API
# ----------------------------

def ensure_wheel_files() -> None:
    """Create wheel_events.csv and wheel_lots.csv (and monthly dir) if missing."""
    if not os.path.isfile(WHEEL_EVENTS_FILE):
        _write_rows(WHEEL_EVENTS_FILE, [], WHEEL_EVENTS_COLUMNS)
    if not os.path.isfile(WHEEL_LOTS_FILE):
        _write_rows(WHEEL_LOTS_FILE, [], WHEEL_LOTS_COLUMNS)
    os.makedirs(WHEEL_MONTHLY_DIR, exist_ok=True)

def record_event(
    *,
    date: str,
    ticker: str,
    event_type: str,
    ref_id: str = "",
    expiry: str = "",
    strike: float = 0.0,
    contracts: int = 0,
    shares: int = 0,
    premium: float = 0.0,
    wheel_value: float = 0.0,
    notes: str = "",
) -> None:
    """Append a normalized event row."""
    d = dt.date.fromisoformat(date)
    week_id = _iso_week_id(d)
    event_id = f"{date}-{event_type}-{ticker}-{ref_id}".strip("-")
    _append_row(WHEEL_EVENTS_FILE, {
        "event_id": event_id,
        "date": date,
        "week_id": week_id,
        "ticker": ticker,
        "event_type": event_type,
        "ref_id": ref_id,
        "expiry": expiry,
        "strike": f"{float(strike):.2f}" if strike else "",
        "contracts": str(int(contracts)) if contracts else "",
        "shares": str(int(shares)) if shares else "",
        "premium": f"{float(premium):.2f}" if premium else "",
        "wheel_value": f"{float(wheel_value):.2f}" if wheel_value else "",
        "notes": notes or "",
    }, WHEEL_EVENTS_COLUMNS)

def get_open_lots() -> List[dict]:
    """
    Return all OPEN stock lots that still hold shares.
    These are the ONLY valid sources for covered calls.
    """
    ensure_wheel_files()
    lots = _load_rows(WHEEL_LOTS_FILE)

    open_lots = []
    for lot in lots:
        if (lot.get("status") or "").upper() != "OPEN":
            continue
        try:
            shares = int(float(lot.get("shares") or 0))
        except Exception:
            continue
        if shares <= 0:
            continue
        open_lots.append(lot)

    return open_lots

def compute_wheel_exposure(today: dt.date) -> Dict[str, float]:
    """
    Exposure is computed from:
      - OPEN CSP rows in csp_positions.csv (cash_reserved)
      - OPEN lots in wheel_lots.csv (assigned_value)

    CCs do not add exposure (they're attached to lots).
    """
    ensure_wheel_files()

    week_id = _iso_week_id(today)

    # CSP exposure
    csp_rows = _load_rows(CSP_POSITIONS_FILE)
    csp_total = 0.0
    csp_week = 0.0
    for r in csp_rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        # If expiry invalid, conservative (treat as open)
        exp_str = (r.get("expiry") or "").strip()
        try:
            exp = dt.date.fromisoformat(exp_str)
            if exp < today:
                continue
        except Exception:
            pass
        cash = float(r.get("cash_reserved") or 0.0)
        csp_total += cash
        if (r.get("week_id") or "") == week_id:
            csp_week += cash

    # Stock lot exposure
    lots = _load_rows(WHEEL_LOTS_FILE)
    lot_total = 0.0
    lot_week = 0.0
    for lot in lots:
        if (lot.get("status") or "").upper() != "OPEN":
            continue
        val = float(lot.get("assigned_value") or 0.0)
        lot_total += val
        # treat lot's open_date for weekly bucket
        od = (lot.get("open_date") or "").strip()
        if od:
            try:
                if _iso_week_id(dt.date.fromisoformat(od)) == week_id:
                    lot_week += val
            except Exception:
                pass

    total = csp_total + lot_total
    week = csp_week + lot_week

    return {
        "week_id": week_id,
        "csp_exposure": csp_total,
        "stock_exposure": lot_total,
        "total_exposure": total,
        "week_exposure": week,
        "cap": float(WHEEL_CAP),
        "weekly_target": float(WHEEL_WEEKLY_TARGET),
    }

def compute_week_remaining(today: dt.date) -> float:
    exp = compute_wheel_exposure(today)
    return max(float(WHEEL_WEEKLY_TARGET) - float(exp["week_exposure"]), 0.0)


def create_lots_from_new_assignments(today: dt.date) -> List[dict]:
    """
    Convert newly ASSIGNED CSP positions into OPEN lots (one lot per CSP id),
    so stock exposure is tracked immediately.
    """
    ensure_wheel_files()

    csp_rows = _load_rows(CSP_POSITIONS_FILE)
    lots = _load_rows(WHEEL_LOTS_FILE)
    existing = { (l.get("csp_id") or "") for l in lots if (l.get("csp_id") or "") }

    created: List[dict] = []

    for r in csp_rows:
        if (r.get("status") or "").upper() != "ASSIGNED":
            continue
        csp_id = (r.get("id") or "").strip()
        if not csp_id or csp_id in existing:
            continue

        ticker = (r.get("ticker") or "").strip().upper()
        shares = int(float(r.get("shares_if_assigned") or 0))
        strike = float(r.get("strike") or 0.0)
        assigned_value = float(strike) * float(shares)

        lot_id = f"LOT-{csp_id}"
        lot = {
            "lot_id": lot_id,
            "ticker": ticker,
            "open_date": (r.get("close_date") or today.isoformat()),
            "shares": str(shares),
            "assigned_strike": f"{strike:.2f}",
            "assigned_value": f"{assigned_value:.2f}",
            "csp_id": csp_id,
            "cc_id": "",
            "status": "OPEN",
            "close_date": "",
            "called_away_strike": "",
            "called_away_value": "",
            "stock_pnl": "",
            "notes": "Created from CSP assignment",
        }
        lots.append(lot)
        created.append(lot)

        # Event: assignment creates stock exposure (and releases CSP collateral logically)
        try:
            prem = float(r.get("est_premium") or 0.0)
        except Exception:
            prem = 0.0
        record_event(
            date=(r.get("close_date") or today.isoformat()),
            ticker=ticker,
            event_type="CSP_ASSIGN",
            ref_id=csp_id,
            expiry=(r.get("expiry") or ""),
            strike=strike,
            contracts=int(float(r.get("contracts") or 0)),
            shares=shares,
            premium=prem,
            wheel_value=assigned_value,
            notes="Assigned -> stock lot created",
        )

    if created:
        _write_rows(WHEEL_LOTS_FILE, lots, WHEEL_LOTS_COLUMNS)

    return created


def link_new_ccs_to_lots(today: dt.date) -> List[Tuple[str, str]]:
    """
    Attach OPEN CC positions to an OPEN lot of the same ticker that doesn't already have a cc_id.
    Returns list of (cc_id, lot_id).
    """
    ensure_wheel_files()
    cc_rows = _load_rows(CC_POSITIONS_FILE)
    lots = _load_rows(WHEEL_LOTS_FILE)

    linked = []
    for cc in cc_rows:
        if (cc.get("status") or "").upper() != "OPEN":
            continue
        cc_id = (cc.get("id") or "").strip()
        if not cc_id:
            continue

        # already linked?
        already = any((l.get("cc_id") or "") == cc_id for l in lots)
        if already:
            continue

        tkr = (cc.get("ticker") or "").strip().upper()
        # pick first open lot with no cc_id
        for lot in lots:
            if (lot.get("status") or "").upper() != "OPEN":
                continue
            if (lot.get("ticker") or "").strip().upper() != tkr:
                continue
            if (lot.get("cc_id") or "").strip():
                continue
            lot["cc_id"] = cc_id
            lot["notes"] = (lot.get("notes") or "") + " | Linked CC"
            linked.append((cc_id, lot.get("lot_id") or ""))

            # Event: CC open (premium tracked, but wheel_value is 0)
            strike = float(cc.get("strike") or 0.0)
            contracts = int(float(cc.get("contracts") or 0))
            prem = float(cc.get("credit_mid") or 0.0) * 100.0 * contracts
            record_event(
                date=(cc.get("open_date") or today.isoformat()),
                ticker=tkr,
                event_type="CC_OPEN",
                ref_id=cc_id,
                expiry=(cc.get("expiry") or ""),
                strike=strike,
                contracts=contracts,
                shares=contracts * 100,
                premium=prem,
                wheel_value=0.0,
                notes="Covered call opened",
            )
            break

    if linked:
        _write_rows(WHEEL_LOTS_FILE, lots, WHEEL_LOTS_COLUMNS)

    return linked


def process_cc_expirations(today: dt.date) -> Dict[str, List[str]]:
    """
    For each OPEN CC whose expiry <= today, mark as EXPIRED or CALLED_AWAY using underlying close.
    If CALLED_AWAY, close the linked stock lot and compute stock PnL using assigned_strike.
    """
    ensure_wheel_files()

    cc_rows = _load_rows(CC_POSITIONS_FILE)
    lots = _load_rows(WHEEL_LOTS_FILE)

    changed_expired: List[str] = []
    changed_called: List[str] = []

    # build map cc_id -> lot
    lot_by_cc: Dict[str, dict] = {}
    for lot in lots:
        cc_id = (lot.get("cc_id") or "").strip()
        if cc_id and (lot.get("status") or "").upper() == "OPEN":
            lot_by_cc[cc_id] = lot

    for cc in cc_rows:
        if (cc.get("status") or "").upper() != "OPEN":
            continue

        exp_str = (cc.get("expiry") or "").strip()
        try:
            exp = dt.date.fromisoformat(exp_str)
        except Exception:
            continue
        if exp > today:
            continue

        ticker = (cc.get("ticker") or "").strip().upper()
        strike = float(cc.get("strike") or 0.0)
        contracts = int(float(cc.get("contracts") or 0))

        # get underlying close at/near expiry
        underlying_close = None
        try:
            start = (exp - dt.timedelta(days=7)).isoformat()
            end = (exp + dt.timedelta(days=1)).isoformat()
            df = yf.download(ticker, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
            df.dropna(inplace=True)
            if df.empty:
                continue
            if hasattr(df.columns, "levels"):  # multiindex guard
                try:
                    import pandas as pd
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.get_level_values(0)
                except Exception:
                    pass
            underlying_close = float(df["Close"].iloc[-1])
        except Exception:
            underlying_close = None

        if underlying_close is None:
            continue

        cc_id = (cc.get("id") or "").strip()
        cc["close_date"] = exp.isoformat()

        if underlying_close > strike:
            # called away
            cc["status"] = "CALLED_AWAY"
            cc["close_type"] = "ASSIGNED_ITM"
            changed_called.append(f"{ticker} {exp_str} {strike:.0f}C")

            # close the lot if linked
            lot = lot_by_cc.get(cc_id)
            if lot:
                shares = int(float(lot.get("shares") or 0))
                assigned_strike = float(lot.get("assigned_strike") or 0.0)
                proceeds = strike * shares
                cost = assigned_strike * shares
                stock_pnl = proceeds - cost

                lot["status"] = "CALLED_AWAY"
                lot["close_date"] = exp.isoformat()
                lot["called_away_strike"] = f"{strike:.2f}"
                lot["called_away_value"] = f"{proceeds:.2f}"
                lot["stock_pnl"] = f"{stock_pnl:.2f}"
                lot["notes"] = (lot.get("notes") or "") + " | Called away"

                record_event(
                    date=exp.isoformat(),
                    ticker=ticker,
                    event_type="CC_ASSIGN",
                    ref_id=cc_id,
                    expiry=exp_str,
                    strike=strike,
                    contracts=contracts,
                    shares=contracts * 100,
                    premium=0.0,
                    wheel_value=0.0,
                    notes=f"CC assigned; stock called away; stock_pnl={stock_pnl:.2f}",
                )
                record_event(
                    date=exp.isoformat(),
                    ticker=ticker,
                    event_type="STOCK_CALLED_AWAY",
                    ref_id=(lot.get("lot_id") or ""),
                    strike=strike,
                    shares=shares,
                    premium=0.0,
                    wheel_value=0.0,
                    notes=f"Proceeds {proceeds:.2f}, cost {cost:.2f}, pnl {stock_pnl:.2f}",
                )
        else:
            # expired
            cc["status"] = "EXPIRED"
            cc["close_type"] = "EXPIRED_OTM"
            changed_expired.append(f"{ticker} {exp_str} {strike:.0f}C")
            record_event(
                date=exp.isoformat(),
                ticker=ticker,
                event_type="CC_EXPIRE",
                ref_id=cc_id,
                expiry=exp_str,
                strike=strike,
                contracts=contracts,
                shares=contracts * 100,
                premium=0.0,
                wheel_value=0.0,
                notes="CC expired OTM",
            )

            # unlink lot's cc_id (so you can sell next CC)
            lot = lot_by_cc.get(cc_id)
            if lot:
                lot["cc_id"] = ""
                lot["notes"] = (lot.get("notes") or "") + " | CC expired"

    # persist
    if cc_rows:
        _write_rows(CC_POSITIONS_FILE, cc_rows, list(cc_rows[0].keys()) if cc_rows else [])
        # ^ If user has custom CC columns, preserve by using file header.
        #   But we don't have the original header here reliably. We'll do better below.

    # Safer: rewrite with existing header from file if present
    if os.path.isfile(CC_POSITIONS_FILE):
        with open(CC_POSITIONS_FILE, "r", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
        if header:
            _write_rows(CC_POSITIONS_FILE, cc_rows, header)

    _write_rows(WHEEL_LOTS_FILE, lots, WHEEL_LOTS_COLUMNS)

    return {"expired": changed_expired, "called_away": changed_called}


def rebuild_monthly_from_events() -> None:
    """
    Build ONE CSV PER MONTH under wheel_monthly/YYYY-MM.csv.
    Each file lists events and has a TOTALS row.
    """
    ensure_wheel_files()
    rows = _load_rows(WHEEL_EVENTS_FILE)
    if not rows:
        return

    os.makedirs(WHEEL_MONTHLY_DIR, exist_ok=True)

    out_fields = [
        "date", "ticker", "event_type", "expiry", "strike", "contracts", "shares",
        "premium", "wheel_value", "notes"
    ]

    by_month: Dict[str, List[dict]] = {}
    for r in rows:
        d = (r.get("date") or "").strip()
        if len(d) < 7:
            continue
        month = d[:7]
        by_month.setdefault(month, []).append(r)

    for month, mrows in by_month.items():
        mrows_sorted = sorted(mrows, key=lambda x: ((x.get("date") or ""), (x.get("ticker") or ""), (x.get("event_type") or "")))

        total_premium = 0.0
        total_stock_pnl = 0.0  # from notes if present; optional

        out = []
        for r in mrows_sorted:
            prem = float(r.get("premium") or 0.0)
            total_premium += prem

            # attempt extract stock_pnl from notes
            notes = (r.get("notes") or "")
            if "stock_pnl=" in notes:
                try:
                    total_stock_pnl += float(notes.split("stock_pnl=")[-1].split()[0].strip().replace(",", ""))
                except Exception:
                    pass

            out.append({
                "date": (r.get("date") or "").strip(),
                "ticker": (r.get("ticker") or "").strip(),
                "event_type": (r.get("event_type") or "").strip(),
                "expiry": (r.get("expiry") or "").strip(),
                "strike": (r.get("strike") or "").strip(),
                "contracts": (r.get("contracts") or "").strip(),
                "shares": (r.get("shares") or "").strip(),
                "premium": f"{prem:.2f}" if prem else "",
                "wheel_value": (r.get("wheel_value") or "").strip(),
                "notes": notes,
            })

        out.append({
            "date": "",
            "ticker": "TOTALS",
            "event_type": "",
            "expiry": "",
            "strike": "",
            "contracts": "",
            "shares": "",
            "premium": f"{total_premium:.2f}",
            "wheel_value": "",
            "notes": f"stock_pnl_sum={total_stock_pnl:.2f}",
        })

        _write_rows(os.path.join(WHEEL_MONTHLY_DIR, f"{month}.csv"), out, out_fields)
