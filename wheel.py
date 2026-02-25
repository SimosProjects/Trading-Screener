"""
wheel.py

Lightweight 'institutional wheel' bookkeeping.

Files (configured in config.py):
  - wheel_events.csv : append-only event log (CSP/CC opens, expiries, assignments, called-away)
  - wheel_lots.csv   : current stock lots created by CSP assignment (used to drive CC ideas)
  - wheel_monthly/   : one CSV per month, rebuilt from wheel_events.csv
"""

from __future__ import annotations

import csv
import datetime as dt
import os
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf

from utils import get_logger, iso_week_id, safe_float, safe_int, atomic_write
from config import (
    WHEEL_EVENTS_FILE,
    WHEEL_LOTS_FILE,
    WHEEL_MONTHLY_DIR,
    WHEEL_CAP,
    WHEEL_WEEKLY_TARGET,
    CSP_POSITIONS_FILE,
    CC_POSITIONS_FILE,
)

log = get_logger(__name__)

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
    "cost_basis",           # total cost basis dollars (strike*shares - csp_premium)
    "source_csp_id",
    "has_open_cc",
    "cc_id",
    "status",               # OPEN / CLOSED
    "cc_premium_collected", # running total of CC premiums closed on this lot (dollars)
    "net_cost_basis",       # cost_basis - cc_premium_collected; what decide_cc_strike uses
]


# ----------------------------
# Helpers
# ----------------------------

def _iso_week_id(d: dt.date) -> str:
    # Thin alias — canonical implementation lives in utils.iso_week_id.
    return iso_week_id(d)


def _ensure_trailing_newline(path: str) -> None:
    """Ensure an existing file ends with a newline so subsequent appends can't glue rows together."""
    try:
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            return
        with open(path, "rb") as fb:
            fb.seek(-1, os.SEEK_END)
            last = fb.read(1)
        if last != b"\n":
            with open(path, "a", newline="") as fa:
                fa.write("\n")
    except Exception:
        # Non-fatal; worst case we behave like before.
        return


def _read_rows(path: str) -> List[dict]:
    """Read CSV rows safely.

    - Returns [] if file missing
    - Skips malformed rows that don't match header column count (prevents crashes)
    """
    if not os.path.isfile(path):
        return []

    rows: List[dict] = []
    try:
        with open(path, "r", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return []
            header = [h.strip() for h in header]

            for parts in reader:
                # Skip completely empty lines
                if not parts or all((p or "").strip() == "" for p in parts):
                    continue
                # Malformed row (often caused by glued lines) -> skip to avoid poisoning downstream
                if len(parts) != len(header):
                    continue
                rows.append({header[i]: parts[i] for i in range(len(header))})
    except Exception:
        # Fall back to DictReader if something odd happened; still might fail if file is corrupted
        try:
            with open(path, "r", newline="") as f:
                return list(csv.DictReader(f))
        except Exception:
            return []

    return rows


def _write_rows(path: str, rows: List[dict], fieldnames: List[str]) -> None:
    """Atomic write: new content is staged in a .tmp file then renamed into place."""
    def _write(f):
        w = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
            lineterminator="\n",
        )
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    atomic_write(path, _write)


def ensure_wheel_files() -> None:
    # Ensure directory exists
    base_dir = os.path.dirname(os.path.abspath(WHEEL_EVENTS_FILE))
    os.makedirs(base_dir, exist_ok=True)

    if not os.path.isfile(WHEEL_EVENTS_FILE):
        with open(WHEEL_EVENTS_FILE, "w", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=EVENT_FIELDS,
                extrasaction="ignore",
                lineterminator="\n",
            )
            w.writeheader()

    if not os.path.isfile(WHEEL_LOTS_FILE):
        with open(WHEEL_LOTS_FILE, "w", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=LOT_FIELDS,
                extrasaction="ignore",
                lineterminator="\n",
            )
            w.writeheader()


def _safe_float(v: object, default: float = 0.0) -> float:
    return safe_float(v, default)


def _safe_int(v: object, default: int = 0) -> int:
    return safe_int(v, default)


# ----------------------------
# Events
# ----------------------------

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

    # Deterministic event_id so rerunning is idempotent
    row["event_id"] = (
        (kwargs.get("event_id") or "").strip()
        or f"{date_str}-{ticker_norm}-{event_type_norm}-{ref_norm}"
    )
    row["date"] = date_str
    row["week_id"] = (kwargs.get("week_id") or "").strip() or _iso_week_id(d)
    row["ticker"] = ticker_norm
    row["event_type"] = event_type_norm
    row["ref_id"] = ref_norm
    row["expiry"] = (kwargs.get("expiry") or "").strip()

    row["strike"] = f"{_safe_float(kwargs.get('strike'), 0.0):.2f}"
    row["contracts"] = str(_safe_int(kwargs.get("contracts"), 0))
    row["shares"] = str(_safe_int(kwargs.get("shares"), 0))
    row["premium"] = f"{_safe_float(kwargs.get('premium'), 0.0):.2f}"
    row["wheel_value"] = f"{_safe_float(kwargs.get('wheel_value'), 0.0):.2f}"
    row["notes"] = (kwargs.get("notes") or "").strip()

    # Idempotency: don't append the same event twice
    try:
        existing = _read_rows(WHEEL_EVENTS_FILE)
        if any((r.get("event_id") or "") == row["event_id"] for r in existing):
            return
    except Exception as e:
        # Non-fatal but worth knowing — if this fails we may write a duplicate event.
        log.warning("wheel event idempotency check failed (event_id=%s): %s", row["event_id"], e)

    # Critical: avoid "glued rows" if file lacks trailing newline
    _ensure_trailing_newline(WHEEL_EVENTS_FILE)

    with open(WHEEL_EVENTS_FILE, "a", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=EVENT_FIELDS,
            extrasaction="ignore",
            lineterminator="\n",
        )
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

        strike = _safe_float(r.get("strike"), 0.0)
        shares = _safe_int(r.get("shares_if_assigned"), 0)
        basis = _safe_float(r.get("assignment_cost_basis"), 0.0)

        if shares <= 0 or strike <= 0:
            continue

        lot_id = _make_lot_id(ticker, open_date, strike)
        lots.append(
            {
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
                "cc_premium_collected": "0.00",
                "net_cost_basis": f"{basis:.2f}",
            }
        )

        record_event(
            date=open_date,
            ticker=ticker,
            event_type="CSP_ASSIGNED",
            ref_id=lot_id,
            expiry=(r.get("expiry") or ""),
            strike=strike,
            contracts=_safe_int(r.get("contracts"), 0),
            shares=shares,
            premium=_safe_float(r.get("premium") or r.get("est_premium") or 0.0, 0.0),
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

        # Premium correctness:
        # In this project, cc_positions.csv stores TOTAL premium dollars as "premium".
        # Do NOT recompute as credit_mid * 100 * contracts unless premium is missing.
        prem = 0.0
        if cc.get("premium") not in (None, "", "NaN", "nan"):
            prem = _safe_float(cc.get("premium"), 0.0)
        else:
            prem = _safe_float(cc.get("credit_mid"), 0.0) * 100.0 * _safe_float(cc.get("contracts"), 0.0)

        record_event(
            date=(cc.get("open_date") or today.isoformat()),
            ticker=t,
            event_type="CC_OPEN",
            ref_id=cc_id,  # FIX: was a typo in prior file
            expiry=(cc.get("expiry") or ""),
            strike=_safe_float(cc.get("strike"), 0.0),
            contracts=_safe_int(cc.get("contracts"), 0),
            shares=_safe_int(cc.get("contracts"), 0) * 100,
            premium=prem,
            wheel_value=0.0,
            notes=f"Linked CC {cc_id} to lot {lot.get('lot_id', '')}",
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
        strike = _safe_float(r.get("strike"), 0.0)

        underlying_close: Optional[float] = None
        try:
            # Same fix as CSP path: filter to exact expiry date rather than using
            # iloc[-1], which can return Monday's data for a Friday-expiry contract.
            start = (exp - dt.timedelta(days=7)).isoformat()
            end   = (exp + dt.timedelta(days=2)).isoformat()
            df = yf.download(tkr, start=start, end=end, interval="1d",
                             auto_adjust=False, progress=False)
            df.dropna(inplace=True)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            if not df.empty:
                df.index = pd.to_datetime(df.index)
                exp_ts = pd.Timestamp(exp)
                exact = df[df.index.normalize() == exp_ts]
                if not exact.empty:
                    underlying_close = float(exact["Close"].iloc[-1])
                else:
                    # Holiday fallback: use nearest prior close.
                    prior = df[df.index.normalize() < exp_ts]
                    if not prior.empty:
                        underlying_close = float(prior["Close"].iloc[-1])
        except Exception as e:
            log.warning("CC expiry price fetch failed for %s exp %s: %s", tkr, exp_str, e)
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
                contracts=_safe_int(r.get("contracts"), 0),
                shares=_safe_int(r.get("contracts"), 0) * 100,
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
                contracts=_safe_int(r.get("contracts"), 0),
                shares=_safe_int(r.get("contracts"), 0) * 100,
                premium=0.0,
                wheel_value=0.0,
                notes="CC expired (best-effort inference)",
            )

        changed = True

    if changed:
        # Keep original headers if present, else fall back
        fieldnames = list(cc_rows[0].keys()) if cc_rows else []
        _write_rows(CC_POSITIONS_FILE, cc_rows, fieldnames)

    # Update lots for any CC that closed this run.
    # - EXPIRED_OTM  → reduce cost basis by the premium collected; keep lot OPEN
    # - CALLED_AWAY  → close the lot entirely (shares sold at strike)
    if expired or called:
        lots = _read_rows(WHEEL_LOTS_FILE)
        lot_changed = False

        # Build lookup: cc_id → closed cc row (for premium)
        closed_cc_by_id: Dict[str, dict] = {
            (r.get("id") or ""): r
            for r in cc_rows
            if (r.get("status") or "").upper() in ("EXPIRED", "CALLED_AWAY")
        }
        called_cc_ids = {
            (r.get("id") or "")
            for r in cc_rows
            if (r.get("status") or "").upper() == "CALLED_AWAY"
        }

        for lot in lots:
            if (lot.get("status") or "").upper() != "OPEN":
                continue
            cc_id = (lot.get("cc_id") or "").strip()
            if not cc_id or cc_id not in closed_cc_by_id:
                continue

            cc_row = closed_cc_by_id[cc_id]
            prem = _safe_float(cc_row.get("premium"), 0.0)

            if cc_id in called_cc_ids:
                # Stock called away — close the lot, no basis update needed
                lot["status"] = "CLOSED"
                lot["has_open_cc"] = "0"
            else:
                # CC expired worthless — collect the premium, reduce basis, clear CC link
                prev_collected = _safe_float(lot.get("cc_premium_collected"), 0.0)
                new_collected   = prev_collected + prem
                orig_basis      = _safe_float(lot.get("cost_basis"), 0.0)
                new_net_basis   = max(orig_basis - new_collected, 0.0)

                lot["cc_premium_collected"] = f"{new_collected:.2f}"
                lot["net_cost_basis"]        = f"{new_net_basis:.2f}"
                lot["has_open_cc"]           = "0"
                lot["cc_id"]                 = ""

                tkr_lot = (lot.get("ticker") or "").strip().upper()
                sh_lot  = _safe_int(lot.get("shares"), 0)
                net_per_share = new_net_basis / sh_lot if sh_lot > 0 else 0.0
                log.info(
                    "CC expired — lot %s basis reduced: collected $%.2f total, "
                    "net_cost_basis $%.2f ($%.4f/sh)",
                    lot.get("lot_id", tkr_lot), new_collected, new_net_basis, net_per_share,
                )

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

        total += _safe_float(r.get("cash_reserved"), 0.0)

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
        strike = _safe_float(lot.get("assigned_strike"), 0.0)
        shares = _safe_float(lot.get("shares"), 0.0)
        total += strike * shares

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
        week_used += _safe_float(r.get("cash_reserved"), 0.0)

    return max(float(WHEEL_WEEKLY_TARGET) - week_used, 0.0)


# ----------------------------
# Monthly rebuild
# ----------------------------

def rebuild_monthly_from_events() -> None:
    """Rebuild one CSV per month from wheel_events.csv.

    Tracks premium credits (CSP_OPEN, CC_OPEN) and buyback costs
    (CSP_CLOSE_TP) so the monthly total reflects actual net premium earned.
    CSP_CLOSE_TP premiums are stored as negative dollars (cash paid out).
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
        evs_sorted = sorted(
            evs, key=lambda x: ((x.get("date") or ""), (x.get("ticker") or ""))
        )

        out_rows: List[dict] = []
        total = 0.0
        for e in evs_sorted:
            et = (e.get("event_type") or "").upper()
            if et not in ("CSP_OPEN", "CC_OPEN", "CSP_CLOSE_TP"):
                continue
            prem = _safe_float(e.get("premium"), 0.0)
            total += prem
            out_rows.append(
                {
                    "date": (e.get("date") or "").strip(),
                    "ticker": (e.get("ticker") or "").strip(),
                    "event_type": et,
                    "ref_id": (e.get("ref_id") or "").strip(),
                    "premium": f"{prem:.2f}",
                }
            )

        out_rows.append(
            {"date": "", "ticker": "TOTAL", "event_type": "", "ref_id": "", "premium": f"{total:.2f}"}
        )

        path = os.path.join(WHEEL_MONTHLY_DIR, f"{month}.csv")
        _write_rows(path, out_rows, out_fields)


# ----------------------------
# Backfill (only if needed)
# ----------------------------

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

        prem = _safe_float(r.get("premium") or r.get("est_premium") or 0.0, 0.0)
        # If premium was stored per-contract, try multiplying (legacy compatibility)
        if prem <= 0:
            prem = _safe_float(r.get("premium"), 0.0) * _safe_float(r.get("contracts"), 0.0)

        record_event(
            date=(r.get("open_date") or today.isoformat()),
            ticker=(r.get("ticker") or ""),
            event_type="CSP_OPEN",
            ref_id=ref,
            expiry=(r.get("expiry") or ""),
            strike=_safe_float(r.get("strike"), 0.0),
            contracts=_safe_int(r.get("contracts"), 0),
            shares=_safe_int(r.get("contracts"), 0) * 100,
            premium=prem,
            wheel_value=_safe_float(r.get("cash_reserved"), 0.0),
            notes="Backfilled from csp_positions.csv",
        )

    # CC opens
    for r in _read_rows(CC_POSITIONS_FILE):
        if (r.get("status") or "").upper() != "OPEN":
            continue
        ref = (r.get("id") or "").strip()
        if not ref or ("CC_OPEN", ref) in existing:
            continue

        # IMPORTANT: cc_positions.csv "premium" is already TOTAL dollars (not per-contract).
        prem = _safe_float(r.get("premium"), 0.0)

        record_event(
            date=(r.get("open_date") or today.isoformat()),
            ticker=(r.get("ticker") or ""),
            event_type="CC_OPEN",
            ref_id=ref,
            expiry=(r.get("expiry") or ""),
            strike=_safe_float(r.get("strike"), 0.0),
            contracts=_safe_int(r.get("contracts"), 0),
            shares=_safe_int(r.get("contracts"), 0) * 100,
            premium=prem,
            wheel_value=0.0,
            notes="Backfilled from cc_positions.csv",
        )