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
    INDIVIDUAL,
    IRA_ACCOUNTS,
    WHEEL_EVENTS_FILE,
    WHEEL_LOTS_FILE,
    WHEEL_MONTHLY_DIR,
    WHEEL_CAPS,
    WHEEL_WEEKLY_TARGETS,
    CSP_POSITIONS_FILE,
    CC_POSITIONS_FILE,
    STOCK_TRADES_FILE,
)

log = get_logger(__name__)

# Module-level cache reference — injected by screener.py at startup via
# set_data_cache().  Falls back to direct yf.download when None.
_cache = None


def set_data_cache(cache) -> None:
    """Inject the pre-warmed DataCache for this run."""
    global _cache
    _cache = cache


def _cached_ohlcv(ticker: str) -> "pd.DataFrame":
    """Return cached OHLCV if available, else empty DataFrame."""
    if _cache is not None and _cache.has(ticker):
        return _cache.ohlcv(ticker)
    return pd.DataFrame()

EVENT_FIELDS = [
    "event_id",
    "account",
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
    "account",
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
    except Exception as e:
        log.warning("_read_rows: primary CSV read failed for %s, trying DictReader fallback: %s", path, e)
        # Fall back to DictReader if something odd happened; still might fail if file is corrupted
        try:
            with open(path, "r", newline="") as f:
                return list(csv.DictReader(f))
        except Exception as e2:
            log.error("_read_rows: DictReader fallback also failed for %s: %s", path, e2)
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



def _append_stock_trade_record(row: dict) -> None:
    """Append one row to stock_trades.csv from wheel.py without importing strategies.

    Keeps wheel.py free of a circular import.  Field order matches
    STOCK_TRADE_FIELDS in strategies.py — any new fields added there
    should be mirrored here.
    """
    fields = [
        "id", "account", "ticker", "entry_date", "entry_price",
        "shares", "exit_date", "exit_price", "reason", "close_type",
        "pnl_abs", "pnl_pct",
    ]
    file_exists = os.path.isfile(STOCK_TRADES_FILE)
    # Ensure trailing newline before appending — prevents row-gluing if the
    # last write was interrupted before the newline was flushed (same fix
    # applied to append_csp_ledger_row in Session 10).
    if file_exists:
        with open(STOCK_TRADES_FILE, "rb+") as f:
            f.seek(0, 2)
            if f.tell() > 0:
                f.seek(-1, 2)
                if f.read(1) != b"\n":
                    f.write(b"\n")
    with open(STOCK_TRADES_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        if not file_exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in fields})

# ----------------------------
# Events
# ----------------------------

def record_event(**kwargs) -> None:
    ensure_wheel_files()

    date_str = (kwargs.get("date") or "").strip() or dt.date.today().isoformat()
    try:
        d = dt.date.fromisoformat(date_str)
    except Exception as e:
        log.warning("record_event: bad date %r for %s/%s, using today: %s",
                    date_str, kwargs.get("ticker", "?"), kwargs.get("event_type", "?"), e)
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
    row["account"] = (kwargs.get("account") or INDIVIDUAL).strip().upper()
    row["date"] = date_str
    row["week_id"] = (kwargs.get("week_id") or "").strip() or iso_week_id(d)
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
        # Inherit account from the parent CSP; default to INDIVIDUAL for pre-4b rows.
        acct = (r.get("account") or INDIVIDUAL).strip().upper()

        strike = _safe_float(r.get("strike"), 0.0)
        shares = _safe_int(r.get("shares_if_assigned"), 0)
        basis = _safe_float(r.get("assignment_cost_basis"), 0.0)

        if shares <= 0 or strike <= 0:
            continue

        lot_id = _make_lot_id(ticker, open_date, strike)
        lots.append(
            {
                "lot_id": lot_id,
                "account": acct,
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
            account=acct,
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
    """Attach OPEN CCs in cc_positions.csv to their OPEN lots.

    Matching priority:
      1. source_lot_id (exact lot match) — used for all new CCs that carry
         the lot_id from plan_ccs_from_open_lots.
      2. ticker-only fallback — for legacy CC rows written before source_lot_id
         was introduced.  Only applied when exactly one unlinked lot exists for
         that ticker to avoid ambiguous assignments.
    """
    ensure_wheel_files()
    lots = _read_rows(WHEEL_LOTS_FILE)
    if not lots:
        return

    cc_rows = _read_rows(CC_POSITIONS_FILE)
    open_ccs = [r for r in cc_rows if (r.get("status") or "").upper() == "OPEN"]

    # Build lookup: source_lot_id → cc row (exact match)
    cc_by_lot_id: Dict[str, dict] = {}
    for r in open_ccs:
        lid = (r.get("source_lot_id") or "").strip()
        if lid:
            cc_by_lot_id[lid] = r

    # Build fallback: ticker → [cc rows without a source_lot_id]
    legacy_ccs_by_ticker: Dict[str, List[dict]] = {}
    for r in open_ccs:
        if (r.get("source_lot_id") or "").strip():
            continue  # already handled by exact-match path
        t = (r.get("ticker") or "").strip().upper()
        if t:
            legacy_ccs_by_ticker.setdefault(t, []).append(r)

    changed = False
    for lot in lots:
        if (lot.get("status") or "").upper() != "OPEN":
            continue
        if (lot.get("has_open_cc") or "").strip() in ("1", "true", "TRUE"):
            continue

        lot_id = (lot.get("lot_id") or "").strip()
        t = (lot.get("ticker") or "").strip().upper()
        if not t:
            continue

        # Path 1: exact lot match via source_lot_id
        cc = cc_by_lot_id.get(lot_id) if lot_id else None

        # Path 2: legacy ticker fallback — only safe when there's exactly one
        # unmatched CC for this ticker (avoids linking the wrong CC to the wrong lot)
        if cc is None:
            candidates = legacy_ccs_by_ticker.get(t, [])
            if len(candidates) == 1:
                cc = candidates[0]
            elif len(candidates) > 1:
                log.warning(
                    "link_new_ccs_to_lots: %d legacy CCs for ticker %s with no lot_id; "
                    "cannot safely link — add source_lot_id to CC records to resolve",
                    len(candidates), t,
                )

        if cc is None:
            continue

        cc_id = (cc.get("id") or "").strip()
        if not cc_id:
            continue

        lot["has_open_cc"] = "1"
        lot["cc_id"] = cc_id
        changed = True

        # Premium correctness:
        # cc_positions.csv stores TOTAL premium dollars as "premium".
        # Do NOT recompute as credit_mid * 100 * contracts unless premium is missing.
        prem = 0.0
        if cc.get("premium") not in (None, "", "NaN", "nan"):
            prem = _safe_float(cc.get("premium"), 0.0)
        else:
            prem = _safe_float(cc.get("credit_mid"), 0.0) * 100.0 * _safe_float(cc.get("contracts"), 0.0)

        record_event(
            date=(cc.get("open_date") or today.isoformat()),
            account=(cc.get("account") or INDIVIDUAL).strip().upper(),
            ticker=t,
            event_type="CC_OPEN",
            ref_id=cc_id,
            expiry=(cc.get("expiry") or ""),
            strike=_safe_float(cc.get("strike"), 0.0),
            contracts=_safe_int(cc.get("contracts"), 0),
            shares=_safe_int(cc.get("contracts"), 0) * 100,
            premium=prem,
            wheel_value=0.0,
            notes=f"Linked CC {cc_id} to lot {lot_id or t}",
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
        except Exception as e:
            log.warning("process_cc_expirations: bad expiry %r for %s: %s",
                        exp_str, r.get("ticker", "?"), e)
            continue

        if exp > today:
            continue

        tkr = (r.get("ticker") or "").strip().upper()
        strike = _safe_float(r.get("strike"), 0.0)

        underlying_close: Optional[float] = None
        try:
            # Try the session cache first (covers ~1 year of daily data).
            # Fall back to a targeted network fetch only when the expiry date
            # predates the cache window.
            cached_df = _cached_ohlcv(tkr)
            if not cached_df.empty:
                cached_df.index = pd.to_datetime(cached_df.index)
                exp_ts = pd.Timestamp(exp)
                exact = cached_df[cached_df.index.normalize() == exp_ts]
                if not exact.empty:
                    underlying_close = float(exact["Close"].iloc[-1])
                else:
                    prior = cached_df[cached_df.index.normalize() < exp_ts]
                    if not prior.empty and prior.index[-1].date() >= (exp - dt.timedelta(days=5)):
                        underlying_close = float(prior["Close"].iloc[-1])

            if underlying_close is None:
                # Cache miss — targeted fetch around expiry date.
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
                account=(r.get("account") or INDIVIDUAL).strip().upper(),
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
                account=(r.get("account") or INDIVIDUAL).strip().upper(),
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

            # Detect corrupted state: lot thinks it has an open CC but has no cc_id.
            # This means process_cc_expirations would silently skip the lot update,
            # leaving the lot permanently blocked from new CC cycles.
            if (lot.get("has_open_cc") or "").strip() in ("1", "true", "TRUE") and not cc_id:
                log.warning(
                    "Lot %s (%s) has has_open_cc=1 but cc_id is empty — "
                    "lot is stuck; manually clear has_open_cc or re-link cc_id to unblock.",
                    lot.get("lot_id", "?"), lot.get("ticker", "?"),
                )

            if not cc_id or cc_id not in closed_cc_by_id:
                continue

            cc_row = closed_cc_by_id[cc_id]
            prem = _safe_float(cc_row.get("premium"), 0.0)

            if cc_id in called_cc_ids:
                # Stock called away — close the lot and record realized P&L
                # on the call-away date so it flows into the stock monthly.
                lot["status"] = "CLOSED"
                lot["has_open_cc"] = "0"

                tkr_lot   = (lot.get("ticker") or "").strip().upper()
                sh_lot    = _safe_int(lot.get("shares"), 0)
                cc_row    = closed_cc_by_id[cc_id]
                cc_strike = _safe_float(cc_row.get("strike"), 0.0)
                cc_exp    = (cc_row.get("expiry") or "").strip()
                cc_acct   = (cc_row.get("account") or INDIVIDUAL).strip().upper()
                net_basis = _safe_float(lot.get("cost_basis"), 0.0)
                proceeds  = cc_strike * sh_lot
                pnl_abs   = proceeds - net_basis
                pnl_pct   = (pnl_abs / net_basis * 100.0) if net_basis > 0 else 0.0
                entry_price_per_sh = (net_basis / sh_lot) if sh_lot > 0 else 0.0

                if sh_lot > 0 and cc_strike > 0:
                    _append_stock_trade_record({
                        "id":          f"{tkr_lot}-{cc_exp}-CALLED_AWAY",
                        "account":     cc_acct,
                        "ticker":      tkr_lot,
                        "entry_date":  (lot.get("open_date") or ""),
                        "entry_price": f"{entry_price_per_sh:.4f}",
                        "shares":      str(sh_lot),
                        "exit_date":   cc_exp,
                        "exit_price":  f"{cc_strike:.2f}",
                        "reason":      "CC_CALLED_AWAY",
                        "close_type":  "CC_CALLED_AWAY",
                        "pnl_abs":     f"{pnl_abs:.2f}",
                        "pnl_pct":     f"{pnl_pct:.2f}",
                    })
                    log.info(
                        "CC called away — %s %s sh, proceeds $%.2f, "
                        "net_basis $%.2f, P&L $%.2f (%.1f%%)",
                        tkr_lot, sh_lot, proceeds, net_basis, pnl_abs, pnl_pct,
                    )
            else:
                # CC expired worthless — collect the premium, reduce basis, clear CC link.
                # net_cost_basis is always derived as (cost_basis - cc_premium_collected)
                # so it self-corrects even if either field was edited manually.
                prev_collected  = _safe_float(lot.get("cc_premium_collected"), 0.0)
                new_collected   = prev_collected + prem
                orig_basis      = _safe_float(lot.get("cost_basis"), 0.0)
                new_net_basis   = max(orig_basis - new_collected, 0.0)

                lot["cc_premium_collected"] = f"{new_collected:.2f}"
                lot["net_cost_basis"]       = f"{new_net_basis:.2f}"   # formula: cost_basis - cc_premium_collected
                lot["has_open_cc"]          = "0"
                lot["cc_id"]               = ""

                tkr_lot = (lot.get("ticker") or "").strip().upper()
                sh_lot  = _safe_int(lot.get("shares"), 0)
                net_per_share = new_net_basis / sh_lot if sh_lot > 0 else 0.0
                log.info(
                    "CC expired — lot %s: +$%.2f collected (total $%.2f), "
                    "net_cost_basis $%.2f ($%.4f/sh) [formula: $%.2f cost_basis - $%.2f collected]",
                    lot.get("lot_id", tkr_lot), prem, new_collected,
                    new_net_basis, net_per_share, orig_basis, new_collected,
                )

            lot_changed = True

        if lot_changed:
            _write_rows(WHEEL_LOTS_FILE, lots, LOT_FIELDS)

    return {"expired": expired, "called_away": called}


# ----------------------------
# Exposure + weekly remaining
# ----------------------------

def compute_wheel_exposure(
    today: dt.date,
    account: str = INDIVIDUAL,
) -> Dict[str, float | int | str]:
    """Compute wheel exposure for one account.

    Defaults to INDIVIDUAL so all existing callers continue to work unchanged.
    Steps 4d+ will call this once per account to get per-account caps/remaining.

    Rows that pre-date Step 4b (no 'account' column) are treated as INDIVIDUAL
    so legacy data never falls through the filter silently.
    """
    ensure_wheel_files()

    acct = account.strip().upper()
    total = 0.0
    aggressive_total = 0
    aggressive_week = 0
    week_id = iso_week_id(today)

    # CSP collateral — only positions belonging to this account
    csp_rows = _read_rows(CSP_POSITIONS_FILE)
    for r in csp_rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        # Rows without an account column (pre-4b files) default to INDIVIDUAL
        row_acct = (r.get("account") or INDIVIDUAL).strip().upper()
        if row_acct != acct:
            continue
        try:
            exp = dt.date.fromisoformat((r.get("expiry") or "").strip())
            if exp < today:
                continue
        except Exception as e:
            log.debug("compute_wheel_exposure: bad expiry for %s %s: %s",
                      acct, r.get("ticker", "?"), e)
            pass

        total += _safe_float(r.get("cash_reserved"), 0.0)

        tier = (r.get("tier") or "").upper()
        if tier == "AGGRESSIVE":
            aggressive_total += 1
            if (r.get("week_id") or "") == week_id:
                aggressive_week += 1

    # Assigned lots (notional) — only lots belonging to this account
    lots = _read_rows(WHEEL_LOTS_FILE)
    for lot in lots:
        if (lot.get("status") or "").upper() != "OPEN":
            continue
        lot_acct = (lot.get("account") or INDIVIDUAL).strip().upper()
        if lot_acct != acct:
            continue
        strike = _safe_float(lot.get("assigned_strike"), 0.0)
        shares = _safe_float(lot.get("shares"), 0.0)
        total += strike * shares

    return {
        "account": acct,
        "week_id": week_id,
        "cap": float(WHEEL_CAPS[acct]),
        "weekly_target": float(WHEEL_WEEKLY_TARGETS[acct]),
        "total_exposure": float(total),
        "aggressive_total": int(aggressive_total),
        "aggressive_week": int(aggressive_week),
    }


def compute_week_remaining(today: dt.date, account: str = INDIVIDUAL) -> float:
    """Weekly new-entry capacity remaining for one account this ISO week.

    Defaults to INDIVIDUAL so all existing callers continue to work unchanged.
    """
    acct = account.strip().upper()
    exp = compute_wheel_exposure(today, acct)
    week_id = exp["week_id"]

    week_used = 0.0
    csp_rows = _read_rows(CSP_POSITIONS_FILE)
    for r in csp_rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        if (r.get("week_id") or "") != week_id:
            continue
        row_acct = (r.get("account") or INDIVIDUAL).strip().upper()
        if row_acct != acct:
            continue
        week_used += _safe_float(r.get("cash_reserved"), 0.0)

    return max(float(WHEEL_WEEKLY_TARGETS[acct]) - week_used, 0.0)


# ----------------------------
# Monthly rebuild
# ----------------------------

def rebuild_monthly_from_events() -> None:
    """Rebuild per-account-group monthly wheel CSVs from wheel_events.csv.

    Produces two files per month:
      wheel_monthly/YYYY-MM-INDIVIDUAL.csv
      wheel_monthly/YYYY-MM-IRA.csv  (IRA + ROTH combined, each row tagged)

    Columns:
      date, account, ticker, event_type, amount, net, notes

    - amount  : cash flow for this event (+premium collected, -buyback paid)
    - net     : realised net on the closing row of a paired trade (open+close);
                blank on open rows so you can see both sides but net at a glance
    - event_type labels:
        CSP_OPEN       premium collected when put was sold
        CC_OPEN        premium collected when call was sold
        CSP_CLOSE_TP   buyback cost (negative) when closed early for profit
        CSP_EXPIRED    $0 event; confirms expiry with no cost
        CC_EXPIRED     $0 event; confirms expiry with no cost
    """
    ensure_wheel_files()
    rows = _read_rows(WHEEL_EVENTS_FILE)
    if not rows:
        return

    os.makedirs(WHEEL_MONTHLY_DIR, exist_ok=True)

    # Relevant event types for the premium income statement
    INCOME_TYPES = {"CSP_OPEN", "CC_OPEN", "CSP_CLOSE_TP", "CC_CLOSE_TP", "CSP_EXPIRED", "CC_EXPIRED", "CC_MANUAL_EXIT_BUYBACK"}

    # Build a lookup of open-event amounts keyed by ref_id so we can compute
    # net profit on close/expiry rows without a second pass.
    open_premiums: Dict[str, float] = {}
    for r in rows:
        et = (r.get("event_type") or "").upper()
        if et in ("CSP_OPEN", "CC_OPEN"):
            ref = (r.get("ref_id") or "").strip()
            if ref:
                open_premiums[ref] = _safe_float(r.get("premium"), 0.0)

    out_fields = ["date", "account", "ticker", "event_type", "amount", "net", "notes"]

    # Bucket rows by (month, file_group)
    by_bucket: Dict[tuple, List[dict]] = {}
    for r in rows:
        et = (r.get("event_type") or "").upper()
        if et not in INCOME_TYPES:
            continue
        d = (r.get("date") or "").strip()
        if len(d) < 7:
            continue
        month  = d[:7]
        acct   = (r.get("account") or INDIVIDUAL).strip().upper()
        group  = "IRA" if acct in IRA_ACCOUNTS else "INDIVIDUAL"
        by_bucket.setdefault((month, group), []).append(r)

    for (month, group), evs in sorted(by_bucket.items()):
        evs_sorted = sorted(evs, key=lambda x: (x.get("date") or "", x.get("ticker") or ""))

        out_rows: List[dict] = []
        total = 0.0

        for e in evs_sorted:
            et     = (e.get("event_type") or "").upper()
            ref    = (e.get("ref_id") or "").strip()
            tkr    = (e.get("ticker") or "").strip().upper()
            acct   = (e.get("account") or INDIVIDUAL).strip().upper()
            prem   = _safe_float(e.get("premium"), 0.0)

            # amount: positive for income, negative for cost
            if et in ("CSP_CLOSE_TP", "CC_CLOSE_TP", "CC_MANUAL_EXIT_BUYBACK"):
                amount = -abs(prem)   # buyback is a cash outflow
            else:
                amount = prem         # open premium or $0 expiry

            total += amount

            # net: on close/expiry rows, compute realised profit for this cycle
            net = ""
            if et in ("CSP_CLOSE_TP", "CC_CLOSE_TP", "CC_MANUAL_EXIT_BUYBACK"):
                orig = open_premiums.get(ref, 0.0)
                net  = f"{orig + amount:.2f}"   # orig is positive; amount is negative buyback
            elif et in ("CSP_EXPIRED", "CC_EXPIRED"):
                # Full premium kept — net equals original open premium
                orig = open_premiums.get(ref, 0.0)
                net  = f"{orig:.2f}" if orig else ""

            # human-readable notes
            if et == "CSP_OPEN":
                exp   = (e.get("expiry") or "").strip()
                strk  = (e.get("strike") or "").strip()
                contr = (e.get("contracts") or "").strip()
                notes = f"{strk}P exp {exp}" + (f" x{contr}" if contr and contr != "1" else "")
            elif et == "CC_OPEN":
                exp   = (e.get("expiry") or "").strip()
                strk  = (e.get("strike") or "").strip()
                contr = (e.get("contracts") or "").strip()
                notes = f"{strk}C exp {exp}" + (f" x{contr}" if contr and contr != "1" else "")
            elif et == "CSP_CLOSE_TP":
                notes = f"TP buyback — net ${float(net):.0f}" if net else "TP buyback"
            elif et == "CC_CLOSE_TP":
                notes = f"CC TP buyback — net ${float(net):.0f}" if net else "CC TP buyback"
            elif et == "CC_MANUAL_EXIT_BUYBACK":
                notes = f"CC buyback (manual exit) — net ${float(net):.0f}" if net else "CC buyback (manual exit)"
            elif et in ("CSP_EXPIRED", "CC_EXPIRED"):
                notes = "expired worthless"
            else:
                notes = ""

            out_rows.append({
                "date":       (e.get("date") or "").strip(),
                "account":    acct,
                "ticker":     tkr,
                "event_type": et,
                "amount":     f"{amount:+.2f}",
                "net":        net,
                "notes":      notes,
            })

        # Summary row
        out_rows.append({
            "date": "", "account": "", "ticker": "NET_PREMIUM",
            "event_type": "", "amount": f"{total:+.2f}", "net": "", "notes": "",
        })

        path = os.path.join(WHEEL_MONTHLY_DIR, f"{month}-{group}.csv")
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
            account=(r.get("account") or INDIVIDUAL).strip().upper(),
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
            account=(r.get("account") or INDIVIDUAL).strip().upper(),
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