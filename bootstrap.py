import csv
import datetime as dt
import os
import re
from typing import List, Dict, Any

# ---- INPUTS ----
INPUT_FILES = [
    "November-November Trading.csv",
    "December-December Trading.csv",
]

# ---- OUTPUTS (match your algo) ----
CSP_POSITIONS_FILE = "csp_positions.csv"
CC_POSITIONS_FILE = "cc_positions.csv"

CSP_POSITIONS_COLUMNS = [
    "id","open_date","week_id","ticker","expiry","dte_open",
    "strike","contracts","credit_mid",
    "cash_reserved","est_premium",
    "status",
    "underlying_last","strike_diff","strike_diff_pct","dte_remaining","itm_otm",
    "close_date","close_type",
    "underlying_close_at_expiry",
    "shares_if_assigned","assignment_cost_basis",
    "notes"
]

CC_POSITIONS_COLUMNS = [
    "id","open_date","ticker","expiry","strike","contracts","credit_mid",
    "status","close_date","close_type","notes"
]


# ---------- helpers ----------

def parse_mmddyyyy(s: str) -> dt.date:
    # handles 12/05/2025
    m, d, y = s.strip().split("/")
    return dt.date(int(y), int(m), int(d))

def iso_week_id(d: dt.date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"

def parse_strike(val: str) -> float:
    """
    Handles:
      '40' -> 40.0
      '40(46)' -> 40.0
      '47(60)' -> 47.0
    """
    if val is None:
        return 0.0
    s = str(val).strip()
    if not s:
        return 0.0
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)", s)
    return float(m.group(1)) if m else 0.0

def parse_int(val: str) -> int:
    try:
        return int(float(str(val).strip()))
    except Exception:
        return 0

def norm_ticker(s: str) -> str:
    return (s or "").strip().upper()

def write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            # ensure all columns exist
            out = {k: r.get(k, "") for k in fieldnames}
            w.writerow(out)


# ---------- main bootstrap ----------

def load_trades_from_files(files: List[str]) -> List[Dict[str, str]]:
    all_rows = []
    for fp in files:
        if not os.path.isfile(fp):
            raise FileNotFoundError(f"Missing input file: {fp}")
        with open(fp, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_rows.append(row)
    return all_rows

def build_open_positions(rows: List[Dict[str, str]]):
    csp_rows = []
    cc_rows = []

    for r in rows:
        status = (r.get("Status") or "").strip().upper()
        action = (r.get("Action") or "").strip().upper()

        if status != "OPEN":
            continue

        open_date_raw = (r.get("Date") or "").strip()
        ticker = norm_ticker(r.get("Stock"))
        exp_raw = (r.get("Expiration Date") or "").strip()
        strike_raw = (r.get("Strike") or "").strip()
        contracts_raw = (r.get("#Contracts") or "").strip()

        if not ticker:
            continue

        # parse dates
        try:
            open_date = parse_mmddyyyy(open_date_raw)
        except Exception:
            continue

        expiry = ""
        dte_open = ""
        if exp_raw and exp_raw.upper() != "N/A" and exp_raw != "---":
            try:
                exp_date = parse_mmddyyyy(exp_raw)
                expiry = exp_date.isoformat()
                dte_open = str((exp_date - open_date).days)
            except Exception:
                expiry = ""
                dte_open = ""

        strike = parse_strike(strike_raw)
        contracts = parse_int(contracts_raw)

        # CSP
        if action == "SELL PUT":
            pos_id = f"{ticker}-{expiry}-{strike:.2f}-{open_date.isoformat()}"
            csp_rows.append({
                "id": pos_id,
                "open_date": open_date.isoformat(),
                "week_id": iso_week_id(open_date),
                "ticker": ticker,
                "expiry": expiry,
                "dte_open": dte_open,
                "strike": f"{strike:.2f}",
                "contracts": str(contracts),
                "credit_mid": "",
                "cash_reserved": f"{strike * 100.0 * contracts:.0f}" if strike and contracts else "",
                "est_premium": "",
                "status": "OPEN",
                "underlying_last": "",
                "strike_diff": "",
                "strike_diff_pct": "",
                "dte_remaining": "",
                "itm_otm": "",
                "close_date": "",
                "close_type": "",
                "underlying_close_at_expiry": "",
                "shares_if_assigned": "",
                "assignment_cost_basis": "",
                "notes": "Bootstrap import",
            })

        # CC
        elif action == "SELL CALL":
            pos_id = f"{ticker}-{expiry}-{strike:.2f}-{open_date.isoformat()}"
            cc_rows.append({
                "id": pos_id,
                "open_date": open_date.isoformat(),
                "ticker": ticker,
                "expiry": expiry,
                "strike": f"{strike:.2f}",
                "contracts": str(contracts),
                "credit_mid": "",
                "status": "OPEN",
                "close_date": "",
                "close_type": "",
                "notes": "Bootstrap import",
            })

    # Optional: sort for readability
    csp_rows.sort(key=lambda x: (x.get("open_date",""), x.get("ticker","")))
    cc_rows.sort(key=lambda x: (x.get("open_date",""), x.get("ticker","")))

    return csp_rows, cc_rows

def main():
    rows = load_trades_from_files(INPUT_FILES)
    csp_rows, cc_rows = build_open_positions(rows)

    write_csv(CSP_POSITIONS_FILE, csp_rows, CSP_POSITIONS_COLUMNS)
    write_csv(CC_POSITIONS_FILE, cc_rows, CC_POSITIONS_COLUMNS)

    print(f"Wrote {len(csp_rows)} OPEN CSP rows -> {CSP_POSITIONS_FILE}")
    print(f"Wrote {len(cc_rows)} OPEN CC rows  -> {CC_POSITIONS_FILE}")

if __name__ == "__main__":
    main()
