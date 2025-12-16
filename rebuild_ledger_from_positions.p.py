import csv
import datetime as dt
import os

CSP_POSITIONS_FILE = "csp_positions.csv"
CSP_LEDGER_FILE = "csp_ledger.csv"

LEDGER_FIELDS = [
    "date","week_id","ticker","expiry","strike","contracts",
    "credit_mid","cash_reserved","est_premium","tier"
]

def iso_week_id(d: dt.date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"

def load_rows(path: str):
    if not os.path.isfile(path):
        return []
    with open(path, "r", newline="") as f:
        return list(csv.DictReader(f))

def write_rows(path: str, rows: list, fieldnames: list):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def safe_float(x, default=0.0):
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return default

def safe_int(x, default=0):
    try:
        return int(float(str(x).replace(",", "").strip()))
    except Exception:
        return default

def normalize_date(s: str):
    s = (s or "").strip()
    # Expect YYYY-MM-DD. If your backfill produced something else, fix it there.
    dt.date.fromisoformat(s)  # raises if invalid
    return s

def main():
    today = dt.date.today()
    positions = load_rows(CSP_POSITIONS_FILE)
    if not positions:
        print("No rows in csp_positions.csv")
        return

    ledger_rows = []
    problems = []

    for r in positions:
        status = (r.get("status") or "").strip().upper()
        if status != "OPEN":
            continue

        try:
            expiry = normalize_date(r.get("expiry"))
            # if already expired, don’t count collateral
            if dt.date.fromisoformat(expiry) < today:
                continue
        except Exception:
            problems.append(f"Bad expiry for {r.get('ticker')}: {r.get('expiry')}")
            continue

        try:
            open_date = normalize_date(r.get("open_date"))
        except Exception:
            problems.append(f"Bad open_date for {r.get('ticker')}: {r.get('open_date')}")
            continue

        contracts = safe_int(r.get("contracts"))
        strike = safe_float(r.get("strike"))
        cash_reserved = safe_float(r.get("cash_reserved"))

        if contracts <= 0 or strike <= 0 or cash_reserved <= 0:
            problems.append(
                f"Bad sizing for {r.get('ticker')} {expiry}: "
                f"contracts={r.get('contracts')} strike={r.get('strike')} cash_reserved={r.get('cash_reserved')}"
            )
            continue

        week_id = iso_week_id(dt.date.fromisoformat(open_date))

        # tier is optional; keep blank if unknown
        tier = (r.get("tier") or "").strip().upper()

        ledger_rows.append({
            "date": open_date,
            "week_id": week_id,
            "ticker": (r.get("ticker") or "").strip().upper(),
            "expiry": expiry,
            "strike": f"{strike:.2f}",
            "contracts": str(contracts),
            "credit_mid": (r.get("credit_mid") or "").strip(),
            "cash_reserved": f"{cash_reserved:.2f}",
            "est_premium": (r.get("est_premium") or "").strip(),
            "tier": tier,
        })

    # Sort by date then ticker
    ledger_rows.sort(key=lambda x: (x["date"], x["ticker"], x["expiry"], float(x["strike"])))

    write_rows(CSP_LEDGER_FILE, ledger_rows, LEDGER_FIELDS)

    print(f"Rebuilt {CSP_LEDGER_FILE} with {len(ledger_rows)} OPEN CSP rows.")
    if problems:
        print("\nProblems found (fix these rows in csp_positions.csv):")
        for p in problems:
            print(" -", p)

if __name__ == "__main__":
    main()
