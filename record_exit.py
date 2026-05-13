#!/usr/bin/env python3
"""
record_exit.py

Interactively record a stop-out (or manual close) for an open stock position.

Usage:
    python record_exit.py

The script will:
  1. Show all currently OPEN stock positions
  2. Ask which ticker was stopped out
  3. Ask for exit price (defaults to the position's stop_price)
  4. Ask for exit date (defaults to today)
  5. Update stock_positions.csv  → marks row CLOSED
  6. Append to stock_trades.csv  → records the closed trade
  7. Append to stock_fills.csv   → records the fill
  8. Rebuilds stock_monthly CSV  → P/L report stays accurate

Run from the Trading-Screener project root:
    python record_stopout.py
"""

import csv
import datetime as dt
import os
import sys


# ── File paths (mirrors config.py) ───────────────────────────────────────────
STOCK_POSITIONS_FILE = "stock_positions.csv"
STOCK_TRADES_FILE    = "stock_trades.csv"
STOCK_FILLS_FILE     = "stock_fills.csv"
STOCK_MONTHLY_DIR    = "stock_monthly"

IRA_ACCOUNTS = ("IRA", "ROTH")

STOCK_POS_FIELDS = [
    "id", "account", "ticker", "signal", "plan_date", "entry_date",
    "entry_price", "shares", "adds", "last_add_date", "initial_entry_price",
    "initial_shares", "stop_price", "target_price", "risk_per_share",
    "r_multiple_target", "stop_type", "status", "exit_date", "exit_price",
    "exit_reason", "pnl_abs", "pnl_pct", "notes",
]
STOCK_TRADE_FIELDS = [
    "id", "account", "ticker", "entry_date", "entry_price", "shares",
    "exit_date", "exit_price", "reason", "close_type", "pnl_abs", "pnl_pct",
]
STOCK_FILL_FIELDS = [
    "date", "account", "ticker", "action", "price", "shares", "reason",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _read(path, fields=None):
    if not os.path.isfile(path):
        return [], fields or []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)
        hdrs   = list(reader.fieldnames or [])
    return rows, hdrs


def _write(path, rows, fields):
    with open(path + ".tmp", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            clean = {k: v for k, v in r.items() if k is not None}
            w.writerow(clean)
    os.replace(path + ".tmp", path)


def _append(path, row, fields):
    file_exists = os.path.isfile(path)
    # Ensure trailing newline to avoid row-gluing
    if file_exists:
        with open(path, "rb+") as f:
            f.seek(0, 2)
            if f.tell() > 0:
                f.seek(-1, 2)
                if f.read(1) != b"\n":
                    f.write(b"\n")
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore",
                           lineterminator="\n")
        if not file_exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in fields})


def _safe_float(v, default=0.0):
    try:
        return float(str(v).strip())
    except Exception:
        return default


def _safe_int(v, default=0):
    try:
        return int(float(str(v).strip()))
    except Exception:
        return default


def _ask(prompt, default=None):
    suffix = f" [{default}]" if default is not None else ""
    val    = input(f"  {prompt}{suffix}: ").strip()
    return val if val else (str(default) if default is not None else "")


def _confirm(prompt):
    return input(f"  {prompt} [y/N]: ").strip().lower() == "y"


def rebuild_monthly(trades_path, monthly_dir):
    if not os.path.isfile(trades_path):
        return
    rows, _ = _read(trades_path)
    if not rows:
        return

    os.makedirs(monthly_dir, exist_ok=True)
    out_fields = ["date", "account", "ticker", "shares",
                  "entry_price", "exit_price", "close_type", "pnl_abs", "pnl_pct"]

    by_bucket = {}
    for r in rows:
        d = (r.get("exit_date") or "").strip()
        if len(d) < 7:
            continue
        month = d[:7]
        acct  = (r.get("account") or "INDIVIDUAL").strip().upper()
        group = "IRA" if acct in IRA_ACCOUNTS else "INDIVIDUAL"
        by_bucket.setdefault((month, group), []).append(r)

    for (month, group), mrows in sorted(by_bucket.items()):
        total    = 0.0
        out_rows = []
        for r in sorted(mrows, key=lambda x: x.get("exit_date") or ""):
            pnl = _safe_float(r.get("pnl_abs"), 0.0)
            total += pnl
            out_rows.append({
                "date":        r.get("exit_date", ""),
                "account":     r.get("account", ""),
                "ticker":      r.get("ticker", ""),
                "shares":      r.get("shares", ""),
                "entry_price": r.get("entry_price", ""),
                "exit_price":  r.get("exit_price", ""),
                "close_type":  r.get("close_type", ""),
                "pnl_abs":     f"{pnl:.2f}",
                "pnl_pct":     r.get("pnl_pct", ""),
            })
        out_rows.append({
            "date": "", "account": "", "ticker": "TOTAL", "shares": "",
            "entry_price": "", "exit_price": "", "close_type": "",
            "pnl_abs": f"{total:.2f}", "pnl_pct": "",
        })
        path = os.path.join(monthly_dir, f"{month}-{group}.csv")
        _write(path, out_rows, out_fields)
        print(f"  Rebuilt {path}  (total P/L ${total:+.2f})")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not os.path.isfile("screener.py"):
        print("ERROR: run this from the Trading-Screener project root.")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("  Record Stock Stop-Out / Manual Close")
    print("=" * 60)

    # Load open positions
    pos_rows, pos_fields = _read(STOCK_POSITIONS_FILE)
    # Ensure stop_type field exists for old rows
    if "stop_type" not in (pos_fields or []):
        pos_fields = STOCK_POS_FIELDS

    open_rows = [r for r in pos_rows if (r.get("status") or "").upper() == "OPEN"
                 and (r.get("account") or "").upper() == "INDIVIDUAL"]

    if not open_rows:
        print("\n  No open INDIVIDUAL stock positions found.")
        sys.exit(0)

    # Display open positions
    print(f"\n  Open INDIVIDUAL positions ({len(open_rows)}):\n")
    print(f"  {'#':<3}  {'Ticker':<6}  {'Signal':<14}  {'Entry':>7}  "
          f"{'Stop':>7}  {'Target':>7}  {'Shares':>6}  {'Stop Type'}")
    print(f"  {'─'*3}  {'─'*6}  {'─'*14}  {'─'*7}  "
          f"{'─'*7}  {'─'*7}  {'─'*6}  {'─'*10}")

    for i, r in enumerate(open_rows, 1):
        stop_type  = (r.get("stop_type") or "FIXED").upper()
        stop_label = "🔄 TRAIL" if stop_type == "TRAIL_EMA8" else "  FIXED"
        print(f"  {i:<3}  {r.get('ticker','?'):<6}  "
              f"{r.get('signal',''):<14}  "
              f"{_safe_float(r.get('entry_price')):>7.2f}  "
              f"{_safe_float(r.get('stop_price')):>7.2f}  "
              f"{_safe_float(r.get('target_price')):>7.2f}  "
              f"{_safe_int(r.get('shares')):>6}  "
              f"{stop_label}")

    print()

    # Pick position
    while True:
        raw = _ask("Enter ticker symbol (or # from list above)").upper()
        if not raw:
            print("  Cancelled.")
            sys.exit(0)

        # Allow selecting by number
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(open_rows):
                pos = open_rows[idx]
                break
            else:
                print(f"  Invalid number. Enter 1–{len(open_rows)}.")
                continue

        matches = [r for r in open_rows if (r.get("ticker") or "").upper() == raw]
        if len(matches) == 1:
            pos = matches[0]
            break
        elif len(matches) > 1:
            print(f"  Multiple positions for {raw} — enter the # instead.")
        else:
            print(f"  '{raw}' not found in open positions.")

    tkr        = (pos.get("ticker") or "").strip().upper()
    entry_px   = _safe_float(pos.get("entry_price"))
    stop_px    = _safe_float(pos.get("stop_price"))
    target_px  = _safe_float(pos.get("target_price"))
    shares     = _safe_int(pos.get("shares"))
    entry_date = (pos.get("entry_date") or "").strip()
    stop_type  = (pos.get("stop_type") or "FIXED").upper()

    print(f"\n  Selected: {tkr}  |  entry ${entry_px:.2f}  |  "
          f"stop ${stop_px:.2f}  |  target ${target_px:.2f}  |  "
          f"{shares} shares  |  {stop_type}")

    # Close reason
    print("\n  Close type:")
    print("    1  STOP    — stopped out at stop price")
    print("    2  TARGET  — target price hit")
    print("    3  MANUAL  — manually closed at a specific price")
    raw_reason = _ask("Choice", default="1")
    if raw_reason == "2":
        close_type = "TARGET"
    elif raw_reason == "3":
        close_type = "MANUAL"
    else:
        close_type = "STOP"

    # Exit price
    if close_type == "STOP":
        default_px = stop_px
    elif close_type == "TARGET":
        default_px = _safe_float(pos.get("target_price"), entry_px)
    else:
        default_px = entry_px
    raw_px     = _ask(f"Exit price", default=f"{default_px:.2f}")
    exit_px    = _safe_float(raw_px, default_px)

    # Shares — defaults to full position, allows partial (e.g. runners)
    raw_shares = _ask(f"Shares to close (press Enter for all)", default=str(shares))
    exit_shares = _safe_int(raw_shares, shares)
    if exit_shares <= 0 or exit_shares > shares:
        exit_shares = shares

    # Exit date
    today    = dt.date.today().isoformat()
    raw_date = _ask("Exit date (YYYY-MM-DD)", default=today)
    try:
        exit_date = dt.date.fromisoformat(raw_date).isoformat()
    except Exception:
        print(f"  Invalid date '{raw_date}' — using today ({today}).")
        exit_date = today

    # Calculate P/L
    pnl_abs = (exit_px - entry_px) * exit_shares
    pnl_pct = (exit_px - entry_px) / entry_px * 100 if entry_px > 0 else 0.0
    partial  = exit_shares < shares
    remaining = shares - exit_shares

    print(f"\n  ── Summary ──────────────────────────────────────")
    print(f"  Ticker:     {tkr}")
    print(f"  Account:    {pos.get('account','?')}")
    print(f"  Signal:     {pos.get('signal','?')}  [{stop_type}]")
    print(f"  Entry:      ${entry_px:.2f}  on {entry_date}")
    print(f"  Exit:       ${exit_px:.2f}  on {exit_date}  [{close_type}]")
    print(f"  Shares:     {exit_shares}" + (f"  ({remaining} remaining as runners)" if partial else "  (full position)"))
    print(f"  P/L:        ${pnl_abs:+.2f}  ({pnl_pct:+.2f}%)")
    print(f"  ────────────────────────────────────────────────")

    if not _confirm("\n  Confirm and write to CSVs?"):
        print("  Cancelled — no files changed.")
        sys.exit(0)

    pos_id = (pos.get("id") or "").strip()

    # ── Update stock_positions.csv ────────────────────────────────────────────
    for r in pos_rows:
        if (r.get("id") or "").strip() == pos_id:
            if partial:
                # Partial close — reduce shares, keep OPEN
                r["shares"]  = str(remaining)
                r["adds"]    = str(_safe_int(r.get("adds"), 0) + 1)
                # Update stop/target to reflect partial exit in notes
                r["notes"]   = (r.get("notes") or "") + (
                    f" | partial exit {exit_shares}sh @ ${exit_px:.2f} on {exit_date}"
                )
            else:
                r["status"]      = "CLOSED"
                r["exit_date"]   = exit_date
                r["exit_price"]  = f"{exit_px:.2f}"
                r["exit_reason"] = close_type
                r["pnl_abs"]     = f"{pnl_abs:.2f}"
                r["pnl_pct"]     = f"{pnl_pct:.2f}"
            break
    _write(STOCK_POSITIONS_FILE, pos_rows, STOCK_POS_FIELDS)
    print(f"\n  ✅ stock_positions.csv updated" + (" (partial — position still OPEN)" if partial else ""))

    # ── Append to stock_trades.csv ────────────────────────────────────────────
    trade_row = {
        "id":          pos_id + (f"-partial-{exit_date}" if partial else ""),
        "account":     pos.get("account", "INDIVIDUAL"),
        "ticker":      tkr,
        "entry_date":  entry_date,
        "entry_price": f"{entry_px:.2f}",
        "shares":      str(exit_shares),
        "exit_date":   exit_date,
        "exit_price":  f"{exit_px:.2f}",
        "reason":      close_type + ("-PARTIAL" if partial else ""),
        "close_type":  close_type + ("-PARTIAL" if partial else ""),
        "pnl_abs":     f"{pnl_abs:.2f}",
        "pnl_pct":     f"{pnl_pct:.2f}",
    }
    _append(STOCK_TRADES_FILE, trade_row, STOCK_TRADE_FIELDS)
    print(f"  ✅ stock_trades.csv updated")

    # ── Append to stock_fills.csv ─────────────────────────────────────────────
    fill_row = {
        "date":    exit_date,
        "account": pos.get("account", "INDIVIDUAL"),
        "ticker":  tkr,
        "action":  "CLOSE" + ("-PARTIAL" if partial else ""),
        "price":   f"{exit_px:.2f}",
        "shares":  str(exit_shares),
        "reason":  close_type,
    }
    _append(STOCK_FILLS_FILE, fill_row, STOCK_FILL_FIELDS)
    print(f"  ✅ stock_fills.csv updated")

    # ── Rebuild monthly P/L reports ───────────────────────────────────────────
    print(f"  Rebuilding monthly reports...")
    rebuild_monthly(STOCK_TRADES_FILE, STOCK_MONTHLY_DIR)

    print(f"\n  Done. {tkr} recorded as {close_type} @ ${exit_px:.2f} "
          f"P/L ${pnl_abs:+.2f} ({pnl_pct:+.2f}%)")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
