# strategies.py
from __future__ import annotations

import csv
import datetime as dt
import os
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf
import ta

from config import (
    DATA_PERIOD, DATA_INTERVAL,
    POSITIONS_FILE, TRADES_LOG_FILE,
    CSP_LEDGER_FILE,
    CSP_POSITIONS_FILE, CC_POSITIONS_FILE,
    CSP_POSITIONS_COLUMNS, CC_POSITIONS_COLUMNS,
    CSP_TARGET_DTE_MIN, CSP_TARGET_DTE_MAX,
    CSP_MAX_CASH_PER_TRADE,
    CSP_MIN_OI, CSP_MIN_VOLUME, CSP_MIN_BID, CSP_MIN_IV,
    CSP_STRIKE_MODE,
    CSP_MIN_PREMIUM_CONSERVATIVE, CSP_MIN_PREMIUM_BALANCED, CSP_MIN_PREMIUM_AGGRESSIVE,
    CSP_MIN_YIELD_CONSERVATIVE, CSP_MIN_YIELD_BALANCED, CSP_MIN_YIELD_AGGRESSIVE,
    CSP_MAX_AGGRESSIVE_TOTAL, CSP_MAX_AGGRESSIVE_PER_WEEK,
)

# ----------------------------
# Data / indicators
# ----------------------------

def download_ohlcv(ticker: str, period: str = DATA_PERIOD, interval: str = DATA_INTERVAL) -> pd.DataFrame:
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    df.dropna(inplace=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]

    df = df.copy()

    df["SMA_50"] = ta.trend.sma_indicator(close, window=50)
    df["SMA_200"] = ta.trend.sma_indicator(close, window=200)
    df["EMA_21"] = ta.trend.ema_indicator(close, window=21)
    df["EMA_10"] = ta.trend.ema_indicator(close, window=10)

    df["ATR_14"] = ta.volatility.average_true_range(high=high, low=low, close=close, window=14)
    df["RSI_2"] = ta.momentum.rsi(close, window=2)
    df["ADX_14"] = ta.trend.adx(high=high, low=low, close=close, window=14)

    df["VOL_SMA_10"] = volume.rolling(window=10).mean()
    df["HIGH_20"] = close.rolling(window=20).max()

    return df

def compute_relative_strength(stock_df: pd.DataFrame, spy_df: pd.DataFrame, lookback: int = 20) -> float:
    if len(stock_df) < lookback or len(spy_df) < lookback:
        return 0.0
    stock_recent = stock_df["Close"].iloc[-lookback]
    stock_last = stock_df["Close"].iloc[-1]
    spy_recent = spy_df["Close"].iloc[-lookback]
    spy_last = spy_df["Close"].iloc[-1]
    stock_return = (stock_last - stock_recent) / stock_recent
    spy_return = (spy_last - spy_recent) / spy_recent
    return float(stock_return - spy_return)

# ----------------------------
# Market regime (SPY/QQQ/VIX)
# ----------------------------

def market_context_from_dfs(spy_df: pd.DataFrame, qqq_df: pd.DataFrame, vix_df: pd.DataFrame) -> Dict[str, float | bool]:
    """Compute market regime flags from pre-fetched SPY/QQQ/VIX OHLCV."""
    spy_last = add_indicators(spy_df).iloc[-1]
    qqq_last = add_indicators(qqq_df).iloc[-1]
    vix_close = float(vix_df["Close"].iloc[-1])

    spy_close = float(spy_last["Close"])
    qqq_close = float(qqq_last["Close"])

    return {
        "spy_close": spy_close,
        "qqq_close": qqq_close,
        "vix_close": vix_close,
        "spy_above_200": bool(spy_close > float(spy_last["SMA_200"])),
        "spy_above_50": bool(spy_close > float(spy_last["SMA_50"])),
        "spy_above_21": bool(spy_close > float(spy_last["EMA_21"])),
        "qqq_above_50": bool(qqq_close > float(qqq_last["SMA_50"])),
        "vix_below_18": bool(vix_close < 18),
        "vix_below_25": bool(vix_close < 25),
    }


def market_context(today: dt.date) -> Dict[str, float | bool]:
    """Screener-friendly wrapper: downloads SPY/QQQ/VIX and returns normalized keys."""
    spy_df = download_ohlcv("SPY")
    qqq_df = download_ohlcv("QQQ")
    vix_df = download_ohlcv("^VIX")
    return market_context_from_dfs(spy_df, qqq_df, vix_df)

# ----------------------------
# Stock entry logic
# ----------------------------

def is_eligible_detailed(stock_row: pd.Series, rs_20: float) -> Tuple[bool, Dict[str, bool]]:
    cond_close_above_sma50 = bool(stock_row["Close"] > stock_row["SMA_50"])
    cond_ema21_above_sma50 = bool(stock_row["EMA_21"] > stock_row["SMA_50"])
    cond_rs_positive = bool(rs_20 > 0)
    cond_adx_ok = bool(stock_row["ADX_14"] > 20)

    eligible = bool(cond_close_above_sma50 and cond_ema21_above_sma50 and cond_rs_positive and cond_adx_ok)
    details = {
        "close_above_sma50": cond_close_above_sma50,
        "ema21_above_sma50": cond_ema21_above_sma50,
        "rs20_positive": cond_rs_positive,
        "adx14_gt_20": cond_adx_ok,
    }
    return eligible, details


def is_eligible(stock_row: pd.Series, rs_20: Optional[float] = None) -> bool:
    """Screener-friendly eligibility boolean.

    If rs_20 is provided, uses the old 'relative strength' filter too.
    """
    try:
        close = float(stock_row["Close"])
        sma50 = float(stock_row["SMA_50"])
        ema21 = float(stock_row["EMA_21"])
        adx = float(stock_row["ADX_14"])
    except Exception:
        return False

    base = bool(close > sma50 and ema21 > sma50 and adx > 20)
    if rs_20 is None:
        return base
    return bool(base and float(rs_20) > 0)

def pullback_signal(stock_row: pd.Series) -> bool:
    rsi_ok = bool(stock_row["RSI_2"] < 5)
    ema_21 = float(stock_row["EMA_21"])
    close = float(stock_row["Close"])
    near_ema = abs(close - ema_21) / ema_21 < 0.005
    return bool(rsi_ok and near_ema)

def breakout_signal(stock_row: pd.Series) -> bool:
    cond_price = bool(stock_row["Close"] > stock_row["HIGH_20"])
    cond_vol = bool(stock_row["Volume"] > 1.5 * stock_row["VOL_SMA_10"])
    return bool(cond_price and cond_vol)

# ----------------------------
# Open positions (stock trades)
# ----------------------------

def load_open_positions() -> Dict[str, dict]:
    positions: Dict[str, dict] = {}
    if not os.path.isfile(POSITIONS_FILE):
        return positions
    with open(POSITIONS_FILE, mode="r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            positions[row["ticker"]] = row
    return positions

def save_open_positions(positions: Dict[str, dict]) -> None:
    fieldnames = ["ticker", "entry_date", "entry_price", "entry_type", "initial_stop"]
    with open(POSITIONS_FILE, mode="w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for _, pos in positions.items():
            w.writerow(pos)

def log_closed_trade(trade: dict) -> None:
    fieldnames = ["ticker","entry_date","entry_price","exit_date","exit_price","reason","pnl_abs","pnl_pct"]
    file_exists = os.path.isfile(TRADES_LOG_FILE)
    with open(TRADES_LOG_FILE, mode="a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        w.writerow(trade)

# ----------------------------
# CSP planning / bookkeeping (paper)
# ----------------------------

def _iso_week_id(d: dt.date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"

def ensure_positions_files() -> None:
    # CSP positions file
    if not os.path.isfile(CSP_POSITIONS_FILE):
        with open(CSP_POSITIONS_FILE, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSP_POSITIONS_COLUMNS)
            w.writeheader()
    # CC positions file
    if not os.path.isfile(CC_POSITIONS_FILE):
        with open(CC_POSITIONS_FILE, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CC_POSITIONS_COLUMNS)
            w.writeheader()

def load_csv_rows(path: str) -> List[dict]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", newline="") as f:
        return list(csv.DictReader(f))

def write_csv_rows(path: str, rows: List[dict], fieldnames: List[str]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def append_csp_ledger_row(row: dict) -> None:
    fieldnames = ["date","week_id","ticker","expiry","strike","contracts","credit_mid","cash_reserved","est_premium","tier"]
    file_exists = os.path.isfile(CSP_LEDGER_FILE)
    with open(CSP_LEDGER_FILE, mode="a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        w.writerow(row)

def csp_already_logged(ledger_rows: List[dict], week_id: str, ticker: str, expiry: str, strike: float) -> bool:
    for r in ledger_rows:
        try:
            if r["week_id"] == week_id and r["ticker"] == ticker and r["expiry"] == expiry and float(r["strike"]) == float(strike):
                return True
        except Exception:
            continue
    return False

def is_csp_eligible(stock_row: pd.Series) -> bool:
    return bool(stock_row["Close"] > stock_row["SMA_200"] and stock_row["Volume"] > 1_000_000)

def _pick_expiry_in_dte_range(ticker_obj: yf.Ticker, dte_min: int, dte_max: int) -> Tuple[Optional[str], Optional[int]]:
    today = dt.date.today()
    expiries: List[Tuple[str, int]] = []
    for exp_str in ticker_obj.options:
        try:
            exp_date = dt.date.fromisoformat(exp_str)
            dte = (exp_date - today).days
            expiries.append((exp_str, dte))
        except Exception:
            continue
    expiries.sort(key=lambda x: x[1])
    for exp_str, dte in expiries:
        if dte_min <= dte <= dte_max:
            return exp_str, dte
    return None, None

def _suggest_put_strike(stock_last: pd.Series, atr_mult: float) -> float:
    close = float(stock_last["Close"])
    ema21 = float(stock_last["EMA_21"])
    atr14 = float(stock_last["ATR_14"])
    if CSP_STRIKE_MODE == "ema21_atr":
        return ema21 - (atr_mult * atr14)
    return close * 0.92

def _round_strike_to_chain(puts_df: pd.DataFrame, target_strike: float) -> float:
    strikes = sorted([float(s) for s in puts_df["strike"].tolist()])
    below = [s for s in strikes if s <= target_strike]
    if not below:
        return strikes[0]
    return below[-1]

def evaluate_csp_candidate(
    ticker: str,
    df_or_last: pd.DataFrame | pd.Series,
    atr_mult: float = 0.50,
) -> Optional[dict]:
    """Evaluate a single CSP candidate.

    screener.py passes the full indicator dataframe, while older code passed the
    last row + an explicit ATR multiplier. This wrapper supports both.
    """

    # Accept either full df (preferred) or a single last-row Series.
    if isinstance(df_or_last, pd.DataFrame):
        df = df_or_last
        stock_last = df.iloc[-1]
        # If the caller passed raw OHLCV, ensure indicators exist.
        if "EMA_21" not in df.columns or "ATR_14" not in df.columns:
            try:
                stock_last = add_indicators(df).iloc[-1]
            except Exception:
                return None
    else:
        stock_last = df_or_last
    try:
        t = yf.Ticker(ticker)
        exp_str, dte = _pick_expiry_in_dte_range(t, CSP_TARGET_DTE_MIN, CSP_TARGET_DTE_MAX)
        if not exp_str:
            return None

        chain = t.option_chain(exp_str)
        puts = chain.puts.copy()
        if puts.empty:
            return None

        raw_strike = _suggest_put_strike(stock_last, atr_mult=atr_mult)
        strike = _round_strike_to_chain(puts, raw_strike)

        row = puts.loc[puts["strike"] == strike]
        if row.empty:
            return None
        row = row.iloc[0]

        bid = float(row.get("bid", 0) or 0)
        ask = float(row.get("ask", 0) or 0)
        oi = int(row.get("openInterest", 0) or 0)
        vol = int(row.get("volume", 0) or 0)
        iv = float(row.get("impliedVolatility", 0) or 0)

        if bid < CSP_MIN_BID or ask <= 0 or ask < bid:
            return None
        if oi < CSP_MIN_OI or vol < CSP_MIN_VOLUME:
            return None
        if CSP_MIN_IV and iv < CSP_MIN_IV:
            return None

        mid = (bid + ask) / 2.0

        cash_required_per_contract = strike * 100.0
        contracts = int(CSP_MAX_CASH_PER_TRADE // cash_required_per_contract)
        if contracts < 1:
            return None

        est_premium = mid * 100.0 * contracts
        cash_reserved = cash_required_per_contract * contracts
        yield_pct = est_premium / cash_reserved

        return {
            "ticker": ticker,
            "expiry": exp_str,
            "dte": int(dte or 0),
            "strike": float(strike),
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "iv": iv,
            "contracts": int(contracts),
            "cash_reserved": float(cash_reserved),
            "est_premium": float(est_premium),
            "yield_pct": float(yield_pct),
            "atr_mult": float(atr_mult),
            "reason": f"Strike≈EMA21-{atr_mult:.2f}*ATR (raw {raw_strike:.2f})",
        }
    except Exception:
        return None

def csp_regime(vix_close: float) -> str:
    if vix_close < 18:
        return "LOW_IV"
    if vix_close <= 25:
        return "NORMAL"
    return "HIGH_IV"

def classify_csp_tier(idea: dict) -> str:
    prem = float(idea["est_premium"])
    y = float(idea["yield_pct"])
    if prem >= CSP_MIN_PREMIUM_AGGRESSIVE and y >= CSP_MIN_YIELD_AGGRESSIVE:
        return "AGGRESSIVE"
    if prem >= CSP_MIN_PREMIUM_BALANCED and y >= CSP_MIN_YIELD_BALANCED:
        return "BALANCED"
    if prem >= CSP_MIN_PREMIUM_CONSERVATIVE and y >= CSP_MIN_YIELD_CONSERVATIVE:
        return "CONSERVATIVE"
    return "REJECT"

def score_csp_idea(idea: dict) -> float:
    prem = float(idea["est_premium"])
    y = float(idea["yield_pct"])
    iv = float(idea["iv"])
    dte = float(idea["dte"])
    s = 0.0
    s += min(prem / 250.0, 2.0)
    s += min(y / 0.04, 2.0)
    s += min(iv / 0.45, 1.5)
    s += 0.5 if 30 <= dte <= 40 else 0.0
    return float(s)

def allowed_tiers_for_regime(reg: str) -> set:
    if reg == "LOW_IV":
        return {"CONSERVATIVE", "BALANCED"}
    return {"CONSERVATIVE", "BALANCED", "AGGRESSIVE"}

def plan_weekly_csp_orders(
    csp_candidates: List[dict],
    *,
    today: dt.date,
    vix_close: float,
    total_remaining_cap: float,
    week_remaining_cap: float,
    aggressive_total: int,
    aggressive_week: int,
) -> Dict[str, object]:
    week_id = _iso_week_id(today)
    reg = csp_regime(vix_close)
    allowed = allowed_tiers_for_regime(reg)

    enriched: List[dict] = []
    for idea in csp_candidates:
        tier = classify_csp_tier(idea)
        if tier == "REJECT" or tier not in allowed:
            continue
        idea2 = dict(idea)
        idea2["tier"] = tier
        idea2["score"] = score_csp_idea(idea2)
        enriched.append(idea2)

    enriched.sort(key=lambda x: x["score"], reverse=True)

    selected: List[dict] = []
    used = set()

    total_remaining = float(total_remaining_cap)
    week_remaining = float(week_remaining_cap)

    for idea in enriched:
        tkr = idea["ticker"]
        if tkr in used:
            continue

        cash = float(idea["cash_reserved"])
        if cash <= 0:
            continue

        if cash > week_remaining or cash > total_remaining:
            continue

        if idea["tier"] == "AGGRESSIVE":
            if aggressive_total >= CSP_MAX_AGGRESSIVE_TOTAL:
                continue
            if aggressive_week >= CSP_MAX_AGGRESSIVE_PER_WEEK:
                continue

        selected.append(idea)
        used.add(tkr)

        week_remaining -= cash
        total_remaining -= cash

        if idea["tier"] == "AGGRESSIVE":
            aggressive_total += 1
            aggressive_week += 1

        if week_remaining < (CSP_MAX_CASH_PER_TRADE * 0.8):
            break

    return {
        "week_id": week_id,
        "regime": reg,
        "vix_close": float(vix_close),
        "selected": selected,
        "week_remaining_after": max(week_remaining, 0.0),
        "total_remaining_after": max(total_remaining, 0.0),
    }

def make_csp_position_id(ticker: str, expiry: str, strike: float, open_date: str) -> str:
    return f"{ticker}-{expiry}-{float(strike):.2f}-{open_date}"

def add_csp_position_from_selected(today: str, week_id: str, idea: dict) -> str:
    ensure_positions_files()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    pos_id = make_csp_position_id(idea["ticker"], idea["expiry"], idea["strike"], today)

    if any((r.get("id") or "") == pos_id for r in rows):
        return pos_id

    rows.append({
        "id": pos_id,
        "open_date": today,
        "week_id": week_id,
        "ticker": idea["ticker"],
        "expiry": idea["expiry"],
        "dte_open": str(int(idea["dte"])),
        "strike": f"{float(idea['strike']):.2f}",
        "contracts": str(int(idea["contracts"])),
        "credit_mid": f"{float(idea['mid']):.2f}",
        "cash_reserved": f"{float(idea['cash_reserved']):.2f}",
        "est_premium": f"{float(idea['est_premium']):.2f}",
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
        "notes": "",
    })

    write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)
    return pos_id

def update_open_csp_status(today: dt.date) -> None:
    ensure_positions_files()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    if not rows:
        return

    open_tickers = sorted({
        (r.get("ticker") or "").strip().upper()
        for r in rows
        if (r.get("status") or "").upper() == "OPEN"
    })

    last_close: Dict[str, float] = {}
    for tkr in open_tickers:
        try:
            df = yf.download(tkr, period="7d", interval="1d", auto_adjust=False, progress=False)
            df.dropna(inplace=True)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            if df.empty:
                continue
            last_close[tkr] = float(df["Close"].iloc[-1])
        except Exception:
            continue

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue

        tkr = (r.get("ticker") or "").strip().upper()
        exp_str = (r.get("expiry") or "").strip()
        try:
            exp = dt.date.fromisoformat(exp_str)
            r["dte_remaining"] = str((exp - today).days)
        except Exception:
            pass

        try:
            strike = float(r.get("strike") or 0.0)
        except Exception:
            strike = 0.0

        px = last_close.get(tkr)
        if not px or strike <= 0:
            continue

        diff = px - strike
        r["underlying_last"] = f"{px:.2f}"
        r["strike_diff"] = f"{diff:.2f}"
        r["strike_diff_pct"] = f"{(diff/strike)*100:.2f}"
        r["itm_otm"] = "ITM" if px < strike else "OTM"

    write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)

def process_csp_expirations(today: dt.date) -> Dict[str, List[str]]:
    ensure_positions_files()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    if not rows:
        return {"expired": [], "assigned": []}

    expired, assigned = [], []

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue

        exp_str = (r.get("expiry") or "").strip()
        try:
            exp = dt.date.fromisoformat(exp_str)
        except Exception:
            continue

        if exp > today:
            continue

        ticker = (r.get("ticker") or "").strip().upper()
        strike = float(r.get("strike") or 0.0)
        contracts = int(float(r.get("contracts") or 0.0))
        shares = contracts * 100

        underlying_close = None
        try:
            start = (exp - dt.timedelta(days=7)).isoformat()
            end = (exp + dt.timedelta(days=1)).isoformat()
            df = yf.download(ticker, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
            df.dropna(inplace=True)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            if not df.empty:
                underlying_close = float(df["Close"].iloc[-1])
        except Exception:
            underlying_close = None

        if underlying_close is None:
            continue

        r["underlying_close_at_expiry"] = f"{underlying_close:.2f}"
        r["close_date"] = exp.isoformat()

        if underlying_close >= strike:
            r["status"] = "EXPIRED"
            r["close_type"] = "EXPIRED_OTM"
            expired.append(f"{ticker} {exp_str} {strike:.0f}P")
        else:
            r["status"] = "ASSIGNED"
            r["close_type"] = "ASSIGNED_ITM"
            r["shares_if_assigned"] = str(shares)
            est_prem = float(r.get("est_premium") or 0.0)
            r["assignment_cost_basis"] = f"{(strike*shares - est_prem):.2f}"
            assigned.append(f"{ticker} {exp_str} {strike:.0f}P -> {shares} sh")

    write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)
    return {"expired": expired, "assigned": assigned}

def get_open_csp_tickers(today: dt.date) -> set:
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    out = set()
    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue
        tkr = (r.get("ticker") or "").strip().upper()
        if not tkr:
            continue
        exp_str = (r.get("expiry") or "").strip()
        try:
            exp = dt.date.fromisoformat(exp_str)
            if exp < today:
                continue
        except Exception:
            pass
        out.add(tkr)
    return out

# ----------------------------
# CC planning from assigned CSPs (classic wheel)
# ----------------------------

def decide_cc_strike(current_price: float, assigned_strike: float) -> Tuple[str, Optional[float]]:
    pct_from = (current_price - assigned_strike) / assigned_strike
    if abs(pct_from) <= 0.02:
        return "SELL_CC", max(current_price, assigned_strike) * 1.02
    if -0.08 <= pct_from < -0.02:
        return "SELL_CC", assigned_strike
    return "WAIT", None

def _round_call_strike_to_chain(calls_df: pd.DataFrame, target_strike: float) -> float:
    strikes = sorted([float(s) for s in calls_df["strike"].tolist()])
    above = [s for s in strikes if s >= target_strike]
    if not above:
        return strikes[-1]
    return above[0]

def plan_covered_calls(today: dt.date, assigned_rows: List[dict], open_cc_tickers: set) -> List[dict]:
    ideas = []
    for pos in assigned_rows:
        ticker = (pos.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        if ticker in open_cc_tickers:
            continue

        try:
            shares = int(float(pos.get("shares_if_assigned") or 0))
        except Exception:
            shares = 0
        contracts = shares // 100
        if contracts < 1:
            continue

        try:
            assigned_strike = float(pos.get("strike") or 0.0)
        except Exception:
            assigned_strike = 0.0
        if assigned_strike <= 0:
            continue

        try:
            df = add_indicators(download_ohlcv(ticker))
            last = df.iloc[-1]
            current_price = float(last["Close"])
        except Exception:
            continue

        decision, target = decide_cc_strike(current_price, assigned_strike)
        if decision != "SELL_CC" or not target:
            continue

        try:
            t = yf.Ticker(ticker)
            exp_str, dte = _pick_expiry_in_dte_range(t, 14, 30)
            if not exp_str:
                continue
            chain = t.option_chain(exp_str)
            calls = chain.calls.copy()
            if calls.empty:
                continue

            strike = _round_call_strike_to_chain(calls, target)
            row = calls.loc[calls["strike"] == strike].iloc[0]
            bid = float(row.get("bid", 0) or 0)
            ask = float(row.get("ask", 0) or 0)
            if bid <= 0 or ask < bid:
                continue
            mid = (bid + ask) / 2.0

            ideas.append({
                "ticker": ticker,
                "expiry": exp_str,
                "strike": float(strike),
                "contracts": int(contracts),
                "mid": float(mid),
                "reason": f"Wheel CC vs assigned {assigned_strike:.0f}",
            })
        except Exception:
            continue

    return ideas

def load_open_cc_tickers() -> set:
    ensure_positions_files()
    rows = load_csv_rows(CC_POSITIONS_FILE)
    out = set()
    for r in rows:
        if (r.get("status") or "").upper() == "OPEN":
            t = (r.get("ticker") or "").strip().upper()
            if t:
                out.add(t)
    return out

def make_cc_position_id(ticker: str, expiry: str, strike: float, open_date: str) -> str:
    return f"{ticker}-{expiry}-{float(strike):.2f}-{open_date}"

def add_cc_position_from_candidate(today: str, idea: dict) -> str:
    ensure_positions_files()
    rows = load_csv_rows(CC_POSITIONS_FILE)
    cc_id = make_cc_position_id(idea["ticker"], idea["expiry"], idea["strike"], today)
    if any((r.get("id") or "") == cc_id for r in rows):
        return cc_id

    rows.append({
        "id": cc_id,
        "open_date": today,
        "ticker": idea["ticker"],
        "expiry": idea["expiry"],
        "strike": f"{float(idea['strike']):.2f}",
        "contracts": str(int(idea["contracts"])),
        "credit_mid": f"{float(idea['mid']):.2f}",
        "status": "OPEN",
        "close_date": "",
        "close_type": "",
        "notes": idea.get("reason", ""),
    })
    write_csv_rows(CC_POSITIONS_FILE, rows, CC_POSITIONS_COLUMNS)
    return cc_id
