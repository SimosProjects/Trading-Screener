import datetime as dt
from typing import List

import pandas as pd
import yfinance as yf # yahoo finance
import ta # technical analysis library

import csv
import os
import requests

# ---- Discord Webhook ---- #
WEBHOOK_URL = "https://discord.com/api/webhooks/1445480294500270081/pBeMhblXLTybjfht9YPOuC8YshLxXD52BKb-IL7TR9YMt1i4fcqteMcbG9sqrzRYnlr_"

# ---- Configuration ---- #
POSITIONS_FILE = "open_positions.csv"
TRADES_LOG_FILE = "closed_trades.csv"
CSP_LEDGER_FILE = "csp_ledger.csv"
CSP_POSITIONS_FILE = "csp_positions.csv"
CC_POSITIONS_FILE = "cc_positions.csv"
CSP_MONTHLY_DIR = "csp_monthly"

STOCKS: List[str] = [
    "AAPL", "NVDA", "MSFT", "AMZN", "META", "GOOGL", "TSLA", "AMD",
    "AVGO", "WMT", "V", "NFLX", "MU", "CELH", "BROS", "ACHR", "TSM",
    "RKLB", "GTLB", "JOBY", "SOFI", "QQQ", "INTC", "DKNG",
    "ASTS", "APLD", "LLY", "JPM", "PLTR", "BAC", "ASML", "ARM", "MCHP",
    "MS", "AXP", "GS", "IONQ", "TREE", "HIMS", "SHOP", "LSCC", "ON", "SMCI",
    "CRWD", "NET", "SNOW", "ZS", "PANW", "MDB", "PAYC", "BILL", "AFRM", 
    "ADYEY", "GLBE", "VRTX", "REGN", "TMDX", "EXAS", "CAT", "DE", "ANET", "ENPH", 
    "FSLR", "RUN", "CARR", "MOD", "F", "LULU", "CMG", "TGT", "COST", "ABNB", "UBER"
]

# Pull data 1 year out in daily intervals
DATA_PERIOD = "1y"
DATA_INTERVAL = "1d"

# ---- CSP (Cash-Secured Put) Configuration ---- #
ENABLE_CSP = True

CSP_STOCKS: List[str] = list(dict.fromkeys(
    STOCKS + [
        # ETFs for consistency & liquidity
        "IWM", "XLF", "XLK", "SMH", "XLE",

        # Extra large-cap / income-friendly names
        "XOM", "CVX", "KO", "PEP", "ABBV", "UNH",
        "HD", "LOW", "DIS", "CMCSA",

        # Optional higher-IV liquid names
        "COIN", "BBAI", "SOUN", "QUBT", "CLSK"
    ]
))

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

ACCOUNT_SIZE = 110_000
CSP_MAX_TOTAL_ALLOCATION_PCT = 0.75 
CSP_MAX_TOTAL_ALLOCATION = int(ACCOUNT_SIZE * CSP_MAX_TOTAL_ALLOCATION_PCT)

# Ladder: 1/4 per week, ~30-45 DTE
CSP_WEEKLY_TARGET_ALLOCATION = CSP_MAX_TOTAL_ALLOCATION / 4.0
# Per-trade cap
CSP_MAX_CASH_PER_TRADE = 7_000
# Premium expectations
CSP_MIN_PREMIUM_CONSERVATIVE = 200
CSP_MIN_PREMIUM_BALANCED = 300
CSP_MIN_PREMIUM_AGGRESSIVE = 400
# Yield expectations (premium / cash_reserved)
CSP_MIN_YIELD_CONSERVATIVE = 0.03   # 3% for ~month
CSP_MIN_YIELD_BALANCED = 0.04       # 4%
CSP_MIN_YIELD_AGGRESSIVE = 0.05     # 5%
# DTE window
CSP_TARGET_DTE_MIN = 25
CSP_TARGET_DTE_MAX = 45
# liquidity filters
CSP_MIN_OI = 100                 
CSP_MIN_VOLUME = 10
CSP_MIN_BID = 0.10
# Tier caps (prevents going too wild)
CSP_MAX_AGGRESSIVE_TOTAL = 2
CSP_MAX_AGGRESSIVE_PER_WEEK = 1
# Strike selection
# "ema21_atr" = strike near EMA21 - (k * ATR)
CSP_STRIKE_MODE = "ema21_atr"
CSP_ATR_MULT = 1.0                  # 1.0 ATR below EMA21
# “Notch below balanced” = slightly further OTM than balanced
CSP_ATR_MULT_CONSERVATIVE = 0.75
CSP_ATR_MULT_BALANCED = 0.50
CSP_ATR_MULT_AGGRESSIVE = 0.25
# Additional CSP sanity filter
CSP_MIN_IV = 0.30                   # 30% IV, optional

# ---- CC (Covered Call) Configuration ---- #
CC_POSITIONS_COLUMNS = [
    "id",
    "open_date",
    "ticker",
    "expiry",
    "strike",
    "contracts",
    "credit_mid",
    "status",          # OPEN / EXPIRED / CALLED_AWAY
    "close_date",
    "close_type",
    "notes"
]


# ---- Methods ---- #

def send_discord(message: str):
    """Send message(s) to Discord via webhook, splitting to avoid 2000-char limit."""
    if not WEBHOOK_URL:
        print("No WEBHOOK_URL set, skipping Discord notification.")
        return

    MAX_LEN = 1900  # safety buffer
    parts = []

    msg = message.strip()
    while len(msg) > MAX_LEN:
        # split on last newline before MAX_LEN
        cut = msg.rfind("\n", 0, MAX_LEN)
        if cut == -1:
            cut = MAX_LEN
        parts.append(msg[:cut].rstrip())
        msg = msg[cut:].lstrip()

    if msg:
        parts.append(msg)

    try:
        for i, part in enumerate(parts, start=1):
            payload = {"content": part if len(parts) == 1 else f"{part}\n\n(Part {i}/{len(parts)})"}
            resp = requests.post(WEBHOOK_URL, json=payload, timeout=10)
            if resp.status_code >= 300:
                print(f"Discord webhook error: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Error sending Discord message: {e}")


""" Download OHLCV data for a single ticker """
def download_ohlcv(ticker: str) -> pd.DataFrame:
    df = yf.download(
        ticker,
        period=DATA_PERIOD,
        interval=DATA_INTERVAL,
        auto_adjust=False
    )
    df.dropna(inplace=True)

    # If MultiIndex columns, flatten first
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    return df


""" Add all the indicators to the dataframe """
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    volume = df["Volume"]

    # Simple Moving Averages
    df["SMA_50"] = ta.trend.sma_indicator(close, window=50)
    df["SMA_200"] = ta.trend.sma_indicator(close, window=200)

    # Exponential Moving Averages
    df["EMA_21"] = ta.trend.ema_indicator(close, window=21)
    df["EMA_10"] = ta.trend.ema_indicator(close, window=10)

    # ATR(14) - Average True Range
    df["ATR_14"] = ta.volatility.average_true_range(
        high=high, low=low, close=close, window=14
    )

    # RSI(2) - Relative Strength Index
    df["RSI_2"] = ta.momentum.rsi(close, window=2)

    # ADX(14) - Average Directional Index
    df["ADX_14"] = ta.trend.adx(high=high, low=low, close=close, window=14)

    # Volume SMA 10
    df["VOL_SMA_10"] = volume.rolling(window=10).mean()

    # 20-day High
    df["HIGH_20"] = close.rolling(window=20).max()

    return df


""" Relative strength vs SPY over 'lookback' days (percentage outperformance) """
def compute_relative_strength(stock_df: pd.DataFrame, spy_df: pd.DataFrame, lookback: int = 20) -> float:
    # SPY and the stock to compare must have at least 20 days of history
    if len(stock_df) < lookback or len(spy_df) < lookback:
        print("Less than 20 days of history, stock skipped.")
        return 0.0
    
    stock_recent = stock_df["Close"].iloc[-lookback]
    stock_last = stock_df["Close"].iloc[-1]
    spy_recent = spy_df["Close"].iloc[-lookback]
    spy_last = spy_df["Close"].iloc[-1]

    stock_return = (stock_last - stock_recent) / stock_recent
    spy_return = (spy_last - spy_recent) / spy_recent

    return stock_return - spy_return


# ------- STRATEGY LOGIC -------- #

""" Market trading filter: SPY > 200 SMA, QQQ > 50 SMA, VIX < 25 """
def allow_trading(spy_df: pd.DataFrame, qqq_df: pd.DataFrame, vix_df: pd.DataFrame) -> bool:
    spy_last = add_indicators(spy_df).iloc[-1]
    qqq_last = add_indicators(qqq_df).iloc[-1]
    vix_close = vix_df["Close"].iloc[-1]

    spy_close = spy_last["Close"]
    qqq_close = qqq_last["Close"]

    spy_above_sma200 = spy_close > spy_last["SMA_200"]
    spy_above_sma50  = spy_close > spy_last["SMA_50"]
    spy_above_ema21  = spy_close > spy_last["EMA_21"]

    qqq_above_sma50  = qqq_close > qqq_last["SMA_50"]
    vix_ok = vix_close < 25

    cond_spy = spy_above_sma200
    cond_qqq = qqq_above_sma50
    cond_vix = vix_ok

    print("\nSPY trend:")
    print(f"  Above 200 SMA: {spy_above_sma200}")
    print(f"  Above 50 SMA:  {spy_above_sma50}")
    print(f"  Above 21 EMA:  {spy_above_ema21}")

    print("\nQQQ trend:")
    print(f"  Above 50 SMA:  {qqq_above_sma50}")

    print("\nVIX trend:")
    print(f"  VIX < 25:      {vix_ok}")
    print()

    return bool(cond_spy and cond_qqq and cond_vix)

def is_eligible(stock_row: pd.Series, rs_20: float):
    """
    Trend filter for the stock.

    Returns:
        eligible (bool),
        details (dict of individual condition flags)
    """
    cond_close_above_sma50 = stock_row["Close"] > stock_row["SMA_50"]
    cond_ema21_above_sma50 = stock_row["EMA_21"] > stock_row["SMA_50"]
    cond_rs_positive = rs_20 > 0
    cond_adx_ok = stock_row["ADX_14"] > 20

    eligible = bool(
        cond_close_above_sma50
        and cond_ema21_above_sma50
        and cond_rs_positive
        and cond_adx_ok
    )

    details = {
        "close_above_sma50": cond_close_above_sma50,
        "ema21_above_sma50": cond_ema21_above_sma50,
        "rs20_positive": cond_rs_positive,
        "adx14_gt_20": cond_adx_ok,
    }

    return eligible, details


""" Pullback entry signal: RSI(2) < 5 and price near 21 EMA """
def pullback_signal(stock_row: pd.Series) -> bool:
    rsi_ok = stock_row["RSI_2"] < 5
    ema_21 = stock_row["EMA_21"]
    close = stock_row["Close"]
    # within 0.5% of EMA 21
    near_ema = abs(close - ema_21) / ema_21 < 0.005

    return bool(rsi_ok and near_ema)

""" Breakout entry signal: close > 20-day high and volume > 1.5 x 10-day avg """
def breakout_signal(stock_row: pd.Series) -> bool:
    cond_price = stock_row["Close"] > stock_row["HIGH_20"]
    cond_vol = stock_row["Volume"] > 1.5 * stock_row["VOL_SMA_10"]

    return bool(cond_price and cond_vol)

""" Load open positions and store by ticker """
def load_open_positions():
    positions = {}
    if not os.path.isfile(POSITIONS_FILE):
        return positions
    with open(POSITIONS_FILE, mode="r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            positions[row["ticker"]] = row
    return positions

""" Once an alert is thrown for a ticker, save the open position """
def save_open_positions(positions: dict):
    fieldnames = ["ticker", "entry_date", "entry_price", "entry_type", "initial_stop"]
    with open(POSITIONS_FILE, mode="w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for ticker, pos in positions.items():
            writer.writerow(pos)

""" Log stats when a trade was closed """
def log_closed_trade(trade: dict):
    fieldnames = [
        "ticker",
        "entry_date",
        "entry_price",
        "exit_date",
        "exit_price",
        "reason",
        "pnl_abs",
        "pnl_pct",
    ]
    file_exists = os.path.isfile(TRADES_LOG_FILE)
    with open(TRADES_LOG_FILE, mode="a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(trade)


# ------- CSP STRATEGY LOGIC -------- #

def _iso_week_id(d: dt.date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"

def load_csp_ledger():
    rows = []
    if not os.path.isfile(CSP_LEDGER_FILE):
        return rows
    with open(CSP_LEDGER_FILE, mode="r", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows

def append_csp_ledger_row(row: dict):
    """
    Row fields:
      date, week_id, ticker, expiry, strike, contracts, credit_mid,
      cash_reserved, est_premium, tier
    """
    fieldnames = [
        "date","week_id","ticker","expiry","strike","contracts",
        "credit_mid","cash_reserved","est_premium","tier"
    ]
    file_exists = os.path.isfile(CSP_LEDGER_FILE)
    with open(CSP_LEDGER_FILE, mode="a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        w.writerow(row)

def current_csp_exposure(ledger_rows: list, week_id: str):
    """
    We assume everything in the ledger is 'open collateral' until expiry passes.
    (Simple + conservative; later we can add status/close handling.)
    """
    today = dt.date.today()
    total_reserved = 0.0
    week_reserved = 0.0
    aggressive_total = 0
    aggressive_week = 0

    for r in ledger_rows:
        try:
            exp = dt.date.fromisoformat(r["expiry"])
            if exp < today:
                continue  # expired -> collateral released
            cash = float(r["cash_reserved"])
            total_reserved += cash
            if r["week_id"] == week_id:
                week_reserved += cash

            tier = (r.get("tier") or "").upper()
            if tier == "AGGRESSIVE":
                aggressive_total += 1
                if r["week_id"] == week_id:
                    aggressive_week += 1
        except Exception:
            continue

    return total_reserved, week_reserved, aggressive_total, aggressive_week

def _pick_expiry_in_dte_range(ticker_obj: yf.Ticker, dte_min: int, dte_max: int):
    """Pick the first expiry whose DTE falls inside [dte_min, dte_max]."""
    today = dt.date.today()
    expiries = []
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

def _suggest_strike(stock_last: pd.Series, atr_mult: float) -> float:
    close = float(stock_last["Close"])
    ema21 = float(stock_last["EMA_21"])
    atr14 = float(stock_last["ATR_14"])

    if CSP_STRIKE_MODE == "ema21_atr":
        return ema21 - (atr_mult * atr14)

    return close * 0.92

def _round_strike_to_chain(puts_df: pd.DataFrame, target_strike: float) -> float:
    """Round down to the nearest available strike in the chain."""
    strikes = sorted([float(s) for s in puts_df["strike"].tolist()])
    below = [s for s in strikes if s <= target_strike]
    # if target is below lowest strike, use lowest
    if not below:
        return strikes[0]
    return below[-1]

def _round_call_strike_to_chain(calls_df: pd.DataFrame, target_strike: float) -> float:
    """Round UP to nearest available call strike."""
    strikes = sorted([float(s) for s in calls_df["strike"].tolist()])
    above = [s for s in strikes if s >= target_strike]
    # if target above highest, use highest
    if not above:
        return strikes[-1]
    return above[0]

def evaluate_csp_candidate(ticker: str, stock_last: pd.Series, atr_mult: float):
    try:
        t = yf.Ticker(ticker)
        exp_str, dte = _pick_expiry_in_dte_range(t, CSP_TARGET_DTE_MIN, CSP_TARGET_DTE_MAX)
        if not exp_str:
            return None

        chain = t.option_chain(exp_str)
        puts = chain.puts.copy()
        if puts.empty:
            return None

        raw_strike = _suggest_strike(stock_last, atr_mult=atr_mult)
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
        yield_pct = est_premium / cash_reserved  # decimal (e.g., 0.035)

        return {
            "ticker": ticker,
            "expiry": exp_str,
            "dte": dte,
            "strike": float(strike),
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "iv": iv,
            "contracts": contracts,
            "cash_reserved": cash_reserved,
            "est_premium": est_premium,
            "yield_pct": yield_pct,
            "reason": f"Strike≈EMA21-{atr_mult:.2f}*ATR (raw {raw_strike:.2f})",
        }

    except Exception as e:
        print(f"[CSP] Error evaluating {ticker}: {e}")
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

    # Conservative is only a notch below balanced
    if prem >= CSP_MIN_PREMIUM_AGGRESSIVE and y >= CSP_MIN_YIELD_AGGRESSIVE:
        return "AGGRESSIVE"
    if prem >= CSP_MIN_PREMIUM_BALANCED and y >= CSP_MIN_YIELD_BALANCED:
        return "BALANCED"
    if prem >= CSP_MIN_PREMIUM_CONSERVATIVE and y >= CSP_MIN_YIELD_CONSERVATIVE:
        return "CONSERVATIVE"
    return "REJECT"

def score_csp_idea(idea: dict) -> float:
    # simple scoring: prefer higher premium, higher yield, higher IV (but not insane)
    prem = float(idea["est_premium"])
    y = float(idea["yield_pct"])
    iv = float(idea["iv"])
    dte = float(idea["dte"])

    # normalize-ish
    s = 0.0
    s += min(prem / 250.0, 2.0)
    s += min(y / 0.04, 2.0)          # 4% monthly-ish
    s += min(iv / 0.45, 1.5)
    s += 0.5 if 30 <= dte <= 40 else 0.0
    return s

def allowed_tiers_for_regime(reg: str):
    # Keep “conservative” not too conservative: still allow BALANCED basically always.
    if reg == "LOW_IV":
        return {"CONSERVATIVE", "BALANCED"}          # avoid forcing aggressive in low IV
    if reg == "NORMAL":
        return {"CONSERVATIVE", "BALANCED", "AGGRESSIVE"}
    return {"CONSERVATIVE", "BALANCED", "AGGRESSIVE"}  # high IV ok, but caps still apply

def plan_weekly_csp_orders(csp_candidates: list, vix_close: float):
    """
    Select CSP ideas to meet weekly tranche target while respecting total allocation and aggressive caps.
    Also uses the ledger to avoid over-allocating.
    """
    today = dt.date.today()
    week_id = _iso_week_id(today)
    ledger = load_csp_ledger()

    total_reserved, week_reserved, aggressive_total, aggressive_week = current_csp_exposure(ledger, week_id)

    total_remaining = CSP_MAX_TOTAL_ALLOCATION - total_reserved
    week_remaining = CSP_WEEKLY_TARGET_ALLOCATION - week_reserved

    reg = csp_regime(vix_close)
    allowed = allowed_tiers_for_regime(reg)

    # Attach tier + score, filter rejects
    enriched = []
    for idea in csp_candidates:
        tier = classify_csp_tier(idea)
        if tier == "REJECT":
            continue
        if tier not in allowed:
            continue
        idea2 = dict(idea)
        idea2["tier"] = tier
        idea2["score"] = score_csp_idea(idea2)
        enriched.append(idea2)

    # Sort best first
    enriched.sort(key=lambda x: x["score"], reverse=True)

    selected = []
    used_tickers = set()

    for idea in enriched:
        if idea["ticker"] in used_tickers:
            continue

        cash = float(idea["cash_reserved"])
        if cash <= 0:
            continue

        # Respect remaining budgets
        if cash > week_remaining:
            continue
        if cash > total_remaining:
            continue

        # Aggressive caps
        if idea["tier"] == "AGGRESSIVE":
            if aggressive_total >= CSP_MAX_AGGRESSIVE_TOTAL:
                continue
            if aggressive_week >= CSP_MAX_AGGRESSIVE_PER_WEEK:
                continue

        selected.append(idea)
        used_tickers.add(idea["ticker"])

        week_remaining -= cash
        total_remaining -= cash

        if idea["tier"] == "AGGRESSIVE":
            aggressive_total += 1
            aggressive_week += 1

        # stop once weekly budget is basically filled
        if week_remaining < (CSP_MAX_CASH_PER_TRADE * 0.8):
            break

    return {
        "week_id": week_id,
        "regime": reg,
        "vix_close": vix_close,
        "total_reserved": total_reserved,
        "week_reserved": week_reserved,
        "total_remaining": max(total_remaining, 0),
        "week_remaining": max(week_remaining, 0),
        "selected": selected
    }

def csp_already_logged(ledger_rows: list, week_id: str, ticker: str, expiry: str, strike: float) -> bool:
    """
    Returns True if this CSP (same week, ticker, expiry, strike) already exists.
    """
    for r in ledger_rows:
        try:
            if (
                r["week_id"] == week_id
                and r["ticker"] == ticker
                and r["expiry"] == expiry
                and float(r["strike"]) == float(strike)
            ):
                return True
        except Exception:
            continue
    return False

def is_csp_eligible(stock_row: pd.Series) -> bool:
    # Keep it “not crazy” but not overly restrictive
    return bool(
        stock_row["Close"] > stock_row["SMA_200"] and
        stock_row["Volume"] > 1_000_000
    )

def load_csv_rows(path: str) -> list:
    if not os.path.isfile(path):
        return []
    with open(path, "r", newline="") as f:
        return list(csv.DictReader(f))

def write_csv_rows(path: str, rows: list, fieldnames: list):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def ensure_csp_positions_file():
    if os.path.isfile(CSP_POSITIONS_FILE):
        return
    write_csv_rows(CSP_POSITIONS_FILE, [], CSP_POSITIONS_COLUMNS)

def make_csp_position_id(ticker: str, expiry: str, strike: float, open_date: str) -> str:
    # stable-ish ID
    return f"{ticker}-{expiry}-{float(strike):.2f}-{open_date}"

def add_csp_position_from_selected(today: str, week_id: str, idea: dict):
    ensure_csp_positions_file()
    rows = load_csv_rows(CSP_POSITIONS_FILE)

    pos_id = make_csp_position_id(idea["ticker"], idea["expiry"], idea["strike"], today)
    # prevent duplicates if you run multiple times/day
    for r in rows:
        if r.get("id") == pos_id:
            return

    rows.append({
        "id": pos_id,
        "open_date": today,
        "week_id": week_id,
        "ticker": idea["ticker"],
        "expiry": idea["expiry"],
        "dte_open": int(idea["dte"]),
        "strike": f"{float(idea['strike']):.2f}",
        "contracts": int(idea["contracts"]),
        "credit_mid": f"{float(idea['mid']):.2f}",
        "cash_reserved": f"{float(idea['cash_reserved']):.2f}",
        "est_premium": f"{float(idea['est_premium']):.2f}",
        "status": "OPEN",
        "close_date": "",
        "close_type": "",
        "underlying_close_at_expiry": "",
        "shares_if_assigned": "",
        "assignment_cost_basis": "",
        "notes": "",
    })

    write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)

def update_open_csp_status(today: dt.date):
    """
    For each OPEN CSP in csp_positions.csv, update:
      - underlying_last
      - strike_diff (underlying - strike)
      - strike_diff_pct ((underlying - strike) / strike)
      - dte_remaining
      - itm_otm (ITM if underlying < strike else OTM)

    Runs fast enough for daily EOD.
    """
    ensure_csp_positions_file()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    if not rows:
        return

    # Ensure new columns exist even if file was created earlier
    needed_cols = [
        "underlying_last", "strike_diff", "strike_diff_pct", "dte_remaining", "itm_otm"
    ]
    for r in rows:
        for c in needed_cols:
            if c not in r:
                r[c] = ""

    # Fetch latest closes once per ticker (avoid repeated downloads)
    open_tickers = sorted({
        (r.get("ticker") or "").strip().upper()
        for r in rows
        if (r.get("status") or "").upper() == "OPEN"
    })

    last_close_map = {}
    for tkr in open_tickers:
        try:
            df = yf.download(tkr, period="7d", interval="1d", auto_adjust=False, progress=False)
            df.dropna(inplace=True)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            if df.empty:
                continue
            last_close_map[tkr] = float(df["Close"].iloc[-1])
        except Exception:
            continue

    # Update each open row
    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue

        tkr = (r.get("ticker") or "").strip().upper()
        if not tkr:
            continue

        # DTE remaining
        try:
            exp = dt.date.fromisoformat((r.get("expiry") or "").strip())
            dte_rem = (exp - today).days
            r["dte_remaining"] = str(dte_rem)
        except Exception:
            r["dte_remaining"] = ""

        # Underlying last + distance to strike
        strike = None
        try:
            strike = float(r.get("strike") or 0)
        except Exception:
            strike = None

        last_px = last_close_map.get(tkr)
        if last_px is None or strike is None or strike <= 0:
            # can’t compute today
            continue

        diff = last_px - strike
        diff_pct = diff / strike

        r["underlying_last"] = f"{last_px:.2f}"
        r["strike_diff"] = f"{diff:.2f}"
        r["strike_diff_pct"] = f"{diff_pct*100:.2f}"  # percent
        r["itm_otm"] = "ITM" if last_px < strike else "OTM"

    # Write back
    write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)

def process_csp_expirations(today: dt.date):
    ensure_csp_positions_file()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    if not rows:
        return {"expired": [], "assigned": []}

    changed_expired = []
    changed_assigned = []

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue

        try:
            exp = dt.date.fromisoformat(r["expiry"])
        except Exception:
            continue

        # only process when expiry has passed (or is today)
        if exp > today:
            continue

        ticker = r["ticker"]
        strike = float(r["strike"])
        contracts = int(float(r["contracts"]))
        shares = contracts * 100

        # get underlying close for expiry date (best-effort)
        try:
            start = (exp - dt.timedelta(days=7)).isoformat()
            end = (exp + dt.timedelta(days=1)).isoformat()
            df = yf.download(ticker, start=start, end=end, interval="1d", auto_adjust=False)
            df.dropna(inplace=True)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            if df.empty:
                continue

            # last close on or before expiry
            underlying_close = float(df["Close"].iloc[-1])
        except Exception:
            underlying_close = None

        if underlying_close is None:
            continue

        r["underlying_close_at_expiry"] = f"{underlying_close:.2f}"
        r["close_date"] = exp.isoformat()

        # Infer outcome
        if underlying_close >= strike:
            r["status"] = "EXPIRED"
            r["close_type"] = "EXPIRED_OTM"
            changed_expired.append(f"{ticker} {r['expiry']} {strike:.0f}P")
        else:
            r["status"] = "ASSIGNED"
            r["close_type"] = "ASSIGNED_ITM"
            r["shares_if_assigned"] = str(shares)
            # cost basis if assigned (strike * shares) minus premium collected (approx)
            est_prem = float(r.get("est_premium") or 0)
            basis = (strike * shares) - est_prem
            r["assignment_cost_basis"] = f"{basis:.2f}"
            changed_assigned.append(f"{ticker} {r['expiry']} {strike:.0f}P -> {shares} sh")

    # write back
    write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)

    return {"expired": changed_expired, "assigned": changed_assigned}

def get_open_csp_tickers(today: dt.date) -> set:
    """
    Returns set of tickers that currently have an OPEN CSP position.
    Uses csp_positions.csv and ignores positions whose expiry < today.
    """
    if not os.path.isfile(CSP_POSITIONS_FILE):
        return set()

    rows = load_csv_rows(CSP_POSITIONS_FILE)
    open_tickers = set()

    for r in rows:
        if (r.get("status") or "").upper() != "OPEN":
            continue

        t = (r.get("ticker") or "").strip().upper()
        if not t:
            continue

        # If expiry is invalid, be conservative and treat as open
        exp_str = (r.get("expiry") or "").strip()
        try:
            exp = dt.date.fromisoformat(exp_str)
            if exp < today:
                continue
        except Exception:
            pass

        open_tickers.add(t)

    return open_tickers

def update_csp_position_notes(pos_id: str, notes: str):
    """Persist notes into csp_positions.csv for a specific position id."""
    ensure_csp_positions_file()
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    changed = False

    for r in rows:
        if (r.get("id") or "") == pos_id:
            r["notes"] = notes
            changed = True
            break

    if changed:
        write_csv_rows(CSP_POSITIONS_FILE, rows, CSP_POSITIONS_COLUMNS)

def update_csp_monthly_csvs_from_ledger():
    """
    Creates/updates ONE CSV PER MONTH based on OPEN DATE (sale date) from csp_ledger.csv.

    Output files:
      csp_monthly/YYYY-MM.csv

    Each monthly file contains:
      date, ticker, strategy, action, contracts, strike, expiration, premium_total
    plus a final TOTAL row that sums premium_total for the month.

    Rebuilds files from the ledger each run (so totals are always correct).
    """
    rows = load_csv_rows(CSP_LEDGER_FILE)

    # Ensure output directory exists
    os.makedirs(CSP_MONTHLY_DIR, exist_ok=True)

    fieldnames = [
        "date",
        "ticker",
        "strategy",
        "action",
        "contracts",
        "strike",
        "expiration",
        "premium_total",
    ]

    if not rows:
        # Nothing to write; optionally keep the folder empty.
        return

    # Group ledger rows by YYYY-MM
    by_month = {}  # month -> list[dict]
    for r in rows:
        date_str = (r.get("date") or "").strip()
        if len(date_str) < 7:
            continue
        month = date_str[:7]
        by_month.setdefault(month, []).append(r)

    for month, month_rows in by_month.items():
        # Sort by date then ticker for readability
        def _sort_key(x):
            return ((x.get("date") or ""), (x.get("ticker") or ""))
        month_rows_sorted = sorted(month_rows, key=_sort_key)

        out_rows = []
        total_premium = 0.0

        for r in month_rows_sorted:
            prem = float(r.get("est_premium") or 0.0)
            total_premium += prem

            out_rows.append({
                "date": (r.get("date") or "").strip(),
                "ticker": (r.get("ticker") or "").strip(),
                "strategy": "CSP",
                "action": "SELL_TO_OPEN",
                "contracts": int(float(r.get("contracts") or 0)),
                "strike": f"{float(r.get('strike') or 0):.2f}",
                "expiration": (r.get("expiry") or "").strip(),
                "premium_total": f"{prem:.2f}",
            })

        # Add a final TOTAL row
        out_rows.append({
            "date": "",
            "ticker": "TOTAL",
            "strategy": "",
            "action": "",
            "contracts": "",
            "strike": "",
            "expiration": "",
            "premium_total": f"{total_premium:.2f}",
        })

        # Write/replace the monthly file
        monthly_path = os.path.join(CSP_MONTHLY_DIR, f"{month}.csv")
        write_csv_rows(monthly_path, out_rows, fieldnames)

def get_assigned_csp_positions():
    rows = load_csv_rows(CSP_POSITIONS_FILE)
    assigned = []

    for r in rows:
        if (r.get("status") or "").upper() != "ASSIGNED":
            continue
        shares = int(float(r.get("shares_if_assigned") or 0))
        if shares <= 0:
            continue
        assigned.append(r)

    return assigned

def decide_cc_strike(current_price: float, csp_strike: float) -> tuple:
    """
    Returns:
      (decision, target_strike)
    decision:
      SELL_CC / WAIT
    """
    pct_from_strike = (current_price - csp_strike) / csp_strike

    if abs(pct_from_strike) <= 0.02:
        return "SELL_CC", max(current_price, csp_strike) * 1.02

    if -0.08 <= pct_from_strike < -0.02:
        return "SELL_CC", csp_strike

    return "WAIT", None

def plan_covered_calls(today: dt.date):
    open_cc_tickers = load_open_cc_tickers()
    assigned = get_assigned_csp_positions()
    ideas = []

    for pos in assigned:
        ticker = pos["ticker"]
        csp_strike = float(pos["strike"])
        shares = int(float(pos["shares_if_assigned"]))
        contracts = shares // 100

        if ticker.upper() in open_cc_tickers:
            continue

        if contracts < 1:
            continue

        try:
            df = download_ohlcv(ticker)
            last = add_indicators(df).iloc[-1]
            current_price = float(last["Close"])
        except Exception:
            continue

        decision, target_strike = decide_cc_strike(current_price, csp_strike)

        if decision == "WAIT":
            update_csp_position_notes(pos.get("id", ""), "Waiting for recovery before CC")
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

            strike = _round_call_strike_to_chain(calls, target_strike)
            row = calls.loc[calls["strike"] == strike].iloc[0]

            bid = float(row.get("bid", 0) or 0)
            ask = float(row.get("ask", 0) or 0)
            if bid <= 0 or ask < bid:
                continue

            mid = (bid + ask) / 2.0

            ideas.append({
                "ticker": ticker,
                "expiry": exp_str,
                "strike": strike,
                "contracts": contracts,
                "mid": mid,
                "reason": f"Wheel CC vs CSP {csp_strike}",
            })

        except Exception:
            continue

    return ideas

def ensure_cc_positions_file():
    if os.path.isfile(CC_POSITIONS_FILE):
        return
    write_csv_rows(CC_POSITIONS_FILE, [], CC_POSITIONS_COLUMNS)

def make_cc_position_id(ticker: str, expiry: str, strike: float, open_date: str) -> str:
    return f"{ticker}-{expiry}-{float(strike):.2f}-{open_date}"

def load_open_cc_tickers() -> set:
    """Tickers that currently have an OPEN covered call in cc_positions.csv."""
    ensure_cc_positions_file()
    rows = load_csv_rows(CC_POSITIONS_FILE)
    out = set()
    for r in rows:
        if (r.get("status") or "").upper() == "OPEN":
            t = (r.get("ticker") or "").strip().upper()
            if t:
                out.add(t)
    return out

def add_cc_position_from_candidate(today: str, idea: dict):
    """
    Records the suggested CC as an OPEN position entry.
    (This is your 'paper trade tracking'—you can later add broker execution.)
    """
    ensure_cc_positions_file()
    rows = load_csv_rows(CC_POSITIONS_FILE)

    pos_id = make_cc_position_id(idea["ticker"], idea["expiry"], idea["strike"], today)
    for r in rows:
        if r.get("id") == pos_id:
            return  # already logged

    rows.append({
        "id": pos_id,
        "open_date": today,
        "ticker": idea["ticker"],
        "expiry": idea["expiry"],
        "strike": f"{float(idea['strike']):.2f}",
        "contracts": int(idea["contracts"]),
        "credit_mid": f"{float(idea['mid']):.2f}",
        "status": "OPEN",
        "close_date": "",
        "close_type": "",
        "notes": idea.get("reason", ""),
    })

    write_csv_rows(CC_POSITIONS_FILE, rows, CC_POSITIONS_COLUMNS)


# ------- STOCK SCREENER -------- #

def run_screener():
    open_positions = load_open_positions()
    today = dt.date.today().isoformat()
    print(f"\n=== Screener for {today} ===")

    print("Downloading SPY / QQQ / VIX...")
    spy_df = download_ohlcv("SPY")
    qqq_df = download_ohlcv("QQQ")
    vix_df = download_ohlcv("^VIX")

    # Testing SPY data was collected
    print("\nLatest SPY:")
    last = spy_df.iloc[-1]
    print(f"  Open:  {last['Open']:.2f}")
    print(f"  High:  {last['High']:.2f}")
    print(f"  Low:   {last['Low']:.2f}")
    print(f"  Close: {last['Close']:.2f}")
    print(f"  Volume: {int(last['Volume']):,}")

    spy_ind = add_indicators(spy_df).iloc[-1]

    trading_on = allow_trading(spy_df, qqq_df, vix_df)

    if not trading_on:
        print("🔻 Trading OFF — No new entries allowed today.")
    else:
        print("✅ Trading ON — Scanning stocks...\n")

    entries_pullback = []
    entries_breakout = []
    watch_eligible = []
    debug_rows = []
    last_rows = {}
    csp_ideas = []
    cc_candidates = []

    open_csp_tickers = get_open_csp_tickers(dt.date.today()) if ENABLE_CSP else set()

    if ENABLE_CSP:
        for ticker in CSP_STOCKS:
            try:
                # Prevent opening another CSP on the same ticker
                if ticker.upper() in open_csp_tickers:
                    continue
        
                df = download_ohlcv(ticker)
                df = add_indicators(df)
                last = df.iloc[-1]

                if not is_csp_eligible(last):
                    continue

                # Evaluate multiple ATR distances so we actually find enough CSPs.
                # Planner will pick best one per ticker via score + tier.
                for atr_mult in (CSP_ATR_MULT_CONSERVATIVE, CSP_ATR_MULT_BALANCED, CSP_ATR_MULT_AGGRESSIVE):
                    csp = evaluate_csp_candidate(ticker, last, atr_mult=atr_mult)
                    if csp:
                        csp["atr_mult"] = atr_mult  # optional, helpful for debugging
                        csp_ideas.append(csp)

            except Exception as e:
                print(f"[CSP] Error processing {ticker}: {e}")

    for ticker in STOCKS:
        try:
            df = download_ohlcv(ticker)
            df = add_indicators(df)
            last = df.iloc[-1]
            last_rows[ticker] = last

            rs_20 = compute_relative_strength(df, spy_df, lookback=20)

            # --- Eligibility + Details ---
            eligible, elig_details = is_eligible(last, rs_20)

            # --- Signal Internals ---

            # Pullback Components
            rsi2 = last["RSI_2"]
            ema21 = last["EMA_21"]
            close = last["Close"]
            near_ema21 = abs(close - ema21) / ema21 < 0.005
            pb_signal = bool((rsi2 < 5) and near_ema21)

            # Breakout Components
            high20 = last["HIGH_20"]
            vol = last["Volume"]
            vol_sma10 = last["VOL_SMA_10"]
            price_breaks_high20 = close > high20
            vol_ok = vol > 1.5 * vol_sma10
            bo_signal = bool(price_breaks_high20 and vol_ok)

            # Category Classification
            if trading_on and eligible and pb_signal:
                category = "ENTRY_PULLBACK"
                entries_pullback.append((ticker, close, rsi2))

                if ticker not in open_positions:
                    # Initial stop: EMA21 - 1.5 * ATR_14
                    atr14 = last["ATR_14"]
                    initial_stop = round(ema21 - 1.5 * atr14, 2)
                    open_positions[ticker] = {
                        "ticker": ticker,
                        "entry_date": today,
                        "entry_price": f"{round(close, 2):.2f}",
                        "entry_type": "pullback",
                        "initial_stop": f"{initial_stop:.2f}",
                    }
            elif trading_on and eligible and bo_signal:
                category = "ENTRY_BREAKOUT"
                entries_breakout.append((ticker, close, vol, vol_sma10))

                if ticker not in open_positions:
                    # Slightly tighter initial stop: EMA21 - 1.0 * ATR_14
                    atr14 = last["ATR_14"]
                    initial_stop = round(ema21 - 1.0 * atr14, 2)
                    open_positions[ticker] = {
                        "ticker": ticker,
                        "entry_date": today,
                        "entry_price": f"{round(close, 2):.2f}",
                        "entry_type": "breakout",
                        "initial_stop": f"{initial_stop:.2f}",
                    }
            elif eligible:
                category = "WATCH_ELIGIBLE"
                watch_eligible.append(ticker)
            else:
                category = "NOT_ELIGIBLE"

            # --- CSV Log ---
            debug_rows.append({
                "date": today,
                "ticker": ticker,
                "close": round(close, 2),
                "sma50": round(last["SMA_50"], 2),
                "ema21": round(ema21, 2),
                "rs20": round(rs_20, 4),
                "adx14": round(last["ADX_14"], 2),
                "eligible": eligible,
                "close_above_sma50": elig_details["close_above_sma50"],
                "ema21_above_sma50": elig_details["ema21_above_sma50"],
                "rs20_positive": elig_details["rs20_positive"],
                "adx14_gt_20": elig_details["adx14_gt_20"],
                "rsi2": round(rsi2, 2),
                "near_ema21": near_ema21,
                "pullback_signal": pb_signal,
                "high20": round(high20, 2),
                "volume": int(vol), 
                "vol_sma10": int(vol_sma10),
                "price_breaks_high20": price_breaks_high20,
                "vol_ok": vol_ok,
                "breakout_signal": bo_signal,
                "category": category,
            })

        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    csp_plan = None
    logged_this_run = []

    if ENABLE_CSP:
        vix_close = float(vix_df["Close"].iloc[-1])
        csp_plan = plan_weekly_csp_orders(csp_ideas, vix_close=vix_close)

        ledger_rows = load_csp_ledger()

        selected_cash = sum(float(i["cash_reserved"]) for i in csp_plan["selected"])
        reserved_after_total = float(csp_plan["total_reserved"]) + selected_cash
        reserved_after_week  = float(csp_plan["week_reserved"]) + selected_cash

        for idea in csp_plan["selected"]:
            if csp_already_logged(
                ledger_rows=ledger_rows,
                week_id=csp_plan["week_id"],
                ticker=idea["ticker"],
                expiry=idea["expiry"],
                strike=idea["strike"],
            ):
                continue

            append_csp_ledger_row({
                "date": today,
                "week_id": csp_plan["week_id"],
                "ticker": idea["ticker"],
                "expiry": idea["expiry"],
                "strike": f"{idea['strike']:.2f}",
                "contracts": int(idea["contracts"]),
                "credit_mid": f"{idea['mid']:.2f}",
                "cash_reserved": f"{idea['cash_reserved']:.2f}",
                "est_premium": f"{idea['est_premium']:.2f}",
                "tier": idea["tier"],
            })

            logged_this_run.append(idea)

    #Create/track CSP positions only for newly-logged trades
    if ENABLE_CSP and csp_plan:
        for idea in logged_this_run:
            add_csp_position_from_selected(today=today, week_id=csp_plan["week_id"], idea=idea)
    
    # Process any CSPs that expired (updates csp_positions.csv) + monthly summary
    csp_expiry_updates = {"expired": [], "assigned": []}
    if ENABLE_CSP:
        csp_expiry_updates = process_csp_expirations(dt.date.today())
        update_open_csp_status(dt.date.today())
        update_csp_monthly_csvs_from_ledger()
        cc_candidates = plan_covered_calls(dt.date.today())

    cc_logged_this_run = []

    if ENABLE_CSP:
        for idea in cc_candidates:
            add_cc_position_from_candidate(today=today, idea=idea)
            cc_logged_this_run.append(idea)


    # ----- Exit logic for open positions ----- #
    exits = []

    # We work on a copy of keys so we can modify open_positions safely
    for ticker, pos in list(open_positions.items()):
        if ticker not in last_rows:
            continue  # in case stock failed to download

        # Don't evaluate exits on the same day we opened the trade
        if pos["entry_date"] == today:
            continue

        last = last_rows[ticker]
        close = float(last["Close"])
        rsi2 = float(last["RSI_2"])
        ema21 = float(last["EMA_21"])
        initial_stop = float(pos["initial_stop"])
        entry_price = float(pos["entry_price"])

        exit_reason = None

        # 1% below EMA21
        buffer_pct = 0.01
        threshold = ema21 * (1.0 - buffer_pct)

        # Profit exit: mean reversion spike
        if rsi2 > 90:
            exit_reason = "RSI2>90 (profit)"

        # Trend break exit: close below EMA21 accounting for threshold
        elif close < threshold:
            exit_reason = "Close < EMA21-1% (trend break)"  

        # Hard stop: price below initial stop
        elif close < initial_stop:
            exit_reason = "Below initial stop"

        if exit_reason:
            pnl_abs = close - entry_price
            pnl_pct = pnl_abs / entry_price * 100.0

            exits.append((ticker, exit_reason, close, pnl_abs, pnl_pct))

            # log trade
            log_closed_trade({
                "ticker": ticker,
                "entry_date": pos["entry_date"],
                "entry_price": pos["entry_price"],
                "exit_date": today,
                "exit_price": f"{close:.2f}",
                "reason": exit_reason,
                "pnl_abs": f"{pnl_abs:.2f}",
                "pnl_pct": f"{pnl_pct:.2f}",
            })

            # remove from open positions
            del open_positions[ticker]

    # Save updated open positions
    save_open_positions(open_positions)

    # Build open positions snapshot
    open_positions_snapshot = []

    for ticker, pos in open_positions.items():
        entry_price = float(pos["entry_price"])
        entry_date = pos["entry_date"]
        entry_type = pos["entry_type"]
        initial_stop = float(pos["initial_stop"])

        # If we have today's last row, compute latest price & PnL
        if ticker in last_rows:
            last = last_rows[ticker]
            close = float(last["Close"])
            pnl_abs = close - entry_price
            pnl_pct = pnl_abs / entry_price * 100.0
            open_positions_snapshot.append(
                {
                    "ticker": ticker,
                    "entry_date": entry_date,
                    "entry_type": entry_type,
                    "entry_price": entry_price,
                    "last_price": close,
                    "pnl_abs": pnl_abs,
                    "pnl_pct": pnl_pct,
                    "initial_stop": initial_stop,
                }
            )
        else:
            # Fallback: no latest price
            open_positions_snapshot.append(
                {
                    "ticker": ticker,
                    "entry_date": entry_date,
                    "entry_type": entry_type,
                    "entry_price": entry_price,
                    "last_price": None,
                    "pnl_abs": None,
                    "pnl_pct": None,
                    "initial_stop": initial_stop,
                }
            )


    # ----- Output ----- #

    # Print open positions
    if open_positions_snapshot:
        print("📂 CURRENT OPEN POSITIONS:")
        for pos in open_positions_snapshot:
            t = pos["ticker"]
            entry_date = pos["entry_date"]
            entry_price = pos["entry_price"]
            entry_type = pos["entry_type"]
            initial_stop = pos["initial_stop"]
            last_price = pos["last_price"]
            pnl_abs = pos["pnl_abs"]
            pnl_pct = pos["pnl_pct"]

            if last_price is not None:
                print(
                    f"  {t}: entry={entry_price:.2f} on {entry_date}, "
                    f"last={last_price:.2f}, "
                    f"PnL={pnl_abs:.2f} ({pnl_pct:.2f}%), "
                    f"type={entry_type}, stop={initial_stop:.2f}"
                )
            else:
                print(
                    f"  {t}: entry={entry_price:.2f} on {entry_date}, "
                    f"type={entry_type}, stop={initial_stop:.2f}"
                )
        print()
    else:
        print("📂 CURRENT OPEN POSITIONS: none\n")


    if entries_pullback:
        print("📉 ENTRY — Pullback Signals:")
        for ticker, close, rsi2 in entries_pullback:
            print(f"  {ticker}: Close={close:.2f}, RSI2={rsi2:.2f}")
        print()

    if entries_breakout:
        print("📈 ENTRY — Breakout Signals:")
        for ticker, close, vol, vol_sma in entries_breakout:
            print(f"  {ticker}: Close={close:.2f}, Vol={int(vol)}, VolSMA10={int(vol_sma)}")
        print()

    if watch_eligible:
        print("👀 WATCH — Eligible Stock but No Entry Signal:")
        print("  " + ", ".join(watch_eligible))
    else:
        print("No eligible stocks without entries today.")


    # ----- Write debug log to CSV ----- #
    log_file = "debug_signals.csv"
    file_exists = os.path.isfile(log_file)

    with open(log_file, mode="a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "date", "ticker", "close",
            "sma50", "ema21", "rs20", "adx14",
            "eligible",
            "close_above_sma50", "ema21_above_sma50",
            "rs20_positive", "adx14_gt_20",
            "rsi2", "near_ema21", "pullback_signal",
            "high20", "volume", "vol_sma10",
            "price_breaks_high20", "vol_ok", "breakout_signal",
            "category",
        ])

        if not file_exists:
            writer.writeheader()

        for row in debug_rows:
            writer.writerow(row)

    
    # ----- Build Discord summary ----- #
    lines = []
    lines.append("━━━━━━━━━━ 📊 ALGO ALERTS ━━━━━━━━━━")
    lines.append(f"📅 {today}")

    # SPY context
    spy_close = round(spy_ind["Close"], 2)
    lines.append(f"📊 SPY close: {spy_close:.2f}")
    lines.append("")

    # Open positions
    if open_positions_snapshot:
        lines.append("📂 Open positions:")
        for pos in open_positions_snapshot:
            t = pos["ticker"]
            entry_date = pos["entry_date"]
            entry_price = pos["entry_price"]
            last_price = pos["last_price"]
            pnl_abs = pos["pnl_abs"]
            pnl_pct = pos["pnl_pct"]
            entry_type = pos["entry_type"]
            initial_stop = pos["initial_stop"]

            if last_price is not None:
                lines.append(
                    f"• {t}: {entry_type}, entry {entry_price:.2f} on {entry_date}, "
                    f"last {last_price:.2f}, PnL {pnl_abs:.2f} ({pnl_pct:.2f}%), "
                    f"stop {initial_stop:.2f}"
                )
            else:
                lines.append(
                    f"• {t}: {entry_type}, entry {entry_price:.2f} on {entry_date}, "
                    f"stop {initial_stop:.2f}"
                )
    else:
        lines.append("📂 Open positions: none")

    lines.append("")

    # New entry signals
    if entries_pullback:
        pb_tickers = ", ".join([t[0] for t in entries_pullback])
        lines.append(f"📉 Pullback entries: {pb_tickers}")
    else:
        lines.append("📉 Pullback entries: none")

    if entries_breakout:
        bo_tickers = ", ".join([t[0] for t in entries_breakout])
        lines.append(f"📈 Breakout entries: {bo_tickers}")
    else:
        lines.append("📈 Breakout entries: none")

    # CSP tranche + selected orders
    if ENABLE_CSP and csp_plan:
        lines.append("")
        lines.append("💰 CSP Ladder (1/4 weekly, ~30–45 DTE)")
        lines.append(f"• Regime: {csp_plan['regime']} | VIX: {csp_plan['vix_close']:.2f}")
        lines.append(f"• Total CSP cap: ${CSP_MAX_TOTAL_ALLOCATION:,.0f}")
        lines.append(f"  - Reserved (before): ${float(csp_plan['total_reserved']):,.0f}")
        lines.append(f"  - New planned today: ${selected_cash:,.0f}")
        lines.append(f"  - Reserved (after):  ${reserved_after_total:,.0f}")
        lines.append(f"  - Remaining (after): ${CSP_MAX_TOTAL_ALLOCATION - reserved_after_total:,.0f}")

        lines.append(f"• Weekly target: ${CSP_WEEKLY_TARGET_ALLOCATION:,.0f}")
        lines.append(f"  - Reserved (before): ${float(csp_plan['week_reserved']):,.0f}")
        lines.append(f"  - Reserved (after):  ${reserved_after_week:,.0f}")
        lines.append(f"  - Remaining (after): ${CSP_WEEKLY_TARGET_ALLOCATION - reserved_after_week:,.0f}")

        lines.append("")

        if csp_plan["selected"]:
            lines.append("✅ CSP Orders (this week):")
            for idea in csp_plan["selected"]:
                lines.append(
                    f"• {idea['ticker']} {idea['expiry']} (DTE {idea['dte']}): "
                    f"{idea['tier']} | Sell {idea['contracts']}x {idea['strike']:.0f}P "
                    f"mid {idea['mid']:.2f} | est ${idea['est_premium']:.0f} "
                    f"on ${idea['cash_reserved']:.0f} ({idea['yield_pct']*100:.1f}%) | IV {idea['iv']*100:.0f}%"
                )
        else:
            lines.append("✅ CSP Orders (this week): none (budgets full or no qualifying contracts)")

    if ENABLE_CSP:
        if csp_expiry_updates["expired"] or csp_expiry_updates["assigned"]:
            lines.append("")
            lines.append("📌 CSP Outcomes (processed today):")
            for x in csp_expiry_updates["expired"]:
                lines.append(f"• EXPIRED: {x}")
            for x in csp_expiry_updates["assigned"]:
                lines.append(f"• ASSIGNED: {x} (wheel -> consider CC)")

    if ENABLE_CSP:
        lines.append("")
        lines.append("📞 Covered Calls (wheel):")
        if cc_candidates:
            for cc in cc_candidates:
                lines.append(
                    f"• {cc['ticker']} {cc['expiry']}: Sell {cc['contracts']}x {cc['strike']:.0f}C "
                    f"mid {cc['mid']:.2f} | {cc.get('reason','')}"
                )
        else:
            lines.append("• none")

    lines.append("")

    # Watch list
    if watch_eligible:
        lines.append("👀 Watch (eligible, no entry):")
        chunk_size = 8
        for i in range(0, len(watch_eligible), chunk_size):
            chunk = watch_eligible[i:i + chunk_size]
            lines.append("  " + ", ".join(chunk))
    else:
        lines.append("👀 Watch: none")

    lines.append("") 

    # Exit signals
    if exits:
        lines.append("🚪 Exits:")
        for ticker, reason, exit_price, pnl_abs, pnl_pct in exits:
            lines.append(
                f"• {ticker}: {reason} @ {exit_price:.2f} "
                f"(PnL {pnl_abs:.2f} / {pnl_pct:.2f}%)"
            )
    else:
        lines.append("🚪 Exits: none")


    # FINAL MESSAGE
    summary_msg = "\n".join(lines)
    print("\nDiscord summary:\n", summary_msg)

    send_discord(summary_msg)



if __name__ == "__main__":
    run_screener()