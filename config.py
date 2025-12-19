# config.py
from __future__ import annotations
from typing import Dict, List

# ---- Discord Webhook ---- #
# Keep OUT of git (use env var in the future if you want)
WEBHOOK_URL = ""

# ---- Files ---- #
# Stock swing / tactical tracking (paper)
STOCK_POSITIONS_FILE = "stock_positions.csv"
STOCK_TRADES_FILE = "stock_trades.csv"
STOCK_FILLS_FILE = "stock_fills.csv"
STOCK_MONTHLY_DIR = "stock_monthly"

# Legacy single-account swing tracking is replaced by the files above
# (kept only if you still want them elsewhere)
POSITIONS_FILE = "open_positions.csv"
TRADES_LOG_FILE = "closed_trades.csv"

# Wheel / options tracking (paper)
CSP_LEDGER_FILE = "csp_ledger.csv"
CSP_POSITIONS_FILE = "csp_positions.csv"
CC_POSITIONS_FILE = "cc_positions.csv"

# Institutional-ish wheel tracking
WHEEL_EVENTS_FILE = "wheel_events.csv"
WHEEL_LOTS_FILE = "wheel_lots.csv"
WHEEL_MONTHLY_DIR = "wheel_monthly"

# Retirement holdings tracking (stock-only “inventory”, separate from swing trades)
RETIREMENT_POSITIONS_FILE = "retirement_positions.csv"

# ============================================================
# Stock scale-in (average-down) rules (paper)
# ============================================================
STOCK_MAX_ADDS_PER_POSITION = 1
STOCK_ADD_COOLDOWN_DAYS = 5
STOCK_ADD_MIN_DRAWdown_PCT = 0.03   # require position to be down at least this much vs avg entry to consider an ADD
STOCK_ADD_NEAR_EMA21_ATR = 0.50     # must be within +/- this * ATR of EMA21
STOCK_ADD_RSI14_MIN = 45

# ============================================================
# Accounts / Allocation
# ============================================================

# Named accounts used everywhere
INDIVIDUAL = "INDIVIDUAL"
IRA = "IRA"
ROTH = "ROTH"

ACCOUNT_SIZES: Dict[str, int] = {
    INDIVIDUAL: 125_000,
    IRA: 100_000,
    ROTH: 100_000,
}

# Wheel allocation is ONLY for INDIVIDUAL account
WHEEL_ACCOUNT = INDIVIDUAL
WHEEL_CAP_PCT = 0.80
WHEEL_CAP = int(ACCOUNT_SIZES[WHEEL_ACCOUNT] * WHEEL_CAP_PCT)
WHEEL_WEEKLY_TARGET = WHEEL_CAP / 5.0

# INDIVIDUAL stock (non-wheel) cap is the remaining % (e.g., 20%)
INDIVIDUAL_STOCK_CAP_PCT = 1.0 - WHEEL_CAP_PCT
INDIVIDUAL_STOCK_CAP = int(ACCOUNT_SIZES[INDIVIDUAL] * INDIVIDUAL_STOCK_CAP_PCT)

# Retirement accounts can be more aggressive but still capped by account size
RETIREMENT_MAX_EQUITY_UTIL_PCT = 0.98  # keep a little cash buffer

# If a retirement holding is down >= 10%, only allow selling at breakeven (entry)
RETIREMENT_BREAKEVEN_ONLY_DD_PCT = 0.10

# ============================================================
# Stock swing trade rules (paper execution)
# ============================================================

# “Run after close” means entries are detected using EOD data.
# We build entry plans meant to be still valid the next day.
STOCK_REQUIRE_NEXTDAY_VALIDATION = True

# If False, a ticker can only be OPEN in ONE account at a time (INDIVIDUAL/IRA/ROTH)
ALLOW_MULTI_ACCOUNT_SAME_TICKER = False

# Minimum position market value (helps avoid tiny positions when risk_per_share is large)
STOCK_MIN_POSITION_VALUE_INDIVIDUAL = 1_500
STOCK_MIN_POSITION_VALUE_RETIREMENT = 3_000

# --- Stock sizing caps (swing stocks) ---
STOCK_MAX_POSITION_PCT_INDIVIDUAL = 0.15   # 15% of account per stock
STOCK_MAX_POSITION_PCT_RETIREMENT = 0.15 

# Gating by market regime
# - INDIVIDUAL swing trades are strict
# - IRA/ROTH tactical trades are slightly less strict (still avoid broken markets)
STOCK_GATE_INDIVIDUAL = "STRICT"   # STRICT = SPY>200 & SPY>50 & SPY>21 & QQQ>50 & VIX<25
STOCK_GATE_RETIREMENT = "SOFT"     # SOFT   = SPY>200 & VIX<25

# Risk / sizing
# Risk is defined as (entry - stop) * shares.
# We size shares to keep risk per trade within these caps.
STOCK_RISK_PCT_INDIVIDUAL = 0.050   # 5.0% of INDIVIDUAL_STOCK_CAP per trade
STOCK_RISK_PCT_RETIREMENT = 0.050   # 5.0% of account per trade

# Targets / exits
STOCK_TARGET_R_MULTIPLE = 2.0           # take-profit at ~2R
STOCK_BREAKEVEN_AFTER_R = 1.0           # optional: move stop to breakeven after +1R
STOCK_USE_BREAKEVEN_TRAIL = True

# Stop building blocks
STOCK_STOP_ATR_PULLBACK = 1.0  # stop = EMA21 - ATR*X for pullbacks
STOCK_STOP_ATR_BREAKOUT = 1.2  # stop = breakout_level - ATR*X

# ============================================================
# Universe
# ============================================================

STOCKS: List[str] = [
    # Mega-cap quality
    "AAPL","MSFT","AMZN","GOOGL","META","NVDA","AVGO","TSM","ASML",

    # Payments / financial quality
    "V","MA","JPM","BAC","MS","GS","AXP",

    # Healthcare (defensive growth)
    "LLY","ABBV","UNH","VRTX","REGN",

    # Consumer staples / resilient demand
    "WMT","COST","KO","PEP",

    # Industrials / infrastructure
    "CAT","DE","CARR",

    # Profitable tech infrastructure
    "ANET","CRWD","PANW",

    # Tactical Growth
    "AMD","MU","INTC","ON","LSCC","MCHP","SMCI",
    "NFLX","LULU","CMG","TGT","ABNB","UBER",
    "PLTR","SHOP","SNOW","MDB","NET","ZS","BILL",
]

# CSP universe (kept focused; price cap is enforced in strategies.evaluate_csp_candidate)
CSP_STOCKS: List[str] = list(dict.fromkeys(
    STOCKS + [
        # Financials / payments
        "BAC","C","SOFI","AXP",

        # Consumer / retail
        "TGT","WMT","UBER","CMCSA",

        # Semiconductors (selectively)
        "INTC","MU","ON","LSCC","MCHP",

        # Industrials / autos
        "F","CARR",

        # Healthcare
        "ABBV","UNH","VRTX","EXAS",

        # Energy
        "XOM","CVX",

        # Media / entertainment
        "DIS",

        # Tactical CSPs
        "PLTR","SHOP","NET","SNOW",
        "DKNG","AFRM","HIMS",
        "CELH","BROS",

        # High-IV under $65 candidates (price cap is enforced anyway)
        "BBAI","SOUN","QUBT","CLSK","RKLB","JOBY","ASTS","ACHR",
    ]
))

# ---- Market data ---- #
DATA_PERIOD = "1y"
DATA_INTERVAL = "1d"

# ---- CSP enable ---- #
ENABLE_CSP = True

# ============================================================
# CSP / CC configuration
# ============================================================

CSP_POSITIONS_COLUMNS = [
    "id",
    "open_date",
    "week_id",
    "ticker",
    "expiry",
    "strike",
    "contracts",
    "premium",
    "cash_reserved",
    "tier",
    "status",
    "close_date",
    "close_type",
    "underlying_close_at_expiry",
    "shares_if_assigned",
    "assignment_cost_basis",
    "notes",
]

CC_POSITIONS_COLUMNS = [
    "id",
    "open_date",
    "ticker",
    "expiry",
    "strike",
    "contracts",
    "premium",
    "status",
    "close_date",
    "close_type",
    "notes",
]

# ---- DTE window ----
CSP_TARGET_DTE_MIN = 25
CSP_TARGET_DTE_MAX = 45

# ---- Risk / sizing ----
CSP_MAX_CASH_PER_TRADE = 6_500  # => $65/share max if 1 contract

# ---- Liquidity filters ----
CSP_MIN_OI = 100
CSP_MIN_VOLUME = 10
CSP_MIN_BID = 0.10

# Optional IV sanity check (set to 0.0/None to disable)
CSP_MIN_IV = 0.30

# ---- Strike selection ----
CSP_STRIKE_MODE = "ema21_atr"

# ---- Premium / yield tiers ----
CSP_MIN_PREMIUM_CONSERVATIVE = 200
CSP_MIN_PREMIUM_BALANCED = 300
CSP_MIN_PREMIUM_AGGRESSIVE = 400

CSP_MIN_YIELD_CONSERVATIVE = 0.03
CSP_MIN_YIELD_BALANCED = 0.04
CSP_MIN_YIELD_AGGRESSIVE = 0.05

# ---- Tier caps ----
CSP_MAX_AGGRESSIVE_TOTAL = 2
CSP_MAX_AGGRESSIVE_PER_WEEK = 1
