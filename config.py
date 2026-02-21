"""config.py

Project configuration.

"""

from __future__ import annotations
from typing import Dict, List

# ---- Discord Webhook ---- #
# Keep OUT of git
WEBHOOK_URL = ""

# ---- Files ---- #
# Stock swing / tactical tracking
STOCK_POSITIONS_FILE = "stock_positions.csv"
STOCK_TRADES_FILE = "stock_trades.csv"
STOCK_FILLS_FILE = "stock_fills.csv"
STOCK_MONTHLY_DIR = "stock_monthly"

# Wheel / options tracking
CSP_LEDGER_FILE = "csp_ledger.csv"
CSP_POSITIONS_FILE = "csp_positions.csv"
CC_POSITIONS_FILE = "cc_positions.csv"

# Wheel tracking
WHEEL_EVENTS_FILE = "wheel_events.csv"
WHEEL_LOTS_FILE = "wheel_lots.csv"
WHEEL_MONTHLY_DIR = "wheel_monthly"

# Retirement holdings
RETIREMENT_POSITIONS_FILE = "retirement_positions.csv"

# ============================================================
# Accounts / Allocation
# ============================================================

# Named accounts
INDIVIDUAL = "INDIVIDUAL"
IRA = "IRA"
ROTH = "ROTH"

ACCOUNT_SIZES: Dict[str, int] = {
    INDIVIDUAL: 125_000,
    IRA: 150_000,
    ROTH: 150_000,
}

# Wheel allocation is ONLY for INDIVIDUAL account
WHEEL_ACCOUNT = INDIVIDUAL
WHEEL_CAP_PCT = 0.80
WHEEL_CAP = int(ACCOUNT_SIZES[WHEEL_ACCOUNT] * WHEEL_CAP_PCT)
WHEEL_WEEKLY_TARGET = WHEEL_CAP / 5.0


# Wheel allocation per account (INDIVIDUAL + retirement).
# These drive per-account exposure limits for CSP/CC activity.
WHEEL_CAPS: Dict[str, int] = {
    INDIVIDUAL: int(ACCOUNT_SIZES[INDIVIDUAL] * WHEEL_CAP_PCT),
    IRA: int(ACCOUNT_SIZES[IRA] * WHEEL_CAP_PCT),
    ROTH: int(ACCOUNT_SIZES[ROTH] * WHEEL_CAP_PCT),
}

WHEEL_WEEKLY_TARGETS: Dict[str, float] = {
    acct: cap / 5.0 for acct, cap in WHEEL_CAPS.items()
}


# INDIVIDUAL stock (non-wheel) cap is the remaining % (e.g., 20%)
INDIVIDUAL_STOCK_CAP_PCT = 1.0 - WHEEL_CAP_PCT
INDIVIDUAL_STOCK_CAP = int(ACCOUNT_SIZES[INDIVIDUAL] * INDIVIDUAL_STOCK_CAP_PCT)

# Retirement accounts can be more aggressive but still capped by account size
RETIREMENT_MAX_EQUITY_UTIL_PCT = 0.98  # cash buffer

# If a retirement holding is down >= 10%, only allow selling at breakeven (entry)
RETIREMENT_BREAKEVEN_ONLY_DD_PCT = 0.10

# ============================================================
# Stock swing trade rules (paper execution)
# ============================================================

# “Run after close” means entries are detected using EOD data.
STOCK_REQUIRE_NEXTDAY_VALIDATION = True

# --- Stock sizing caps (swing stocks) ---
STOCK_MAX_POSITION_PCT_INDIVIDUAL = 0.15   # 15% of account per stock
STOCK_MAX_POSITION_PCT_RETIREMENT = 0.15 

# Risk / sizing
STOCK_RISK_PCT_INDIVIDUAL = 0.050   # 5.0% of INDIVIDUAL_STOCK_CAP per trade
STOCK_RISK_PCT_RETIREMENT = 0.050   # 5.0% of account per trade

# Targets / exits
STOCK_TARGET_R_MULTIPLE = 2.0           # take-profit at ~2R
STOCK_BREAKEVEN_AFTER_R = 1.0           # move stop to breakeven after +1R
STOCK_USE_BREAKEVEN_TRAIL = True

# Stop building blocks
STOCK_STOP_ATR_PULLBACK = 1.0  # stop = EMA21 - ATR*X for pullbacks
STOCK_STOP_ATR_BREAKOUT = 1.2  # stop = breakout_level - ATR*X

# ============================================================
# Stock Universe
# ============================================================

STOCKS: List[str] = list(dict.fromkeys([
    # Mega-cap quality
    "AAPL","MSFT","AMZN","GOOGL","META","NVDA","AVGO","TSM","ASML","INTC",

    # Payments / financial quality
    "V","MA","JPM","BAC","MS","GS","AXP","SOFI",

    # Healthcare (defensive growth)
    "LLY","ABBV","UNH","VRTX","REGN",

    # Consumer staples / resilient demand
    "WMT","COST","KO","PEP","CELH","BROS",

    # Industrials / infrastructure
    "CAT","DE","CARR",

    # Profitable tech infrastructure
    "ANET","CRWD","PANW","MU",

    # High risk growth
    "RKLB", "JOBY", "ACHR","SOUN", "BBAI","ASTS",

    # Tactical Growth
    "AMD","MU","INTC","ON","LSCC","MCHP","SMCI",
    "NFLX","LULU","CMG","TGT","ABNB","UBER",
    "PLTR","SHOP","SNOW","MDB","NET","ZS","BILL","PL",
]))

# CSP Universe
CSP_STOCKS: List[str] = list(dict.fromkeys(
    STOCKS + [
        # ETFs / lower-beta
        "SPLG","SCHD","JEPI","XLU","XLF","EEM",

        # Defensive single names
        "KO","PFE","VZ","T","GIS","KHC","BAC","OXY",

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

        # High-IV candidates
        "BBAI","SOUN","QUBT","CLSK","RKLB","JOBY","ASTS","ACHR","PL",
    ]
))


# CSP tickers to allow even in "risk-off" market regime (high VIX / SPY below 200SMA).
# These should be liquid, lower-beta names and/or broad ETFs.
CSP_DEFENSIVE_STOCKS: List[str] = [
    # ETFs
    "SPLG","SCHD","JEPI","XLU","XLF","EEM",
    # Large / defensive-ish single names
    "KO","PFE","VZ","T","GIS","KHC","BAC",
    # Energy/commodity
    "OXY",
]

# Market-regime controls for CSP scan
CSP_RISK_OFF_VIX = 25.0  # VIX > this => "RISK_OFF" (defensive-only + farther OTM)
CSP_RISK_OFF_MIN_OTM_PCT_DEFENSIVE = 0.10  # at least 10% OTM when risk-off (defensive)
CSP_RISK_OFF_MIN_OTM_PCT_RISKY = 0.15      # at least 15% OTM when risk-off
CSP_NORMAL_MIN_OTM_PCT = 0.06              # baseline cushion

# Strike base for puts
CSP_STRIKE_BASE_NORMAL = "EMA_21"          # fast, but fine when trend is healthy
CSP_STRIKE_BASE_RISK_OFF = "SMA_50"        # slower, avoids chasing price down

# ---- Market data ---- #
DATA_PERIOD = "1y"
DATA_INTERVAL = "1d"

# ---- CSP enabled flag ---- #
ENABLE_CSP = True

# ============================================================
# CSP / CC configuration
# ============================================================

CSP_POSITIONS_COLUMNS = [
    "id",
    "open_date",
    "week_id",
    "account",
    "ticker",
    "expiry",
    "dte_open",
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
    "account",
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
CSP_MAX_CASH_PER_TRADE = 8_000  # => $80/share max if 1 contract

# ---- Liquidity filters ----
CSP_MIN_OI = 100
CSP_MIN_VOLUME = 10
CSP_MIN_BID = 0.10

# IV sanity check (set to 0.0/None to disable)
CSP_MIN_IV = 0.20  # IV filter is redundant with premium/yield; lower IV does not increase risk

# ---- Strike selection ----
CSP_STRIKE_MODE = "ema21_atr"

# Try a small set of ATR distances to find a liquid strike.
# Higher = farther OTM (lower risk, less premium).
CSP_ATR_MULTS = [1.50, 1.25, 1.00]  # safer strikes (farther OTM)

# ---- Premium / yield tiers ----
CSP_MIN_PREMIUM_CONSERVATIVE = 100
CSP_MIN_PREMIUM_BALANCED = 175
CSP_MIN_PREMIUM_AGGRESSIVE = 250

CSP_MIN_YIELD_CONSERVATIVE = 0.010
CSP_MIN_YIELD_BALANCED = 0.015
CSP_MIN_YIELD_AGGRESSIVE = 0.020

# ---- Tier caps ----
CSP_MAX_AGGRESSIVE_TOTAL = 4
CSP_MAX_AGGRESSIVE_PER_WEEK = 2