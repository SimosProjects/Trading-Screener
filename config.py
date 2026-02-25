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
    INDIVIDUAL: 100_000,
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
    # =========================
    # Tier A – Mega Cap Quality
    # =========================
    "AAPL","MSFT","AMZN","GOOGL","META","NVDA",
    "AVGO","TSM","ASML","ORCL","CRM",
    "V","MA","JPM","GS","MS",
    "LLY","ABBV","UNH","VRTX",
    "WMT","COST","KO","PEP",
    "HD","LOW",
    "MCD","SBUX",
    "ADBE","INTU",

    # =========================
    # Tier B – High Quality Mid Caps
    # =========================
    "ANET","PANW","CRWD",
    "CDNS","SNPS",
    "LULU","CMG",
    "DE","CAT",
    "SHW",
    "ZTS",
    "ELV",
    "ADSK",

    # =========================
    # Tier C – Cyclical but Liquid
    # =========================
    "AMD","MU","INTC","ON","MCHP",
    "XOM","CVX",
    "COP",
    "CARR",
    "TGT",
    "ABNB","UBER",
    "NFLX",

    # =========================
    # Select Growth
    # =========================
    "PLTR","SHOP","NET","MDB","SNOW","BROS","CELH",
]))

# CSP Universe
CSP_STOCKS: List[str] = list(dict.fromkeys(
    STOCKS + [
        # =========================
        # Core ETFs (wheel stabilizers)
        # =========================
        "SPLG","SPY","QQQ","TQQQ",
        "SCHD","JEPI",
        "XLU","XLF","XLE","XLV",
        "SMH",

        # =========================
        # Additional Defensive
        # =========================
        "PFE","MRK","BMY",
        "PG","CL",
        "GIS","KHC",

        # =========================
        # Financial breadth
        # =========================
        "BAC","C","AXP",
        "BLK",

        # =========================
        # Industrial breadth
        # =========================
        "ETN","PH",
        "EMR",

        # =========================
        # Retail / consumer
        # =========================
        "TJX",
        "ROST",
    ]
))

CSP_DEFENSIVE_STOCKS: List[str] = [
    # Broad ETFs
    "SPY","SPLG","SCHD","JEPI","XLU","XLF","XLV",

    # Consumer staples
    "KO","PEP","PG","CL","GIS","KHC",

    # Healthcare large cap
    "ABBV","MRK","UNH","LLY",

    # Financial large cap
    "JPM","BAC",

    # Energy majors
    "XOM","CVX",
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

# ---- Take-profit / early close ----
# Close a CSP when current mid-price <= original premium * this fraction.
# 0.50 = close at 50% profit (the classic "half-profit" rule).
CSP_TAKE_PROFIT_PCT = 0.50

# Skip the take-profit close if the bid/ask spread is wider than this fraction
# of mid.  Wide spreads mean the quote is stale or illiquid — better to hold
# than close at an unknown price.  0.50 = spread must be <= 50% of mid.
CSP_TP_MAX_SPREAD_PCT = 0.50

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

# ============================================================
# Covered Call (CC) policy
# ============================================================

# DTE window for CC selection (tighter than CSP — want faster theta decay)
CC_TARGET_DTE_MIN = 14
CC_TARGET_DTE_MAX = 30

# --- ATR-scaled strike selection ---
# Strike target = current_price + (atr_mult × ATR_14).
# Using ATR instead of a fixed % naturally scales to each stock's actual
# daily range — a volatile stock gets more room to recover than a stable one.
#
# Multiplier tiers (based on how far current price is below net cost basis):
#   At/above basis        → CC_ATR_MULT_NORMAL  (modest OTM, normal income)
#   0–10% underwater      → CC_ATR_MULT_MILD    (more room, moderate premium)
#   10–25% underwater     → CC_ATR_MULT_DEEP    (protect recovery potential)
#   >25% underwater       → CC_ATR_MULT_SEVERE  (distress — never cap a big bounce)
#
# Example: stock at $40, ATR=$1.50
#   NORMAL  (1.0×): target $41.50 — ~3.75% OTM
#   MILD    (1.5×): target $42.25 — ~5.6% OTM
#   DEEP    (2.0×): target $43.00 — ~7.5% OTM
#   SEVERE  (2.5×): target $43.75 — ~9.4% OTM
CC_ATR_MULT_NORMAL  = 1.0   # at/above basis
CC_ATR_MULT_MILD    = 1.5   # 0–10% underwater
CC_ATR_MULT_DEEP    = 2.0   # 10–25% underwater
CC_ATR_MULT_SEVERE  = 2.5   # >25% underwater

# Underwater tier thresholds (as positive fractions of basis)
CC_UNDERWATER_MILD_PCT   = 0.10   # 0–10% below basis → MILD
CC_UNDERWATER_DEEP_PCT   = 0.25   # 10–25% below basis → DEEP
# anything worse than 25% → SEVERE

# Hard floor: don't sell a CC whose (chain-rounded) strike is more than this %
# below the *current price*.  Anchoring to current price rather than original
# basis is intentional — when the stock is already underwater the basis-relative
# floor blocks every sensible strike (e.g., stock at $40 vs $50 basis means
# floor at $47.50, which is ITM — useless).  The goal here is simply to avoid
# selling a CC so close to current price that it provides no real OTM cushion.
# 2% means: don't sell a strike below $39.20 when the stock is at $40.
CC_STRIKE_FLOOR_BELOW_CURRENT_PCT = 0.02  # always at least 2% OTM from current price

# Minimum bid to bother with a CC (too thin a market = bad fill risk).
CC_MIN_BID = 0.05

# Roll signal threshold: flag an open CC as a roll candidate when
# current_price / cc_strike >= this value (i.e., within ~3% of being called away).
# Informational only — no automated roll execution.
CC_ROLL_SIGNAL_THRESHOLD = 0.97