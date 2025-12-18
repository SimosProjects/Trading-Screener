# config.py
from __future__ import annotations

from typing import Dict, List

# ============================================================
# Discord
# ============================================================
# Tip: keep this OUT of git in real life (env var / local override).
WEBHOOK_URL = "https://discord.com/api/webhooks/1445480294500270081/pBeMhblXLTybjfht9YPOuC8YshLxXD52BKb-IL7TR9YMt1i4fcqteMcbG9sqrzRYnlr_"


# ============================================================
# Files
# ============================================================
POSITIONS_FILE = "open_positions.csv"
TRADES_LOG_FILE = "closed_trades.csv"

CSP_LEDGER_FILE = "csp_ledger.csv"
CSP_POSITIONS_FILE = "csp_positions.csv"
CC_POSITIONS_FILE = "cc_positions.csv"

# Wheel bookkeeping
WHEEL_EVENTS_FILE = "wheel_events.csv"
WHEEL_LOTS_FILE = "wheel_lots.csv"
WHEEL_MONTHLY_DIR = "wheel_monthly"

# Retirement tracking (stock-only)
RETIREMENT_POSITIONS_FILE = "retirement_positions.csv"

# ============================================================
# Accounts / allocation
# ============================================================
ACCOUNT_SIZES: Dict[str, float] = {
    "INDIVIDUAL": 125_000,
    "IRA": 100_000,
    "ROTH": 100_000,
}

# Which account runs the wheel (CSP/CC) logic
WHEEL_ACCOUNT = "INDIVIDUAL"

# Wheel cap is applied ONLY to the wheel account
WHEEL_CAP_PCT = 0.80
WHEEL_CAP = float(ACCOUNT_SIZES[WHEEL_ACCOUNT]) * float(WHEEL_CAP_PCT)

# “Weekly target” is a pacing number for new CSP collateral, not a hard rule.
WHEEL_WEEKLY_TARGET = WHEEL_CAP / 5.0

# Retirement rule: if a position is down >= this %, flag as "breakeven-only" target (sell at entry).
RETIREMENT_BREAKEVEN_ONLY_DD_PCT = 0.10

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

    # Tactical Growth (still quality filters in code)
    "AMD","MU","INTC","ON","LSCC","MCHP","SMCI",
    "NFLX","LULU","CMG","TGT","ABNB","UBER",
    "PLTR","SHOP","SNOW","MDB","NET","ZS","BILL",

    # Your higher-beta names you trade
    "CELH","BROS","ACHR","RKLB","GTLB","JOBY","SOFI","DKNG",
    "IONQ","TREE","HIMS","AFRM","QUBT","SOUN","BBAI","CLSK","ASTS","APLD","EHTH",
]

# CSP universe:
# - MUST respect CSP_MAX_CASH_PER_TRADE cap, so underlyings above ~$65 are auto-excluded in strategies.py
CSP_STOCKS: List[str] = list(dict.fromkeys(
    STOCKS + [
        "SOFI","RKLB","JOBY","BBAI","ASTS","ACHR","CLSK",
        "DKNG","AFRM","HIMS","CELH","BROS",
        "IONQ","TREE","QUBT","SOUN","EHTH",
    ]
))

# ============================================================
# Market data
# ============================================================
DATA_PERIOD = "1y"
DATA_INTERVAL = "1d"

# ============================================================
# CSP enable
# ============================================================
ENABLE_CSP = True

# ============================================================
# CSP / CC configuration
# ============================================================

# ---- CSV schemas (stable) ----
CSP_POSITIONS_COLUMNS = [
    "id",
    "open_date",
    "week_id",
    "ticker",
    "expiry",
    "dte_open",
    "strike",
    "contracts",
    "credit_mid",
    "cash_reserved",
    "est_premium",
    "tier",
    "status",
    "underlying_last",
    "strike_diff",
    "strike_diff_pct",
    "dte_remaining",
    "itm_otm",
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
    "credit_mid",
    "status",  # OPEN / EXPIRED / CALLED_AWAY
    "close_date",
    "close_type",
    "notes",
]

# ---- DTE window ----
CSP_TARGET_DTE_MIN = 25
CSP_TARGET_DTE_MAX = 45

# ---- Risk / sizing ----
# Per-trade cash collateral cap (strike * 100 * contracts)
CSP_MAX_CASH_PER_TRADE = 6_500

# ---- Liquidity filters ----
CSP_MIN_OI = 100
CSP_MIN_VOLUME = 10
CSP_MIN_BID = 0.10

# Optional IV sanity check (set to 0.0 to disable)
CSP_MIN_IV = 0.30

# ---- Strike selection ----
# Supported by strategies.py: "ema21_atr" or fallback percentage.
CSP_STRIKE_MODE = "ema21_atr"

# ---- Premium / yield tiers ----
# Premium numbers are per position (already contracts-adjusted).
CSP_MIN_PREMIUM_CONSERVATIVE = 200
CSP_MIN_PREMIUM_BALANCED = 300
CSP_MIN_PREMIUM_AGGRESSIVE = 400

# Yield is premium / cash_reserved (decimal).
CSP_MIN_YIELD_CONSERVATIVE = 0.03
CSP_MIN_YIELD_BALANCED = 0.04
CSP_MIN_YIELD_AGGRESSIVE = 0.05

# ---- Tier caps ----
CSP_MAX_AGGRESSIVE_TOTAL = 2
CSP_MAX_AGGRESSIVE_PER_WEEK = 1
