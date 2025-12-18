# config.py
from typing import List

# ---- Discord Webhook ---- #
WEBHOOK_URL = "https://discord.com/api/webhooks/1445480294500270081/pBeMhblXLTybjfht9YPOuC8YshLxXD52BKb-IL7TR9YMt1i4fcqteMcbG9sqrzRYnlr_"

# ---- Files ---- #
POSITIONS_FILE = "open_positions.csv"
TRADES_LOG_FILE = "closed_trades.csv"

CSP_LEDGER_FILE = "csp_ledger.csv"
CSP_POSITIONS_FILE = "csp_positions.csv"
CC_POSITIONS_FILE = "cc_positions.csv"

# Institutional wheel tracking
WHEEL_EVENTS_FILE = "wheel_events.csv"
WHEEL_LOTS_FILE = "wheel_lots.csv"
WHEEL_MONTHLY_DIR = "wheel_monthly"

# ---- Account / Allocation ---- #
ACCOUNT_SIZE = 125_000
WHEEL_CAP_PCT = 0.8
WHEEL_CAP = int(ACCOUNT_SIZE * WHEEL_CAP_PCT)
WHEEL_WEEKLY_TARGET = WHEEL_CAP / 5.0

# ---- Universe ---- #
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
    "PLTR","SHOP","SNOW","MDB","NET","ZS","BILL"
]

# CSP universe (STOCKS + liquid ETFs + a few high-IV names)
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
    "CELH","BROS"
    ]
))

"""
STOCKS: List[str] = [
    "AAPL","NVDA","MSFT","AMZN","META","GOOGL","TSLA","AMD",
    "AVGO","WMT","V","NFLX","MU","CELH","BROS","ACHR","TSM",
    "RKLB","GTLB","JOBY","SOFI","QQQ","INTC","DKNG",
    "ASTS","APLD","LLY","JPM","PLTR","BAC","ASML","ARM","MCHP",
    "MS","AXP","GS","IONQ","TREE","HIMS","SHOP","LSCC","ON","SMCI",
    "CRWD","NET","SNOW","ZS","PANW","MDB","PAYC","BILL","AFRM",
    "ADYEY","GLBE","VRTX","REGN","TMDX","EXAS","CAT","DE","ANET","ENPH",
    "FSLR","RUN","CARR","MOD","F","LULU","CMG","TGT","COST","ABNB","UBER"
]

# CSP universe (STOCKS + liquid ETFs + a few high-IV names)
CSP_STOCKS: List[str] = list(dict.fromkeys(
    STOCKS + [
        "IWM", "XLF", "XLK", "SMH", "XLE",
        "XOM", "CVX", "KO", "PEP", "ABBV", "UNH",
        "HD", "LOW", "DIS", "CMCSA", "BETA",
        "COIN", "BBAI", "SOUN", "QUBT", "CLSK"
    ]
))"""

# ---- Market data ---- #
DATA_PERIOD = "1y"
DATA_INTERVAL = "1d"

# ---- CSP enable ---- #
ENABLE_CSP = True

# ============================================================
# CSP / CC configuration (institutional-ish defaults)
# ============================================================

# ---- CSV schemas ----
# Keep these stable; changing columns will break existing CSVs.

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

# Optional IV sanity check (set to 0.0/None to disable)
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
