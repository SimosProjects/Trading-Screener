"""config.py

Project configuration.

"""

from __future__ import annotations
import os
from typing import Dict, List

# ============================================================
# Market Regime Thresholds
# ============================================================
# Five regimes — ordered from most to least aggressive:
#
#   MOMENTUM    : VIX < 16 AND SPY above all 3 MAs AND at/near 52W high
#                 High-octane bull — loosen everything, chase breakouts
#   STRONG_BULL : VIX < 18 AND SPY above all 3 MAs
#                 Low vol confirmed uptrend — standard aggressive mode
#   BULL        : VIX < 22 AND SPY above 200 + 50
#                 Normal healthy market — standard parameters
#   NEUTRAL     : VIX < 25 AND SPY above 200
#                 Elevated uncertainty — tighten slightly
#   RISK_OFF    : VIX >= 25 OR SPY below 200 SMA
#                 Defensive mode — protect capital first
REGIME_VIX_MOMENTUM    = 16.0   # VIX below this + SPY near highs = MOMENTUM
REGIME_VIX_STRONG_BULL = 18.0   # VIX below this = calm/recovering market
REGIME_VIX_BULL        = 22.0   # VIX below this = normal healthy market
REGIME_VIX_NEUTRAL     = 25.0   # VIX below this = elevated but manageable

# ---- Discord Webhook ---- #
WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

# ---- Files ---- #
STOCK_POSITIONS_FILE = "stock_positions.csv"
STOCK_TRADES_FILE    = "stock_trades.csv"
STOCK_FILLS_FILE     = "stock_fills.csv"
STOCK_MONTHLY_DIR    = "stock_monthly"

CSP_LEDGER_FILE   = "csp_ledger.csv"
CSP_POSITIONS_FILE = "csp_positions.csv"
CC_POSITIONS_FILE  = "cc_positions.csv"

WHEEL_EVENTS_FILE = "wheel_events.csv"
WHEEL_LOTS_FILE   = "wheel_lots.csv"
WHEEL_MONTHLY_DIR = "wheel_monthly"

RETIREMENT_POSITIONS_FILE = "retirement_positions.csv"

# ============================================================
# Accounts / Allocation
# ============================================================

INDIVIDUAL = "INDIVIDUAL"
IRA        = "IRA"
ROTH       = "ROTH"

IRA_ACCOUNTS = (IRA, ROTH)

ACCOUNT_SIZES: Dict[str, int] = {
    INDIVIDUAL: 120_000,
    IRA:        150_000,
    ROTH:       150_000,
}

# ---- Per-account wheel configuration ----
# INDIVIDUAL gets 10% margin; IRA/ROTH are cash-secured only.
# Stock swing trades get 30% of INDIVIDUAL (up from 20%) — enough capital to matter.
INDIVIDUAL_MARGIN_PCT = 0.10

WHEEL_ACCOUNT_CONFIG: Dict[str, dict] = {
    INDIVIDUAL: {
        "buying_power":   ACCOUNT_SIZES[INDIVIDUAL] * (1.0 + INDIVIDUAL_MARGIN_PCT),
        "cap_pct":        0.70,   # 70% to wheel — was 80%; frees 30% for swing trades
        "weekly_divisor": 5.0,
    },
    IRA: {
        "buying_power":   float(ACCOUNT_SIZES[IRA]),
        "cap_pct":        0.80,
        "weekly_divisor": 5.0,
    },
    ROTH: {
        "buying_power":   float(ACCOUNT_SIZES[ROTH]),
        "cap_pct":        0.80,
        "weekly_divisor": 5.0,
    },
}

WHEEL_CAPS: Dict[str, int] = {
    acct: int(cfg["buying_power"] * cfg["cap_pct"])
    for acct, cfg in WHEEL_ACCOUNT_CONFIG.items()
}
WHEEL_WEEKLY_TARGETS: Dict[str, float] = {
    acct: cfg["buying_power"] * cfg["cap_pct"] / cfg["weekly_divisor"]
    for acct, cfg in WHEEL_ACCOUNT_CONFIG.items()
}

WHEEL_CAP_PCT           = WHEEL_ACCOUNT_CONFIG[INDIVIDUAL]["cap_pct"]
INDIVIDUAL_STOCK_CAP_PCT = 1.0 - WHEEL_CAP_PCT
INDIVIDUAL_STOCK_CAP    = int(ACCOUNT_SIZES[INDIVIDUAL] * INDIVIDUAL_STOCK_CAP_PCT)
# $120K * 30% = $36K for swing trades — meaningful position sizes

RETIREMENT_STOCK_CAP_PCT = 1.0 - WHEEL_ACCOUNT_CONFIG[IRA]["cap_pct"]
RETIREMENT_STOCK_CAPS: Dict[str, int] = {
    IRA:  int(ACCOUNT_SIZES[IRA]  * RETIREMENT_STOCK_CAP_PCT),
    ROTH: int(ACCOUNT_SIZES[ROTH] * RETIREMENT_STOCK_CAP_PCT),
}

RETIREMENT_MAX_EQUITY_UTIL_PCT  = 0.98
RETIREMENT_BREAKEVEN_ONLY_DD_PCT = 0.10
RETIREMENT_STOP_LOSS_PCT         = 0.35

# ---- Retirement buy-and-hold ----
RETIREMENT_POSITION_SIZE_PCT   = 0.50
RETIREMENT_MAX_STOCK_POSITIONS = 2

RETIREMENT_STOCKS: List[str] = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "NVDA", "AVGO", "ORCL", "ADBE", "INTU",
    "V", "MA",
    "LLY", "ABBV", "UNH", "VRTX",
    "WMT", "COST", "KO", "PEP", "HD",
    "JPM", "GS",
    "MCD", "SBUX",
]

RETIREMENT_DIVERSIFY_SECTORS = True

RETIREMENT_STOCK_YIELDS: Dict[str, float] = {
    "AAPL": 0.005, "MSFT": 0.007, "GOOGL": 0.005, "AMZN": 0.000,
    "META": 0.004, "NVDA": 0.001, "AVGO": 0.013, "ORCL": 0.013,
    "ADBE": 0.000, "INTU": 0.007,
    "V":    0.008, "MA":   0.006,
    "LLY":  0.006, "ABBV": 0.035, "UNH":  0.015, "VRTX": 0.000,
    "WMT":  0.010, "COST": 0.006, "KO":   0.030, "PEP":  0.030,
    "HD":   0.023, "JPM":  0.022, "GS":   0.022,
    "MCD":  0.022, "SBUX": 0.030,
}

# ============================================================
# Stock swing trade rules -- INDIVIDUAL account only
# ============================================================

# EOD-based signals; entries filled at next open.
STOCK_REQUIRE_NEXTDAY_VALIDATION = True

# ── Position sizing ──────────────────────────────────────────────────────────
# No fixed risk %; sizing is purely proportional to account slice and regime.
# Max position = INDIVIDUAL_STOCK_CAP * STOCK_MAX_POSITION_PCT.
# You decide whether a name is too expensive — no artificial price cap.
#
# With $36K stock slice and 25% max = $9K max per swing trade.
# At $200/sh that's 45 shares — meaningful exposure.
STOCK_MAX_POSITION_PCT: Dict[str, float] = {
    "MOMENTUM":    0.30,   # 30% of slice = ~$10.8K — high-octane market, size up
    "STRONG_BULL": 0.25,   # 25% = ~$9K
    "BULL":        0.20,   # 20% = ~$7.2K
    "NEUTRAL":     0.15,   # 15% = ~$5.4K — tighten in uncertain conditions
    "RISK_OFF":    0.10,   # 10% = ~$3.6K — capital preservation mode
}

# Max simultaneous open swing positions (prevents overconcentration).
STOCK_MAX_OPEN_POSITIONS: Dict[str, int] = {
    "MOMENTUM":    5,
    "STRONG_BULL": 4,
    "BULL":        3,
    "NEUTRAL":     2,
    "RISK_OFF":    1,
}

# Targets / exits
STOCK_TARGET_R_MULTIPLE  = 2.0
STOCK_BREAKEVEN_AFTER_R  = 1.0
STOCK_USE_BREAKEVEN_TRAIL = True

# Stop distances (ATR multiples)
STOCK_STOP_ATR_PULLBACK = 1.0   # stop = EMA21 - 1.0*ATR
STOCK_STOP_ATR_EMA8     = 0.75  # tighter stop for EMA8 entries (shorter hold)
STOCK_STOP_ATR_BREAKOUT = 1.2   # stop = breakout_level - 1.2*ATR

# ── ADX floor for stock eligibility ─────────────────────────────────────────
# In momentum/bull markets ADX compresses — a static 20 floor misses valid entries.
STOCK_MIN_ADX: Dict[str, float] = {
    "MOMENTUM":    12.0,   # trend momentum speaks for itself
    "STRONG_BULL": 15.0,
    "BULL":        18.0,
    "NEUTRAL":     22.0,
    "RISK_OFF":    25.0,
}

# ── EMA8 pullback signal (primary signal for trending markets) ───────────────
# Entry: stock in strong uptrend, pulls back to touch/slightly cross EMA8.
# Best signal in MOMENTUM and STRONG_BULL regimes.
# Requires: EMA8 > EMA21 > SMA50 (stacked), RSI14 between 40-70 (not overbought)
STOCK_EMA8_PULLBACK_RSI14_MIN: Dict[str, float] = {
    "MOMENTUM":    35.0,   # wider range — trend is strong
    "STRONG_BULL": 38.0,
    "BULL":        40.0,
    "NEUTRAL":     42.0,
    "RISK_OFF":    45.0,
}
STOCK_EMA8_PULLBACK_RSI14_MAX: Dict[str, float] = {
    "MOMENTUM":    75.0,   # allow more RSI room in strong trends
    "STRONG_BULL": 72.0,
    "BULL":        70.0,
    "NEUTRAL":     65.0,
    "RISK_OFF":    60.0,
}
# How close to EMA8 price must be (fraction of EMA8)
STOCK_EMA8_BAND: Dict[str, float] = {
    "MOMENTUM":    0.025,  # within 2.5% of EMA8
    "STRONG_BULL": 0.020,
    "BULL":        0.018,
    "NEUTRAL":     0.015,
    "RISK_OFF":    0.010,
}

# ── RSI(2) deep pullback signal (secondary — works in all regimes) ───────────
# Best when market has pulled back and stock is genuinely oversold near support.
STOCK_PULLBACK_RSI2_MAX: Dict[str, float] = {
    "MOMENTUM":    15.0,   # looser — even brief dips qualify in a ripping market
    "STRONG_BULL": 10.0,
    "BULL":        8.0,
    "NEUTRAL":     6.0,
    "RISK_OFF":    4.0,
}
STOCK_PULLBACK_EMA_BAND: Dict[str, float] = {
    "MOMENTUM":    0.040,  # within 4% of EMA21 (wider — strong trends have shallower pullbacks)
    "STRONG_BULL": 0.030,
    "BULL":        0.025,
    "NEUTRAL":     0.020,
    "RISK_OFF":    0.015,
}

# ── Breakout signal ──────────────────────────────────────────────────────────
# 20-day high break with volume confirmation.
STOCK_BREAKOUT_VOL_MULT: Dict[str, float] = {
    "MOMENTUM":    1.0,    # nearly any volume on new high is valid when market rips
    "STRONG_BULL": 1.1,
    "BULL":        1.2,    # was 1.4 — was killing legit breakouts
    "NEUTRAL":     1.4,
    "RISK_OFF":    1.7,
}

# ============================================================
# Stock Universe
# ============================================================

STOCKS: List[str] = list(dict.fromkeys([
    # =========================
    # Tier A – Mega Cap Quality
    # =========================
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA",
    "AVGO", "TSM",  "ASML", "ORCL",  "CRM",
    "V",    "MA",   "JPM",  "GS",    "MS",
    "LLY",  "ABBV", "UNH",  "VRTX",
    "WMT",  "COST", "KO",   "PEP",
    "HD",   "LOW",
    "MCD",  "SBUX",
    "ADBE", "INTU",

    # =========================
    # Tier B – High Quality
    # =========================
    "ANET", "PANW", "CRWD",
    "CDNS", "SNPS",
    "LULU", "CMG",
    "DE",   "CAT",
    "SHW",
    "ZTS",
    "ELV",
    "ADSK",

    # =========================
    # Tier C – Cyclical / Liquid
    # =========================
    "AMD",  "MU",   "INTC", "ON",   "MCHP",
    "XOM",  "CVX",
    "COP",
    "CARR",
    "TGT",
    "ABNB", "UBER",
    "NFLX",

    # =========================
    # Select Growth
    # =========================
    "PLTR", "SHOP", "NET",  "MDB",  "SNOW", "BROS", "CELH",
]))

# ---- CSP excluded tickers (assignment risk too high for wheel) ----
CSP_EXCLUDED_TICKERS: List[str] = [
    "PLTR",   # 100x+ multiple, 2.5 beta, no dividend
    "SHOP",   # hypergrowth, no dividend
    "NET",    # unprofitable cloud
    "MDB",    # unprofitable database
    "SNOW",   # prone to -20% earnings gaps
    "BROS",   # small cap speculative
    "CELH",   # lesson learned — assigned at $48.59, -25% unrealized
]

# ---- CSP universe ----
CSP_STOCKS: List[str] = list(dict.fromkeys(
    [t for t in STOCKS if t not in CSP_EXCLUDED_TICKERS]
    + [
        "SPLG", "SPY",  "QQQ",
        "SCHD", "JEPI",
        "XLU",  "XLF",  "XLE", "XLV",
        "SMH",
        "PFE",  "MRK",  "BMY",
        "PG",   "CL",
        "GIS",  "KHC",
        "BAC",  "C",    "AXP",
        "BLK",
        "ETN",  "PH",
        "EMR",
        "TJX",
        "ROST",
    ]
))

CSP_DEFENSIVE_STOCKS: List[str] = [
    "SPY", "SPLG", "SCHD", "JEPI", "XLU", "XLF", "XLV",
    "KO",  "PEP",  "PG",   "CL",   "GIS", "KHC",
    "ABBV", "MRK", "UNH",  "LLY",
    "JPM",  "BAC",
    "XOM",  "CVX",
]

# ============================================================
# CSP configuration
# ============================================================

CSP_RISK_OFF_VIX                  = 25.0
CSP_RISK_OFF_MIN_OTM_PCT_DEFENSIVE = 0.10
CSP_RISK_OFF_MIN_OTM_PCT_RISKY     = 0.15

CSP_STRIKE_BASE_NORMAL  = "EMA_21"
CSP_STRIKE_BASE_RISK_OFF = "SMA_50"

DATA_PERIOD   = "1y"
DATA_INTERVAL = "1d"

ENABLE_CSP = True

CSP_POSITIONS_COLUMNS = [
    "id", "account", "open_date", "week_id", "ticker", "expiry",
    "dte_open", "strike", "contracts", "premium", "fill_premium",
    "cash_reserved", "tier", "status", "close_date", "close_type",
    "underlying_close_at_expiry", "shares_if_assigned",
    "assignment_cost_basis", "notes",
]

CC_POSITIONS_COLUMNS = [
    "id", "account", "open_date", "ticker", "expiry", "strike",
    "contracts", "premium", "status", "close_date", "close_type",
    "source_lot_id", "notes",
]

# ---- Take-profit ----
CSP_TAKE_PROFIT_PCT: Dict[str, float] = {
    "MOMENTUM":    0.80,   # hold longer — theta decay is your friend in calm markets
    "STRONG_BULL": 0.75,
    "BULL":        0.70,
    "NEUTRAL":     0.60,
    "RISK_OFF":    0.50,
}
CSP_TP_MAX_SPREAD_PCT = 0.50

# ---- DTE window ----
CSP_TARGET_DTE_MIN = 25
CSP_TARGET_DTE_MAX = 45

# ---- Risk / sizing ----
CSP_MAX_CASH_PER_TRADE = 20_000
CSP_MAX_CONTRACTS      = 3

# ---- Liquidity ----
CSP_MIN_OI     = 200
CSP_MIN_OI_ETF = 100
CSP_MIN_VOLUME = 10
CSP_MIN_BID    = 0.10
CSP_MIN_IV     = 0.20

# ---- Price ceiling ----
# REMOVED — no artificial price cap. If NVDA, GOOGL, META, or SPY pass all
# other filters, they are valid CSP candidates. You decide if you want to
# commit the notional. CSP_MAX_STOCK_PRICE = 0.0 means disabled.
CSP_MAX_STOCK_PRICE = 0.0

# ---- SMA200 slope filter ----
# Loosened in BULL/NEUTRAL to admit recovering quality names whose SMA200
# is still softening slightly after a correction. Genuine downtrends show
# -0.005 or worse; these thresholds admit correction-phase bounce candidates.
CSP_SMA200_MIN_SLOPE: Dict[str, float] = {
    "MOMENTUM":    None,     # no slope requirement — confirmed uptrend
    "STRONG_BULL": None,     # no slope requirement
    "BULL":        -0.003,   # was -0.001 — now admits most recovering names
    "NEUTRAL":     -0.004,   # correction-phase tolerance
    "RISK_OFF":    -0.005,   # most tolerant — defensive universe already restricted
}

# ---- Strike selection ----
CSP_STRIKE_MODE = "ema21_atr"
CSP_ATR_MULTS   = [1.50, 1.25, 1.00]

# ---- Minimum OTM floor ----
CSP_NORMAL_MIN_OTM_PCT: Dict[str, float] = {
    "MOMENTUM":    0.03,   # 3% — tightest cushion, market is in your favour
    "STRONG_BULL": 0.03,
    "BULL":        0.04,
    "NEUTRAL":     0.05,
    "RISK_OFF":    0.10,
}

# ---- ADX floor for CSP eligibility ----
CSP_MIN_ADX: Dict[str, float] = {
    "MOMENTUM":    10.0,
    "STRONG_BULL": 12.0,
    "BULL":        15.0,
    "NEUTRAL":     18.0,
    "RISK_OFF":    10.0,
}

# ---- Premium / yield tiers ----
CSP_MIN_PREMIUM_CONSERVATIVE = 100
CSP_MIN_PREMIUM_BALANCED     = 175
CSP_MIN_PREMIUM_AGGRESSIVE   = 250

CSP_MIN_YIELD_CONSERVATIVE: Dict[str, float] = {
    "MOMENTUM":    0.010,
    "STRONG_BULL": 0.009,
    "BULL":        0.008,
    "NEUTRAL":     0.007,
    "RISK_OFF":    0.010,
}
CSP_MIN_YIELD_BALANCED: Dict[str, float] = {
    "MOMENTUM":    0.015,
    "STRONG_BULL": 0.014,
    "BULL":        0.013,
    "NEUTRAL":     0.011,
    "RISK_OFF":    0.015,
}
CSP_MIN_YIELD_AGGRESSIVE: Dict[str, float] = {
    "MOMENTUM":    0.022,
    "STRONG_BULL": 0.020,
    "BULL":        0.018,
    "NEUTRAL":     0.016,
    "RISK_OFF":    0.025,
}

# ---- Tier caps ----
CSP_MAX_AGGRESSIVE_TOTAL    = 4
CSP_MAX_AGGRESSIVE_PER_WEEK = 2

# ---- Early assignment detection ----
CSP_EARLY_ASSIGN_ITM_PCT  = 0.15
CSP_EARLY_ASSIGN_MAX_DTE  = 3
CSP_EARLY_ASSIGN_WARN_ONLY = False

# ---- Roll detection ----
CSP_ROLL_CANDIDATE_ITM_PCT = 0.10
CSP_ROLL_CANDIDATE_MIN_DTE = 10

# ---- Intraday VIX spike guard ----
VIX_INTRADAY_SPIKE_THRESHOLD = 4.0

# ---- Sector concentration ----
CSP_MAX_POSITIONS_PER_SECTOR: Dict[str, int] = {
    "MOMENTUM":    5,
    "STRONG_BULL": 4,
    "BULL":        3,
    "NEUTRAL":     2,
    "RISK_OFF":    1,
}

CSP_TICKER_SECTOR: Dict[str, str] = {
    # Technology
    "AAPL": "TECH",  "MSFT": "TECH",  "NVDA": "TECH",  "AVGO": "TECH",
    "TSM":  "TECH",  "ASML": "TECH",  "ORCL": "TECH",  "CRM":  "TECH",
    "ADBE": "TECH",  "INTU": "TECH",  "AMD":  "TECH",  "MU":   "TECH",
    "INTC": "TECH",  "ON":   "TECH",  "MCHP": "TECH",  "ANET": "TECH",
    "CDNS": "TECH",  "SNPS": "TECH",  "ADSK": "TECH",
    "PLTR": "TECH",  "NET":  "TECH",  "MDB":  "TECH",  "SNOW": "TECH",
    # Internet
    "GOOGL": "INTERNET", "META": "INTERNET",
    "NFLX":  "INTERNET", "SHOP": "INTERNET",
    # Financials
    "V":   "FINANCIALS", "MA":  "FINANCIALS", "JPM": "FINANCIALS",
    "GS":  "FINANCIALS", "MS":  "FINANCIALS", "BAC": "FINANCIALS",
    "C":   "FINANCIALS", "AXP": "FINANCIALS", "BLK": "FINANCIALS",
    "XLF": "FINANCIALS",
    # Healthcare
    "LLY":  "HEALTHCARE", "ABBV": "HEALTHCARE", "UNH": "HEALTHCARE",
    "VRTX": "HEALTHCARE", "PFE":  "HEALTHCARE", "MRK": "HEALTHCARE",
    "BMY":  "HEALTHCARE", "ZTS":  "HEALTHCARE", "ELV": "HEALTHCARE",
    "XLV":  "HEALTHCARE",
    # Consumer Discretionary
    "AMZN": "CONSUMER_DISC", "LULU": "CONSUMER_DISC", "CMG": "CONSUMER_DISC",
    "MCD":  "CONSUMER_DISC", "SBUX": "CONSUMER_DISC", "HD":  "CONSUMER_DISC",
    "LOW":  "CONSUMER_DISC", "TGT":  "CONSUMER_DISC", "ABNB": "CONSUMER_DISC",
    "UBER": "CONSUMER_DISC", "CELH": "CONSUMER_DISC", "BROS": "CONSUMER_DISC",
    "ROST": "CONSUMER_DISC", "TJX":  "CONSUMER_DISC",
    # Consumer Staples
    "WMT": "STAPLES", "COST": "STAPLES", "KO":  "STAPLES",
    "PEP": "STAPLES", "PG":   "STAPLES", "CL":  "STAPLES",
    "GIS": "STAPLES", "KHC":  "STAPLES",
    # Energy
    "XOM": "ENERGY", "CVX": "ENERGY", "COP": "ENERGY",
    # Industrials
    "DE":   "INDUSTRIALS", "CAT":  "INDUSTRIALS", "ETN":  "INDUSTRIALS",
    "PH":   "INDUSTRIALS", "EMR":  "INDUSTRIALS", "CARR": "INDUSTRIALS",
    "SHW":  "INDUSTRIALS",
    # ETFs (own sector, no concentration limit applies meaningfully)
    "SPY":  "ETF_BROAD", "SPLG": "ETF_BROAD", "QQQ":  "ETF_BROAD",
    "JEPI": "ETF_BROAD", "XLU":  "ETF_BROAD", "SMH":  "ETF_BROAD",
    "SCHD": "ETF_BROAD", "XLE":  "ETF_BROAD", "XLF":  "ETF_BROAD",
}

# ============================================================
# Covered Call (CC) policy
# ============================================================

CC_TARGET_DTE_MIN = 14
CC_TARGET_DTE_MAX = 30

CC_ATR_MULT_NORMAL = 1.0
CC_ATR_MULT_MILD   = 1.5
CC_ATR_MULT_DEEP   = 2.0
CC_ATR_MULT_SEVERE = 2.5

CC_UNDERWATER_MILD_PCT = 0.10
CC_UNDERWATER_DEEP_PCT = 0.25

CC_STRIKE_FLOOR_BELOW_CURRENT_PCT = 0.02
CC_MIN_BID                        = 0.05
CC_ROLL_SIGNAL_THRESHOLD          = 0.97

CC_TAKE_PROFIT_PCT: Dict[str, float] = {
    "MOMENTUM":    0.80,
    "STRONG_BULL": 0.75,
    "BULL":        0.70,
    "NEUTRAL":     0.60,
    "RISK_OFF":    0.50,
}
CC_TP_MAX_SPREAD_PCT = 0.50

CC_DTE_BY_TIER: Dict[str, tuple] = {
    "NORMAL": (14, 21),
    "MILD":   (14, 25),
    "DEEP":   (21, 35),
    "SEVERE": (25, 35),
}

# ============================================================
# Slippage & fill model
# ============================================================

STOCK_SLIPPAGE_PER_SHARE    = 0.08
OPT_SELL_FILL_PCT           = 0.70
OPT_BUY_FILL_PCT            = 0.70
OPT_COMMISSION_PER_CONTRACT = 0.65
