"""config.py

Project configuration.

"""

from __future__ import annotations
import os
from typing import Dict, List

# ---- Discord Webhook ---- #
# Read from environment so the URL is never committed to version control.
# If the env var is not set, Discord alerting is silently skipped (same
# behaviour as the previous hardcoded empty string).
WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

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

# Accounts that share the IRA monthly file (both tagged individually inside it)
IRA_ACCOUNTS = (IRA, ROTH)

ACCOUNT_SIZES: Dict[str, int] = {
    INDIVIDUAL: 120_000,
    IRA: 150_000,
    ROTH: 150_000,
}

# ---- Per-account wheel configuration ----
# INDIVIDUAL gets 10% margin on top of account size; IRA/ROTH are cash-secured only.
# weekly_divisor: buying_power * cap_pct / divisor = weekly new-entry target.
INDIVIDUAL_MARGIN_PCT = 0.10

WHEEL_ACCOUNT_CONFIG: Dict[str, dict] = {
    INDIVIDUAL: {
        "buying_power":   ACCOUNT_SIZES[INDIVIDUAL] * (1.0 + INDIVIDUAL_MARGIN_PCT),
        "cap_pct":        0.80,
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

# Derived caps and weekly targets — recalculate automatically when ACCOUNT_SIZES changes.
WHEEL_CAPS: Dict[str, int] = {
    acct: int(cfg["buying_power"] * cfg["cap_pct"])
    for acct, cfg in WHEEL_ACCOUNT_CONFIG.items()
}
WHEEL_WEEKLY_TARGETS: Dict[str, float] = {
    acct: cfg["buying_power"] * cfg["cap_pct"] / cfg["weekly_divisor"]
    for acct, cfg in WHEEL_ACCOUNT_CONFIG.items()
}

# INDIVIDUAL stock (non-wheel) cap is the slice not allocated to the wheel.
WHEEL_CAP_PCT = WHEEL_ACCOUNT_CONFIG[INDIVIDUAL]["cap_pct"]
INDIVIDUAL_STOCK_CAP_PCT = 1.0 - WHEEL_CAP_PCT
INDIVIDUAL_STOCK_CAP = int(ACCOUNT_SIZES[INDIVIDUAL] * INDIVIDUAL_STOCK_CAP_PCT)

# Retirement stock cap: same logic — the slice of each account not reserved for the wheel.
# Covers both tactical swing entries and long-hold retirement positions combined.
RETIREMENT_STOCK_CAP_PCT = 1.0 - WHEEL_ACCOUNT_CONFIG[IRA]["cap_pct"]
RETIREMENT_STOCK_CAPS: Dict[str, int] = {
    IRA:  int(ACCOUNT_SIZES[IRA]  * RETIREMENT_STOCK_CAP_PCT),
    ROTH: int(ACCOUNT_SIZES[ROTH] * RETIREMENT_STOCK_CAP_PCT),
}

# Retirement accounts can be more aggressive but still capped by account size
RETIREMENT_MAX_EQUITY_UTIL_PCT = 0.98  # cash buffer

# If a retirement holding is down >= 10%, only allow selling at breakeven (entry)
RETIREMENT_BREAKEVEN_ONLY_DD_PCT = 0.10

# Hard stop-loss for retirement buy-and-hold positions.
# Wide by design — these are quality compounders meant to be held through
# normal corrections (20–25% drawdowns are routine for great businesses).
# Only fires on genuine catastrophic blowups, not ordinary volatility.
# Set to 0.0 to disable entirely.
RETIREMENT_STOP_LOSS_PCT = 0.35

# ---- Retirement buy-and-hold stock strategy ----
# Retirement accounts run a different strategy from INDIVIDUAL swing trades:
#   - Quality Tier A names only (long-term compounders)
#   - Pullback entries only — no breakout chasing
#   - Flat position sizing (50% of $20K slice = ~$10K per name)
#   - Max 2 simultaneous positions per retirement account
#   - No take-profit target — let winners run indefinitely
#   - Exit only on catastrophic stop or manual decision
#
# Position size = RETIREMENT_STOCK_CAPS[acct] * RETIREMENT_POSITION_SIZE_PCT
RETIREMENT_POSITION_SIZE_PCT = 0.50   # 50% of the stock slice per position (~$10K)
RETIREMENT_MAX_STOCK_POSITIONS = 2    # max simultaneous holds per retirement account

# Tier A quality names eligible for retirement buy-and-hold.
# Deliberately narrow — these are businesses you'd want to own for years.
# Avoid high-beta, cyclicals, and speculative names regardless of signal quality.
RETIREMENT_STOCKS: List[str] = [
    # Mega-cap tech / software (durable moats, compounding earnings)
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "NVDA", "AVGO", "ORCL", "ADBE", "INTU",

    # Payments (toll-booth businesses, recession-resistant)
    "V", "MA",

    # Healthcare (pricing power, aging demographics)
    "LLY", "ABBV", "UNH", "VRTX",

    # Consumer staples / quality retail (pricing power, predictable cash flows)
    "WMT", "COST", "KO", "PEP", "HD",

    # High-quality financials
    "JPM", "GS",

    # Best-in-class franchises
    "MCD", "SBUX",
]

# Soft cross-sector diversification for retirement buy-and-hold.
# When True: if both retirement slots in an account would be in the same
# sector (e.g., both TECH), print a warning at entry time. No hard block.
RETIREMENT_DIVERSIFY_SECTORS = True

# Estimated annual dividend yields for retirement holdings.
# Used for display-only annotation of estimated dividend income — not written
# to any CSV. Approximate trailing figures; update periodically.
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

# Run after close: entries detected using EOD data, filled next open.
STOCK_REQUIRE_NEXTDAY_VALIDATION = True

# Position sizing: max % of INDIVIDUAL_STOCK_CAP per position
STOCK_MAX_POSITION_PCT_INDIVIDUAL = 0.15   # 15% of $20K slice = ~$3K max

# Risk per trade: % of INDIVIDUAL_STOCK_CAP at risk on the stop
STOCK_RISK_PCT_INDIVIDUAL = 0.050          # 5% of $20K slice = ~$1K risk cap

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

# Tickers explicitly excluded from the CSP/Wheel universe.
# These remain in STOCKS for swing-trade entries but are not eligible for
# cash-secured puts.  The Wheel requires willingness to own the stock at the
# assigned price for potentially months while selling CCs to recover cost
# basis.  High-multiple growth names with no dividend, extreme intraday vol,
# and unpredictable earnings gaps are poor Wheel candidates regardless of
# the premium they generate — high IV on these names reflects genuine tail
# risk, not just an attractive selling opportunity.
#
# CELH is included as a lesson already learned: assigned at $48.59, currently
# -25%, selling CCs at strikes still well below cost basis.
CSP_EXCLUDED_TICKERS: List[str] = [
    # === Speculative / hypergrowth — no dividend, extreme vol, assignment is a trap ===
    "PLTR",   # 100x+ revenue multiple, 2.5 beta, no dividend
    "SHOP",   # hypergrowth e-commerce, no dividend, wide swings
    "NET",    # unprofitable cloud, no dividend
    "MDB",    # unprofitable database, no dividend
    "SNOW",   # hypergrowth data, no dividend, prone to -20% earnings gaps
    "BROS",   # small cap, speculative consumer
    "CELH",   # lesson learned — assigned at $48.59, -25% unrealized
    # === Add others here as needed ===
]

# CSP Universe — STOCKS minus excluded tickers, plus stable ETFs and additional
# quality names suited to the Wheel (comfortable holding at assigned price).
CSP_STOCKS: List[str] = list(dict.fromkeys(
    [t for t in STOCKS if t not in CSP_EXCLUDED_TICKERS]
    + [
        # =========================
        # Core ETFs (wheel stabilizers)
        # =========================
        "SPLG","SPY","QQQ",
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
    "account",
    "open_date",
    "week_id",
    "ticker",
    "expiry",
    "dte_open",
    "strike",
    "contracts",
    "premium",        # mid-price estimate (display/scoring)
    "fill_premium",   # slippage+commission-adjusted actual fill (used for cost_basis at assignment)
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
    "account",
    "open_date",
    "ticker",
    "expiry",
    "strike",
    "contracts",
    "premium",
    "status",
    "close_date",
    "close_type",
    "source_lot_id",   # lot_id of the wheel lot this CC was written against
    "notes",
]

# ---- Take-profit / early close ----
# Close a CSP when current mid-price <= original premium * this fraction.
# 0.60 = close at 60% profit.
CSP_TAKE_PROFIT_PCT = 0.60

# Skip the take-profit close if the bid/ask spread is wider than this fraction
# of mid.  Wide spreads mean the quote is stale or illiquid — better to hold
# than close at an unknown price.  0.50 = spread must be <= 50% of mid.
CSP_TP_MAX_SPREAD_PCT = 0.50

# ---- DTE window ----
# Min lowered to 14 so the expiry picker can find pre-earnings expirations
# during earnings season, when 25-45 DTE windows land squarely on announcement
# dates for most of the universe.  The earnings guard in build_csp_candidates
# still blocks any expiry that straddles an earnings date, so lowering the
# floor does not increase earnings IV-crush risk — it simply gives the picker
# more expiry candidates to evaluate.
CSP_TARGET_DTE_MIN = 25
CSP_TARGET_DTE_MAX = 45

# ---- Risk / sizing ----
CSP_MAX_CASH_PER_TRADE = 20_000  # per-contract notional ceiling (strike * 100); used for early-exit sizing check only
# Hard cap on contracts per CSP position — prevents oversizing cheap stocks.
# 3 contracts = manageable assignment risk (~$15-45K notional depending on strike).
CSP_MAX_CONTRACTS = 3

# Liquidity filters
# Individual stocks need higher OI so stressed-market rolls stay fillable.
# ETFs (SPY, QQQ, SPLG, etc.) are deeply liquid; the lower floor is fine.
CSP_MIN_OI      = 200   # stocks — lowered from 300; still a meaningful liquidity floor
                        # for 1-2 contract retail trades while admitting quality names
                        # whose 6%+ OTM strikes have OI in the 150-300 range
CSP_MIN_OI_ETF  = 100   # ETF_BROAD sector tickers only
CSP_MIN_VOLUME  = 10
CSP_MIN_BID = 0.10

# IV sanity check (set to 0.0 to disable).
# Lower IV = less premium per unit of risk. This filter prevents selling very
# cheap options where the edge doesn't justify the capital commitment.
CSP_MIN_IV = 0.20

# Maximum underlying stock price for CSP eligibility.
# Belt-and-suspenders guard against high-priced speculative names that aren't
# explicitly listed in CSP_EXCLUDED_TICKERS.  Stocks above this price tend to
# either be high-multiple growth names (poor Wheel fit — high IV reflects real
# tail risk, not edge) or require $10K+ notional per contract (concentration).
# Set to 0.0 to disable this guard entirely.
CSP_MAX_STOCK_PRICE = 350.0

# SMA200 slope filter for CSP eligibility (NORMAL mode).
# Instead of requiring close > SMA200 (a hard binary cross), we require the
# SMA200 itself to still be rising — meaning the long-term trend was intact
# until recently.  Price below a rising SMA200 = pullback/correction in an
# uptrend.  Price below a falling SMA200 = structural downtrend; avoid.
#
# Measured as the 20-trading-day % change in SMA200 (pct_change(20)).
# -0.002 = allows very slight SMA200 softening (up to -0.2% over 20 days).
#   Genuine downtrends show -0.005 or worse; correction-phase quality names
#   sit in the -0.001 to -0.003 range.  Tighten back toward 0.0 once the
#   market recovery is confirmed.
# Set to None to fall back to the legacy close > SMA200 binary check.
CSP_SMA200_MIN_SLOPE = -0.002   # allow very slight SMA200 softening (correction phase)

# LOW_IV regime (VIX < 18): tighter yield floors.
# When markets are calm, premiums thin out. Raise the bar so we only sell
# when risk/reward still makes sense. Better to sit out than chase thin credits.
# AGGRESSIVE tier is blocked entirely in LOW_IV via allowed_tiers_for_regime().
CSP_MIN_YIELD_CONSERVATIVE_LOW_IV = 0.015   # vs 0.010 in NORMAL
CSP_MIN_YIELD_BALANCED_LOW_IV     = 0.020   # vs 0.015 in NORMAL

# ---- Strike selection ----
CSP_STRIKE_MODE = "ema21_atr"

# Try a small set of ATR distances to find a liquid strike.
# Higher = farther OTM (lower risk, less premium).
CSP_ATR_MULTS = [1.50, 1.25, 1.00]  # safer strikes (farther OTM)

# ---- Premium / yield tiers ----
# Yield floors calibrated for VIX ~18-25 (NORMAL regime).
# Original floors (1.0% / 1.5% / 2.0%) were set for a low-VIX bull market
# where quality names at 6% OTM / 30 DTE generated richer premiums.  In a
# VIX-20 correction, the same quality names (NVDA 170P, XLE 53P) produce
# 0.89-0.99% — just below the old floor.  Lowered by ~20% to reflect the
# current premium environment while keeping real yield requirements.
# At 0.8% on $17K cash over 28 days = ~$136 collected = ~10.5% annualised.
# Tighten back toward 1.0% once VIX settles below 18 (LOW_IV regime takes over).
CSP_MIN_PREMIUM_CONSERVATIVE = 100
CSP_MIN_PREMIUM_BALANCED = 175
CSP_MIN_PREMIUM_AGGRESSIVE = 250

CSP_MIN_YIELD_CONSERVATIVE = 0.008
CSP_MIN_YIELD_BALANCED = 0.013
CSP_MIN_YIELD_AGGRESSIVE = 0.018

# ---- Tier caps ----
CSP_MAX_AGGRESSIVE_TOTAL = 4
CSP_MAX_AGGRESSIVE_PER_WEEK = 2

# ---- Early assignment detection ----
# Scan OPEN CSPs daily and flag (or auto-mark) those deeply ITM.
# American-style options can be exercised any time — waiting until the
# scheduled expiry date leaves assigned lots unrecognised for days,
# blocking CC income and distorting exposure calculations.
#
# BOTH conditions must be true to trigger:
#   1. current_price <= strike * (1 - CSP_EARLY_ASSIGN_ITM_PCT)
#      Stock must be this far below the put strike.  15% is deep enough
#      that even high-beta names are very unlikely to recover in time.
#   2. DTE <= CSP_EARLY_ASSIGN_MAX_DTE
#      Only fires in the final days of the contract — same expiry week.
#      With 3 days left there is almost no realistic path back OTM for a
#      deeply ITM position, so auto-marking is safe.
#
# Together these prevent phantom assignments on volatile names like ASTS
# where a 10% ITM position with 8+ DTE still has a real chance of recovery.
CSP_EARLY_ASSIGN_ITM_PCT = 0.15    # 15% below strike required
CSP_EARLY_ASSIGN_MAX_DTE = 3       # must be within 3 calendar days of expiry

# False  → auto-mark ASSIGNED immediately (starts CC income sooner, recommended).
# True   → warn only, no state change (safer if you want manual confirmation).
CSP_EARLY_ASSIGN_WARN_ONLY = False

# ---- CSP roll candidate detection (display only) ----
# Flag open CSPs that are meaningfully ITM with enough DTE remaining that a
# roll (buy-to-close + re-open lower / further out) may collect a net credit.
# Both conditions must be true to flag:
#   1. current_price < strike * (1 - CSP_ROLL_CANDIDATE_ITM_PCT)
#   2. DTE remaining > CSP_ROLL_CANDIDATE_MIN_DTE
# No automated action — human decides whether to roll.
CSP_ROLL_CANDIDATE_ITM_PCT = 0.10   # 10% below strike
CSP_ROLL_CANDIDATE_MIN_DTE = 10     # at least 10 DTE remaining

# ---- Intraday VIX spike guard ----
# Before executing CSP orders, fetch the live VIX and compare to the prior
# EOD close. If the intraday VIX has risen by more than this threshold, any
# AGGRESSIVE-tier candidates are downgraded to BALANCED for that run.
# This prevents selling into a morning panic the prior day's VIX didn't show.
VIX_INTRADAY_SPIKE_THRESHOLD = 4.0  # points

# ---- Sector concentration limit ----
# No more than this many simultaneously OPEN CSPs in the same sector,
# enforced PER ACCOUNT.  Each account (INDIVIDUAL, IRA, ROTH) has its own
# independent sector count — they are separate legal entities with separate
# capital, so a TECH CSP in IRA does not block TECH capacity in INDIVIDUAL.
# The per-account cap still prevents correlated-assignment blowup within
# each pool of capital.
CSP_MAX_POSITIONS_PER_SECTOR = 3

# Static ticker→sector map covering the full CSP_STOCKS universe.
# Tickers not listed here fall into "OTHER" (no concentration limit applied).
# Sectors are intentionally broad — the goal is to prevent correlated-assignment
# blowups (e.g., all tech puts going ITM together), not precise GICS classification.
CSP_TICKER_SECTOR: Dict[str, str] = {
    # ── Technology ──────────────────────────────────────────────────
    "AAPL":  "TECH", "MSFT": "TECH", "NVDA": "TECH", "AVGO": "TECH",
    "TSM":   "TECH", "ASML": "TECH", "ORCL": "TECH", "CRM":  "TECH",
    "ADBE":  "TECH", "INTU": "TECH", "AMD":  "TECH", "MU":   "TECH",
    "INTC":  "TECH", "ON":   "TECH", "MCHP": "TECH", "ANET": "TECH",
    "CDNS":  "TECH", "SNPS": "TECH", "ADSK": "TECH",
    "PLTR":  "TECH", "NET":  "TECH", "MDB":  "TECH", "SNOW": "TECH",

    # ── Internet / E-Commerce ────────────────────────────────────────
    "GOOGL": "INTERNET", "META": "INTERNET",
    "NFLX":  "INTERNET", "SHOP":  "INTERNET",

    # ── Financials ───────────────────────────────────────────────────
    "V":    "FINANCIALS", "MA":  "FINANCIALS", "JPM": "FINANCIALS",
    "GS":   "FINANCIALS", "MS":  "FINANCIALS", "BAC": "FINANCIALS",
    "C":    "FINANCIALS", "AXP": "FINANCIALS", "BLK": "FINANCIALS",
    "XLF":  "FINANCIALS",

    # ── Healthcare ───────────────────────────────────────────────────
    "LLY":  "HEALTHCARE", "ABBV": "HEALTHCARE", "UNH": "HEALTHCARE",
    "VRTX": "HEALTHCARE", "PFE":  "HEALTHCARE", "MRK": "HEALTHCARE",
    "BMY":  "HEALTHCARE", "ZTS":  "HEALTHCARE", "ELV": "HEALTHCARE",
    "XLV":  "HEALTHCARE",

    # ── Consumer Discretionary ───────────────────────────────────────
    "AMZN": "CONSUMER_DISC", "LULU": "CONSUMER_DISC", "CMG": "CONSUMER_DISC",
    "MCD":  "CONSUMER_DISC", "SBUX": "CONSUMER_DISC", "HD":  "CONSUMER_DISC",
    "LOW":  "CONSUMER_DISC", "TGT":  "CONSUMER_DISC", "ABNB": "CONSUMER_DISC",
    "UBER": "CONSUMER_DISC", "CELH": "CONSUMER_DISC", "BROS": "CONSUMER_DISC",
    "ROST": "CONSUMER_DISC", "TJX":  "CONSUMER_DISC",

    # ── Consumer Staples ─────────────────────────────────────────────
    "WMT":  "STAPLES", "COST": "STAPLES", "KO":  "STAPLES",
    "PEP":  "STAPLES", "PG":   "STAPLES", "CL":  "STAPLES",
    "GIS":  "STAPLES", "KHC":  "STAPLES",

    # ── Energy ───────────────────────────────────────────────────────
    "XOM":  "ENERGY", "CVX": "ENERGY", "COP": "ENERGY",

    # ── Industrials ──────────────────────────────────────────────────
    "DE":   "INDUSTRIALS", "CAT": "INDUSTRIALS", "ETN": "INDUSTRIALS",
    "PH":   "INDUSTRIALS", "EMR": "INDUSTRIALS", "CARR": "INDUSTRIALS",
    "SHW":  "INDUSTRIALS",

    # ── Broad ETFs (low correlation — treated as their own sector) ───
    "SPY":  "ETF_BROAD", "SPLG": "ETF_BROAD", "QQQ": "ETF_BROAD",
    "JEPI": "ETF_BROAD", "XLU":  "ETF_BROAD",
    # Sector ETFs — no earnings calendar, treated same as broad ETFs for guard purposes
    "SMH":  "ETF_BROAD", "SCHD": "ETF_BROAD", "XLE":  "ETF_BROAD",
    "XLF":  "ETF_BROAD", "XLV":  "ETF_BROAD",
}

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

# ---- CC take-profit ----
# Close a CC when current mid value <= this fraction of opening premium.
# Mirrors the CSP take-profit rule — lock in gains early and recycle the lot.
CC_TAKE_PROFIT_PCT     = 0.60   # close at 50% profit
CC_TP_MAX_SPREAD_PCT   = 0.50   # skip if bid/ask spread > 50% of mid

# ---- CC DTE window by underwater tier ----
# Shorter DTE for NORMAL/MILD (fast cycling near or above basis).
# Longer DTE for DEEP/SEVERE (more premium per cycle on underwater lots).
CC_DTE_BY_TIER: Dict[str, tuple] = {
    "NORMAL": (14, 21),
    "MILD":   (14, 25),
    "DEEP":   (21, 35),
    "SEVERE": (25, 35),
}
# ============================================================
# Slippage & fill model  (paper trading realism)
# ============================================================
# Applied at every paper "execution" so P&L reflects realistic fills,
# not the optimistic mid-price / prior-close assumption.
#
# --- Stock entries ---
# Signal detected on yesterday's close; model next-morning fill as
# close + STOCK_SLIPPAGE_PER_SHARE.  Exits are not slipped.
STOCK_SLIPPAGE_PER_SHARE = 0.08      # ~$0.08/share ≈ 0.1-0.2% on a $50 stock

# --- Options sells (CSP open, CC open) ---
# fill_price = bid + (mid - bid) * OPT_SELL_FILL_PCT
OPT_SELL_FILL_PCT = 0.70             # 70% of way from bid to mid

# --- Options buys (CSP take-profit close) ---
# fill_price = ask - (ask - mid) * OPT_BUY_FILL_PCT
OPT_BUY_FILL_PCT = 0.70              # 70% of way from ask to mid

# --- Commissions ---
OPT_COMMISSION_PER_CONTRACT = 0.65   # $0.65/contract (typical retail)
