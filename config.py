# config.py
from typing import List

# ---- Discord Webhook ---- #
# Set this to your Discord webhook. Keep it OUT of git.
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
ACCOUNT_SIZE = 110_000
WHEEL_CAP_PCT = 0.75
WHEEL_CAP = int(ACCOUNT_SIZE * WHEEL_CAP_PCT)
WHEEL_WEEKLY_TARGET = WHEEL_CAP / 4.0

# ---- Universe ---- #
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
        "HD", "LOW", "DIS", "CMCSA",
        "COIN", "BBAI", "SOUN", "QUBT", "CLSK"
    ]
))

# ---- Market data ---- #
DATA_PERIOD = "1y"
DATA_INTERVAL = "1d"

# ---- CSP enable ---- #
ENABLE_CSP = True
