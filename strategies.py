# strategies.py
import pandas as pd
import yfinance as yf
import ta

from typing import Dict, List, Tuple


# ============================================================
# Market data helpers
# ============================================================

def download_ohlcv(ticker: str, period="1y", interval="1d") -> pd.DataFrame:
    df = yf.download(
        ticker,
        period=period,
        interval=interval,
        auto_adjust=False,
        progress=False,
    )
    df.dropna(inplace=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close, high, low, vol = df["Close"], df["High"], df["Low"], df["Volume"]

    df["SMA_50"] = ta.trend.sma_indicator(close, 50)
    df["SMA_200"] = ta.trend.sma_indicator(close, 200)
    df["EMA_21"] = ta.trend.ema_indicator(close, 21)
    df["ATR_14"] = ta.volatility.average_true_range(high, low, close, 14)
    df["RSI_2"] = ta.momentum.rsi(close, 2)
    df["ADX_14"] = ta.trend.adx(high, low, close, 14)
    df["VOL_SMA_10"] = vol.rolling(10).mean()
    df["HIGH_20"] = close.rolling(20).max()

    return df


# ============================================================
# Market regime (SPY / QQQ / VIX)
# ============================================================

def market_context() -> Dict[str, float]:
    spy = add_indicators(download_ohlcv("SPY")).iloc[-1]
    qqq = add_indicators(download_ohlcv("QQQ")).iloc[-1]
    vix_df = download_ohlcv("^VIX")
    vix_close = float(vix_df["Close"].iloc[-1])

    context = {
        "spy_close": float(spy["Close"]),
        "spy_above_200": spy["Close"] > spy["SMA_200"],
        "spy_above_50": spy["Close"] > spy["SMA_50"],
        "spy_above_21": spy["Close"] > spy["EMA_21"],
        "qqq_close": float(qqq["Close"]),
        "qqq_above_50": qqq["Close"] > qqq["SMA_50"],
        "vix": vix_close,
        "vix_lt_18": vix_close < 18,
        "vix_lt_25": vix_close < 25,
    }

    return context


def print_market_context(ctx: Dict[str, float]) -> None:
    print("\n📊 MARKET CONTEXT")
    print("SPY:")
    print(f"  Close: {ctx['spy_close']:.2f}")
    print(f"  Above 200 SMA: {ctx['spy_above_200']}")
    print(f"  Above 50 SMA:  {ctx['spy_above_50']}")
    print(f"  Above 21 EMA:  {ctx['spy_above_21']}")

    print("\nQQQ:")
    print(f"  Close: {ctx['qqq_close']:.2f}")
    print(f"  Above 50 SMA: {ctx['qqq_above_50']}")

    print("\nVIX:")
    print(f"  Close: {ctx['vix']:.2f}")
    print(f"  < 18 (low): {ctx['vix_lt_18']}")
    print(f"  < 25 (ok):  {ctx['vix_lt_25']}")
    print()


# ============================================================
# CSP Strategy (THIS IS YOUR EDGE)
# ============================================================

def csp_signal(stock_row: pd.Series) -> bool:
    """
    High-quality CSP entry:
    - strong uptrend
    - short-term pullback
    """
    return (
        stock_row["Close"] > stock_row["SMA_200"] and
        stock_row["Close"] > stock_row["SMA_50"] and
        stock_row["EMA_21"] > stock_row["SMA_50"] and
        stock_row["ADX_14"] > 20 and
        stock_row["RSI_2"] < 10
    )


def choose_csp_strike(stock_row: pd.Series, puts: pd.DataFrame):
    """
    Sell puts BELOW technical support (EMA21 - ATR).
    """
    support = stock_row["EMA_21"] - stock_row["ATR_14"]
    valid = puts[puts["strike"] < support]

    if valid.empty:
        return None

    return valid.iloc[-1]


def find_csp_candidates(tickers: List[str]) -> Tuple[List[dict], List[str]]:
    """
    Returns:
      - list of CSP trade ideas
      - list of watchlist tickers (trend good, no pullback)
    """
    ideas = []
    watch = []

    for ticker in tickers:
        try:
            df = add_indicators(download_ohlcv(ticker))
            last = df.iloc[-1]

            if not (
                last["Close"] > last["SMA_200"] and
                last["Close"] > last["SMA_50"] and
                last["EMA_21"] > last["SMA_50"]
            ):
                continue

            if not csp_signal(last):
                watch.append(ticker)
                continue

            t = yf.Ticker(ticker)
            expiries = t.options
            if not expiries:
                continue

            exp = expiries[0]
            puts = t.option_chain(exp).puts

            strike_row = choose_csp_strike(last, puts)
            if strike_row is None:
                continue

            bid = float(strike_row["bid"])
            ask = float(strike_row["ask"])
            if bid <= 0 or ask < bid:
                continue

            mid = (bid + ask) / 2

            ideas.append({
                "ticker": ticker,
                "expiry": exp,
                "strike": float(strike_row["strike"]),
                "premium": mid * 100,
                "reason": "Pullback to EMA21 in uptrend",
            })

        except Exception:
            continue

    return ideas, watch


# ============================================================
# Discord summary builder
# ============================================================

def build_discord_summary(
    date: str,
    market_ctx: Dict[str, float],
    csp_ideas: List[dict],
    watch: List[str],
) -> str:

    lines = []
    lines.append("━━━━━━━━━━ 📊 STRATEGY SUMMARY ━━━━━━━━━━")
    lines.append(f"📅 {date}")
    lines.append("")

    lines.append("📊 Market:")
    lines.append(
        f"SPY {market_ctx['spy_close']:.2f} | "
        f"QQQ {market_ctx['qqq_close']:.2f} | "
        f"VIX {market_ctx['vix']:.2f}"
    )
    lines.append("")

    if csp_ideas:
        lines.append("💰 CSP Signals:")
        for i in csp_ideas:
            lines.append(
                f"• {i['ticker']} {i['expiry']} "
                f"{i['strike']:.0f}P | ${i['premium']:.0f} | {i['reason']}"
            )
    else:
        lines.append("💰 CSP Signals: none")

    lines.append("")

    if watch:
        lines.append("👀 Watchlist:")
        lines.append(", ".join(watch))
    else:
        lines.append("👀 Watchlist: none")

    return "\n".join(lines)
