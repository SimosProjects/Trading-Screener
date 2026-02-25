"""data_cache.py

Single-session price and OHLCV cache.

All yfinance downloads for the screener run go through here.
One batch call at startup replaces 80+ individual per-ticker calls,
cutting runtime from ~10 minutes to under 2 minutes.

Usage (in screener orchestrator):
    from data_cache import DataCache
    cache = DataCache(all_tickers)
    cache.warm()                     # one batch download
    df = cache.ohlcv("AAPL")        # O(1) lookup; returns copy
    px = cache.last_close("AAPL")   # float or None
"""

from __future__ import annotations

import datetime as dt
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf

from utils import get_logger

log = get_logger(__name__)


class DataCache:
    """
    Holds a full-year daily OHLCV slice for every ticker needed in one run.

    Design constraints:
      - Immutable after warm(): callers get copies, so indicator mutations
        in add_indicators() don't corrupt the cache.
      - Thread-unsafe by design: this is a single-process screener.
      - Failures are per-ticker and logged; a bad ticker doesn't abort the run.
    """

    def __init__(self, tickers: List[str], period: str = "1y", interval: str = "1d") -> None:
        self._tickers: List[str] = sorted({t.strip().upper() for t in tickers if t and t.strip()})
        self._period   = period
        self._interval = interval
        # Keyed by upper-case ticker; value is a clean OHLCV DataFrame.
        self._store: Dict[str, pd.DataFrame] = {}
        self._warmed = False

    # ------------------------------------------------------------------
    # Population
    # ------------------------------------------------------------------

    def warm(self) -> None:
        """
        Download all tickers in a single yfinance batch call.

        yfinance returns a MultiIndex DataFrame for multi-ticker downloads.
        We split it into per-ticker slices immediately so the rest of the
        codebase sees the same simple column structure it always has.
        """
        if self._warmed:
            return

        if not self._tickers:
            self._warmed = True
            return

        log.info("DataCache: warming %d tickers (period=%s, interval=%s) …",
                 len(self._tickers), self._period, self._interval)

        # yfinance batch download — pass tickers as a list, not a space-joined string,
        # to avoid silent truncation on very long strings.
        try:
            raw = yf.download(
                self._tickers,
                period=self._period,
                interval=self._interval,
                auto_adjust=False,
                progress=False,
                group_by="ticker",
            )
        except Exception as e:
            log.error("DataCache batch download failed: %s — falling back to empty cache", e)
            self._warmed = True
            return

        if raw is None or raw.empty:
            log.warning("DataCache: batch download returned empty DataFrame")
            self._warmed = True
            return

        # Single-ticker download doesn't produce a MultiIndex.
        if len(self._tickers) == 1:
            tkr = self._tickers[0]
            df = raw.copy()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.dropna(inplace=True)
            if not df.empty:
                self._store[tkr] = df
        else:
            for tkr in self._tickers:
                try:
                    # yfinance multi-ticker: top-level is ticker, second level is OHLCV field.
                    if tkr not in raw.columns.get_level_values(0):
                        log.warning("DataCache: %s not in batch result", tkr)
                        continue
                    df = raw[tkr].copy()
                    df.dropna(inplace=True)
                    if not df.empty:
                        self._store[tkr] = df
                except Exception as e:
                    log.warning("DataCache: failed to extract %s: %s", tkr, e)

        loaded = len(self._store)
        missed = len(self._tickers) - loaded
        log.info("DataCache: ready — %d tickers loaded, %d failed/empty", loaded, missed)
        self._warmed = True

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def ohlcv(self, ticker: str) -> pd.DataFrame:
        """
        Return a copy of the cached OHLCV DataFrame.

        Always returns a copy so callers can mutate freely (e.g., add_indicators).
        Returns empty DataFrame if ticker not in cache.
        """
        tkr = ticker.strip().upper()
        df = self._store.get(tkr)
        if df is None or df.empty:
            return pd.DataFrame()
        return df.copy()

    def last_close(self, ticker: str) -> Optional[float]:
        """Return the most recent close price, or None if unavailable."""
        df = self.ohlcv(ticker)
        if df.empty or "Close" not in df.columns:
            return None
        try:
            return float(df["Close"].dropna().iloc[-1])
        except Exception:
            return None

    def last_closes(self, tickers: List[str]) -> Dict[str, float]:
        """Batch last-close lookup — O(n) dict build, no network calls."""
        result: Dict[str, float] = {}
        for t in tickers:
            px = self.last_close(t)
            if px is not None:
                result[t.strip().upper()] = px
        return result

    def has(self, ticker: str) -> bool:
        return ticker.strip().upper() in self._store

    @property
    def tickers(self) -> List[str]:
        return list(self._store.keys())
