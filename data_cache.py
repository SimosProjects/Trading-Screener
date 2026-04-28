"""data_cache.py

Single-session price and OHLCV cache.

All yfinance downloads for the screener run go through here.
One batch call at startup replaces 80+ individual per-ticker calls,
cutting runtime from ~10 minutes to under 2 minutes.

Usage (in screener orchestrator):
    from data_cache import DataCache
    cache = DataCache(all_tickers)
    cache.warm()                     # one batch download, with retry
    df = cache.ohlcv("AAPL")        # O(1) lookup; returns copy
    px = cache.last_close("AAPL")   # float or None
"""

from __future__ import annotations

import time
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf

from utils import get_logger

log = get_logger(__name__)

# Max attempts for the initial full-batch download before falling back to chunks.
_BATCH_MAX_ATTEMPTS = 3

# Seconds to wait before each retry: [30s, 60s] between attempts 1→2 and 2→3.
_BATCH_RETRY_DELAYS = [30, 60]

# Chunk size and inter-chunk delay used when the full batch keeps failing.
# Smaller chunks are less likely to trigger rate limits; the delay gives
# yfinance's per-IP bucket time to refill between chunks.
_CHUNK_SIZE  = 20
_CHUNK_DELAY = 15   # seconds between chunks


class DataCache:
    """
    Holds a full-year daily OHLCV slice for every ticker needed in one run.

    Design notes:
      - Immutable after warm(): callers get copies so indicator mutations
        in add_indicators() don't corrupt the cache.
      - Thread-unsafe by design — single-process screener.
      - Per-ticker failures are logged; a bad ticker never aborts the run.
      - Rate limit errors trigger a retry with backoff, then a chunked
        fallback, so a transient GitHub Actions IP throttle doesn't produce
        an empty cache and a useless run.
    """

    def __init__(self, tickers: List[str], period: str = "1y", interval: str = "1d") -> None:
        self._tickers: List[str] = sorted({t.strip().upper() for t in tickers if t and t.strip()})
        self._period   = period
        self._interval = interval
        self._store: Dict[str, pd.DataFrame] = {}
        self._warmed   = False

    # ------------------------------------------------------------------
    # Population
    # ------------------------------------------------------------------

    def warm(self) -> None:
        """
        Download all tickers and populate the cache.

        Strategy:
          1. Attempt a single full-batch yfinance download (fastest path).
          2. If rate-limited, wait and retry up to _BATCH_MAX_ATTEMPTS times.
          3. If still failing after all retries, fall back to chunked downloads
             (_CHUNK_SIZE tickers at a time) with a delay between each chunk.

        This ensures the cache is populated even when the runner's IP is
        being throttled, which is common on shared GitHub Actions hosts.
        """
        if self._warmed:
            return
        if not self._tickers:
            self._warmed = True
            return

        log.info("DataCache: warming %d tickers (period=%s, interval=%s) …",
                 len(self._tickers), self._period, self._interval)

        # --- Attempt full-batch download with retry ---
        raw = self._download_batch_with_retry(self._tickers)

        if raw is not None and not raw.empty:
            self._extract_from_raw(raw, self._tickers)
        else:
            # Full batch failed — fall back to chunked downloads.
            log.warning(
                "DataCache: full-batch download failed after %d attempts — "
                "falling back to chunked download (%d tickers/chunk, %ds delay).",
                _BATCH_MAX_ATTEMPTS, _CHUNK_SIZE, _CHUNK_DELAY,
            )
            self._download_in_chunks(self._tickers)

        loaded = len(self._store)
        missed = len(self._tickers) - loaded
        log.info("DataCache: ready — %d tickers loaded, %d failed/empty.", loaded, missed)
        self._warmed = True

    def _download_batch_with_retry(self, tickers: List[str]) -> Optional[pd.DataFrame]:
        """
        Attempt a yfinance batch download, retrying on rate limit errors.
        Returns the raw DataFrame on success, or None if all attempts fail.
        """
        for attempt in range(1, _BATCH_MAX_ATTEMPTS + 1):
            try:
                raw = yf.download(
                    tickers,
                    period=self._period,
                    interval=self._interval,
                    auto_adjust=False,
                    progress=False,
                    group_by="ticker",
                )
                if raw is not None and not raw.empty:
                    return raw
                log.warning("DataCache: attempt %d/%d returned empty DataFrame.",
                            attempt, _BATCH_MAX_ATTEMPTS)
            except Exception as e:
                is_rate_limit = "rate" in str(e).lower() or "429" in str(e)
                log.warning("DataCache: attempt %d/%d failed: %s", attempt, _BATCH_MAX_ATTEMPTS, e)
                if not is_rate_limit:
                    # Non-rate-limit error (network down, bad ticker list, etc.)
                    # — no point retrying.
                    return None

            if attempt < _BATCH_MAX_ATTEMPTS:
                delay = _BATCH_RETRY_DELAYS[attempt - 1]
                log.info("DataCache: waiting %ds before retry …", delay)
                time.sleep(delay)

        return None

    def _download_in_chunks(self, tickers: List[str]) -> None:
        """
        Download tickers in small chunks with a delay between each chunk.
        Populates self._store directly. Used as a fallback when the full
        batch keeps hitting rate limits.
        """
        chunks = [tickers[i:i + _CHUNK_SIZE] for i in range(0, len(tickers), _CHUNK_SIZE)]
        log.info("DataCache: chunked download — %d chunks of up to %d tickers.",
                 len(chunks), _CHUNK_SIZE)

        for idx, chunk in enumerate(chunks, start=1):
            if idx > 1:
                log.info("DataCache: chunk %d/%d — waiting %ds …", idx, len(chunks), _CHUNK_DELAY)
                time.sleep(_CHUNK_DELAY)

            log.info("DataCache: chunk %d/%d — downloading %d tickers.", idx, len(chunks), len(chunk))
            try:
                raw = yf.download(
                    chunk,
                    period=self._period,
                    interval=self._interval,
                    auto_adjust=False,
                    progress=False,
                    group_by="ticker",
                )
                if raw is None or raw.empty:
                    log.warning("DataCache: chunk %d/%d returned empty — skipping.", idx, len(chunks))
                    continue
                self._extract_from_raw(raw, chunk)
            except Exception as e:
                log.warning("DataCache: chunk %d/%d failed: %s — skipping.", idx, len(chunks), e)

    def _extract_from_raw(self, raw: pd.DataFrame, tickers: List[str]) -> None:
        """
        Parse a raw yfinance DataFrame and store per-ticker OHLCV slices.
        Handles both single-ticker (flat columns) and multi-ticker (MultiIndex) results.
        """
        if len(tickers) == 1:
            tkr = tickers[0]
            df  = raw.copy()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.dropna(inplace=True)
            if not df.empty:
                self._store[tkr] = df
            return

        # Multi-ticker result — top level is ticker, second level is OHLCV field.
        for tkr in tickers:
            try:
                if tkr not in raw.columns.get_level_values(0):
                    log.warning("DataCache: %s not in batch result.", tkr)
                    continue
                df = raw[tkr].copy()
                df.dropna(inplace=True)
                if not df.empty:
                    self._store[tkr] = df
            except Exception as e:
                log.warning("DataCache: failed to extract %s: %s", tkr, e)

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def ohlcv(self, ticker: str) -> pd.DataFrame:
        """Return a copy of the cached OHLCV DataFrame, or empty if unavailable."""
        tkr = ticker.strip().upper()
        df  = self._store.get(tkr)
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
        """Batch last-close lookup — no network calls, O(n) over the store."""
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
