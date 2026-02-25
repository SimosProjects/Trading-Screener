"""utils.py

Shared helpers used across all modules.

Keep this file free of trading logic — pure utility only.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import sys
from typing import Union


# ============================================================
# Logging
# ============================================================

def get_logger(name: str) -> logging.Logger:
    """
    Return a module-level logger writing to stdout at INFO+.

    Call once at module top-level:
        log = get_logger(__name__)

    Production note: redirect to a rotating file handler before going live.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured; avoid duplicate handlers on reload

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    ))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


# ============================================================
# Date helpers
# ============================================================

def iso_week_id(d: dt.date) -> str:
    """ISO 8601 week identifier, e.g. '2025-W03'."""
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


# ============================================================
# Safe numeric coercion
# ============================================================

def safe_float(v: object, default: float = 0.0) -> float:
    """
    Convert v to float without raising.

    Returns default on None, empty string, or 'NaN'.
    Used pervasively when reading CSV fields that may be blank.
    """
    try:
        if v is None:
            return default
        s = str(v).strip()
        if s == "" or s.upper() == "NAN":
            return default
        return float(s)
    except Exception:
        return default


def safe_int(v: object, default: int = 0) -> int:
    """
    Convert v to int without raising.

    Goes through float first so '1.0' parses as 1.
    """
    try:
        if v is None:
            return default
        s = str(v).strip()
        if s == "" or s.upper() == "NAN":
            return default
        return int(float(s))
    except Exception:
        return default


# ============================================================
# Atomic CSV write
# ============================================================

def atomic_write(path: str, write_fn) -> None:
    """
    Write to a .tmp file then rename into place.

    On POSIX, os.replace is atomic — a crash between write and rename
    leaves the original file intact.  On Windows it is not truly atomic
    but still safer than truncating in place.

    Usage:
        def _write(f):
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            for row in rows:
                w.writerow(row)
        atomic_write(path, _write)
    """
    tmp = path + ".tmp"
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        with open(tmp, "w", newline="") as f:
            write_fn(f)
        os.replace(tmp, path)
    except Exception:
        # Clean up the tmp file if something went wrong; don't corrupt the original.
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
