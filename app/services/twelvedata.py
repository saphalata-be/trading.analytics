"""
TwelveData service — instrument search and OHLCV download.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import httpx

from app.config import TWELVEDATA_API_KEY

BASE_URL = "https://api.twelvedata.com"

# TwelveData free plan (testing): 1 request every 10 seconds
_REQUEST_INTERVAL = 10  # seconds between requests

_last_request_time: float = 0.0


class TwelveDataError(Exception):
    pass


def _get(endpoint: str, params: dict[str, Any]) -> dict:
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < _REQUEST_INTERVAL:
        time.sleep(_REQUEST_INTERVAL - elapsed)
    _last_request_time = time.time()

    params["apikey"] = TWELVEDATA_API_KEY
    with httpx.Client(timeout=30) as client:
        resp = client.get(f"{BASE_URL}{endpoint}", params=params)
        resp.raise_for_status()
        data = resp.json()
    if isinstance(data, dict) and data.get("status") == "error":
        raise TwelveDataError(data.get("message", "Unknown TwelveData error"))
    return data


def search_instruments(query: str, instrument_type: str = "") -> list[dict]:
    """
    Search instruments by symbol or name.
    Returns a list of dicts with keys: symbol, instrument_name, type, currency, exchange, country.
    """
    params: dict[str, Any] = {"symbol": query, "outputsize": 30}
    if instrument_type:
        params["type"] = instrument_type

    data = _get("/symbol_search", params)
    results = data.get("data", [])
    return [
        {
            "symbol": r.get("symbol", ""),
            "name": r.get("instrument_name", ""),
            "type": r.get("instrument_type", ""),
            "currency": r.get("currency", ""),
            "exchange": r.get("exchange", ""),
            "country": r.get("country", ""),
        }
        for r in results
    ]


def fetch_full_history(
    symbol: str,
    exchange: str,
    timeframe: str,
    progress_callback=None,
) -> list[dict]:
    """
    Download the full available OHLCV history for a symbol/timeframe.
    TwelveData returns max 5000 bars per request; we paginate using `end_date`.

    progress_callback(fetched: int, total_so_far: int) — optional callable.
    Returns list of dicts: {datetime, open, high, low, close, volume}
    """
    all_bars: list[dict] = []
    end_date: str | None = None
    page = 0

    while True:
        params: dict[str, Any] = {
            "symbol": symbol,
            "exchange": exchange,
            "interval": timeframe,
            "outputsize": 5000,
            "order": "DESC",  # newest first so we can paginate backwards
            "format": "JSON",
        }
        if end_date:
            params["end_date"] = end_date

        data = _get("/time_series", params)
        values = data.get("values", [])

        if not values:
            break

        bars = [
            {
                "datetime": v["datetime"],
                "open": float(v["open"]),
                "high": float(v["high"]),
                "low": float(v["low"]),
                "close": float(v["close"]),
                "volume": float(v.get("volume") or 0),
            }
            for v in values
        ]
        all_bars.extend(bars)
        page += 1

        if progress_callback:
            progress_callback(len(bars), len(all_bars))

        # If we got fewer bars than requested, we've reached the beginning
        if len(values) < 5000:
            break

        # Next page: end just before the oldest bar we received
        oldest_dt = values[-1]["datetime"]
        end_date = oldest_dt

    # Sort ascending
    all_bars.sort(key=lambda b: b["datetime"])
    return all_bars


def get_earliest_date(symbol: str, exchange: str, timeframe: str) -> str | None:
    """Return the datetime string of the earliest available bar."""
    params: dict[str, Any] = {
        "symbol": symbol,
        "exchange": exchange,
        "interval": timeframe,
        "outputsize": 1,
        "order": "ASC",
        "format": "JSON",
    }
    data = _get("/time_series", params)
    values = data.get("values", [])
    if values:
        return values[0]["datetime"]
    return None
