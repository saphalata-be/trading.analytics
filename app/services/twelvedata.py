"""
TwelveData service — instrument search and OHLCV download.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.config import TWELVEDATA_API_KEY

BASE_URL = "https://api.twelvedata.com"

# These values come from TwelveData's instrument_type field and are not valid
# exchange identifiers for the time_series / earliest_timestamp endpoints.
_NON_EXCHANGE_VALUES = {"COMMODITY", "PHYSICAL CURRENCY", ""}

# TwelveData symbol_search returns a few commodity identifiers that are not the
# identifiers accepted by time_series. Use the working history symbols here.
_HISTORY_SYMBOL_ALIASES: dict[str, tuple[str, ...]] = {
    "W_1": ("ZW1",),
    "S_1": ("S1",),
}

_INVALID_SYMBOL_ERROR = "**symbol** or **figi** parameter is missing or invalid"

# TwelveData free plan (testing): 1 request every 10 seconds
_REQUEST_INTERVAL = 1.5  # seconds between requests

_last_request_time: float = 0.0

# Oldest date we ever want to fetch
HISTORY_START_DATE = "2010-01-01 00:00:00"


class TwelveDataError(Exception):
    pass


def _time_series_params(
    symbol: str,
    exchange: str,
    timeframe: str,
    outputsize: int,
    order: str,
    end_date: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "symbol": symbol,
        "interval": timeframe,
        "outputsize": outputsize,
        "order": order,
        "format": "JSON",
    }
    if exchange and exchange not in _NON_EXCHANGE_VALUES:
        params["exchange"] = exchange
    if end_date:
        params["end_date"] = end_date
    return params


def _time_series_candidates(symbol: str) -> list[str]:
    candidates = [symbol, *_HISTORY_SYMBOL_ALIASES.get(symbol, ())]
    seen: set[str] = set()
    deduped: list[str] = []
    for candidate in candidates:
        if candidate not in seen:
            seen.add(candidate)
            deduped.append(candidate)
    return deduped


def _unsupported_history_symbol_error(symbol: str, candidates: list[str]) -> TwelveDataError:
    aliases = [candidate for candidate in candidates if candidate != symbol]
    if aliases:
        return TwelveDataError(
            f"Aucune serie historique TwelveData exploitable pour {symbol}. "
            f"Alias testes: {', '.join(aliases)}."
        )
    return TwelveDataError(f"Aucune serie historique TwelveData exploitable pour {symbol}.")


def _get_time_series(
    symbol: str,
    exchange: str,
    timeframe: str,
    outputsize: int,
    order: str,
    end_date: str | None = None,
) -> tuple[str, dict[str, Any]]:
    candidates = _time_series_candidates(symbol)

    for candidate in candidates:
        params = _time_series_params(candidate, exchange, timeframe, outputsize, order, end_date)
        try:
            return candidate, _get("/time_series", params)
        except TwelveDataError as exc:
            if _INVALID_SYMBOL_ERROR in str(exc):
                continue
            if candidate != symbol:
                raise TwelveDataError(
                    f"{symbol} utilise le symbole TwelveData {candidate}: {exc}"
                ) from exc
            raise

    raise _unsupported_history_symbol_error(symbol, candidates)


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
    start_date: str | None = None,
    progress_callback=None,
    batch_callback=None,
) -> list[dict]:
    """
    Download OHLCV history for a symbol/timeframe.

    - If `start_date` is provided, only bars >= start_date are fetched (incremental update).
    - Otherwise, history is fetched from HISTORY_START_DATE (2014-01-01) forward.
    - TwelveData returns max 5000 bars per request; we paginate using `end_date` (DESC order).

    progress_callback(fetched: int, total_so_far: int) — optional callable.
    Returns list of dicts: {datetime, open, high, low, close, volume}, sorted ascending.
    """
    # Determine the earliest datetime we want (as a comparable string "YYYY-MM-DD HH:MM:SS")
    cutoff = start_date if start_date else HISTORY_START_DATE

    all_bars: list[dict] = []
    total_bars_fetched = 0
    end_date: str | None = None  # pagination cursor (DESC walk)
    page = 0
    api_symbol: str | None = None

    while True:
        if api_symbol is None:
            api_symbol, data = _get_time_series(
                symbol,
                exchange,
                timeframe,
                outputsize=5000,
                order="DESC",
                end_date=end_date,
            )
        else:
            data = _get(
                "/time_series",
                _time_series_params(api_symbol, exchange, timeframe, 5000, "DESC", end_date),
            )
        values = data.get("values", [])

        if not values:
            break

        bars = []
        reached_cutoff = False
        for v in values:
            dt_str = v["datetime"]
            # Stop collecting if we've gone past our cutoff date
            if dt_str < cutoff:
                reached_cutoff = True
                break
            bars.append({
                "datetime": dt_str,
                "open": float(v["open"]),
                "high": float(v["high"]),
                "low": float(v["low"]),
                "close": float(v["close"]),
                "volume": float(v.get("volume") or 0),
            })

        if batch_callback and bars:
            batch_callback(bars)
        else:
            all_bars.extend(bars)

        total_bars_fetched += len(bars)
        page += 1

        if progress_callback:
            progress_callback(len(bars), total_bars_fetched)

        # Stop if we hit the cutoff date or got fewer bars than the page size
        if reached_cutoff or len(values) < 5000:
            break

        # Next page: set end_date to 1 minute before the oldest bar received.
        # TwelveData's end_date is inclusive, so reusing the exact datetime
        # would return the same bar again and loop forever on full pages.
        oldest_dt = datetime.strptime(values[-1]["datetime"], "%Y-%m-%d %H:%M:%S")
        end_date = (oldest_dt - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")

    # Sort ascending
    all_bars.sort(key=lambda b: b["datetime"])
    return all_bars


def get_earliest_date(symbol: str, exchange: str, timeframe: str) -> str | None:
    """Return the datetime string of the earliest available bar."""
    _, data = _get_time_series(symbol, exchange, timeframe, outputsize=1, order="ASC")
    values = data.get("values", [])
    if values:
        return values[0]["datetime"]
    return None
