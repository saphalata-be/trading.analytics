from __future__ import annotations

import calendar
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

DEFAULT_ATR_MODE = "d1_1month"

ATR_MODE_DAILY_MONTHS = {
    DEFAULT_ATR_MODE: 1,
    "d1_6months": 6,
}

ATR_MODE_LABELS = {
    DEFAULT_ATR_MODE: "ATR D1 1 mois",
    "d1_6months": "ATR D1 6 mois",
}

ATR_MODE_OPTIONS = tuple(ATR_MODE_LABELS)


def normalize_atr_mode(value: str | None) -> str:
    if value in ATR_MODE_LABELS:
        return value
    return DEFAULT_ATR_MODE


def atr_mode_label(value: str | None) -> str:
    return ATR_MODE_LABELS[normalize_atr_mode(value)]


def atr_mode_months(value: str | None) -> int:
    return ATR_MODE_DAILY_MONTHS[normalize_atr_mode(value)]


def subtract_months(value: date | datetime, months: int) -> date:
    if isinstance(value, datetime):
        value = value.date()

    month_index = value.month - 1 - months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def daily_atr_window_start(before_dt: date | datetime, atr_mode: str | None) -> date:
    return subtract_months(before_dt, atr_mode_months(atr_mode))


def infer_point_size(symbol: str, prices: list[float]) -> float:
    """Infer the MT5-like point size from imported prices."""
    max_decimals = 0
    for price in prices[:5000]:
        try:
            decimal = Decimal(str(price)).normalize()
        except (InvalidOperation, ValueError):
            continue
        exponent = decimal.as_tuple().exponent
        if exponent < 0:
            max_decimals = max(max_decimals, -exponent)

    if max_decimals > 0:
        return 10 ** -max_decimals

    symbol = symbol.upper()
    if len(symbol) == 6 and symbol.isalpha():
        return 0.001 if symbol.endswith("JPY") else 0.00001
    return 1.0

