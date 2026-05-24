from __future__ import annotations

from decimal import Decimal, InvalidOperation

DEFAULT_ATR_MODE = "d1_50"

ATR_MODE_FIXED_POINTS = {
    "fixed_500": 500.0,
    "fixed_1000": 1000.0,
}

ATR_MODE_LABELS = {
    DEFAULT_ATR_MODE: "ATR50 D1",
    "fixed_500": "ATR fixe 500 pts",
    "fixed_1000": "ATR fixe 1000 pts",
}

ATR_MODE_OPTIONS = tuple(ATR_MODE_LABELS)


def normalize_atr_mode(value: str | None) -> str:
    if value in ATR_MODE_LABELS:
        return value
    return DEFAULT_ATR_MODE


def atr_mode_label(value: str | None) -> str:
    return ATR_MODE_LABELS[normalize_atr_mode(value)]


def fixed_atr_points(value: str | None) -> float | None:
    return ATR_MODE_FIXED_POINTS.get(normalize_atr_mode(value))


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


def fixed_atr_price_value(atr_mode: str | None, point_size: float) -> float | None:
    points = fixed_atr_points(atr_mode)
    if points is None:
        return None
    return points * point_size
