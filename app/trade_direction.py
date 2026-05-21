from __future__ import annotations

DEFAULT_TRADE_DIRECTION = "BOTH"
VALID_TRADE_DIRECTIONS = {DEFAULT_TRADE_DIRECTION, "LONG", "SHORT"}

TRADE_DIRECTION_OPTIONS = [
    {"value": DEFAULT_TRADE_DIRECTION, "label": "Long + Short"},
    {"value": "LONG", "label": "Long uniquement"},
    {"value": "SHORT", "label": "Short uniquement"},
]


def normalize_trade_direction(value: str | None) -> str:
    if value in VALID_TRADE_DIRECTIONS:
        return value
    return DEFAULT_TRADE_DIRECTION



def expand_trade_directions(value: str | None) -> tuple[str, ...]:
    direction = normalize_trade_direction(value)
    if direction == DEFAULT_TRADE_DIRECTION:
        return ("LONG", "SHORT")
    return (direction,)



def trade_direction_label(value: str | None) -> str:
    direction = normalize_trade_direction(value)
    for option in TRADE_DIRECTION_OPTIONS:
        if option["value"] == direction:
            return option["label"]
    return TRADE_DIRECTION_OPTIONS[0]["label"]
