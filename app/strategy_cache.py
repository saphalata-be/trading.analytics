from __future__ import annotations

from app.strategy_filters import (
    DEFAULT_ENTRY_FILTER_ID,
    ENTRY_FILTER_ADX_RANGE,
    entry_filter_label,
    normalize_entry_filter,
)
from app.strategy_atr import DEFAULT_ATR_MODE, atr_mode_label, normalize_atr_mode
from app.trade_direction import normalize_trade_direction

STRATEGY_CACHE_VERSION = 10
SUPPORTED_STRATEGY_CACHE_VERSIONS = {STRATEGY_CACHE_VERSION}

_PEAK_LEVEL_FIELDS = (
    "peak_levels_complete",
    "peak_levels_incomplete",
    "peak_levels_all",
)


def _normalize_direction_stats(stats: dict | None) -> dict | None:
    if not isinstance(stats, dict):
        return stats

    normalized = dict(stats)
    for field in _PEAK_LEVEL_FIELDS:
        normalized.setdefault(field, None)
    normalized.setdefault("level_reach", [])
    return normalized


def normalize_strategy_cache_payload(payload: dict | None) -> dict | None:
    if not isinstance(payload, dict):
        return None

    version = payload.get("cache_version")
    if version not in SUPPORTED_STRATEGY_CACHE_VERSIONS:
        return None

    normalized = dict(payload)
    normalized["cache_version"] = STRATEGY_CACHE_VERSION
    normalized["direction_mode"] = normalize_trade_direction(payload.get("direction_mode"))
    normalized["atr_mode"] = normalize_atr_mode(payload.get("atr_mode", DEFAULT_ATR_MODE))
    normalized["atr_mode_label"] = atr_mode_label(normalized["atr_mode"])
    entry_filter_id = payload.get("entry_filter_id", DEFAULT_ENTRY_FILTER_ID)
    first_filter_param = payload.get("initial_move_atr")
    second_filter_param = payload.get("initial_retrace_atr")
    if entry_filter_id == ENTRY_FILTER_ADX_RANGE:
        first_filter_param = payload.get("adx_max") or first_filter_param
        second_filter_param = payload.get("adx_period") or second_filter_param
    entry_filter = normalize_entry_filter(
        entry_filter_id,
        first_filter_param,
        second_filter_param,
    )
    normalized["entry_filter_id"] = entry_filter.filter_id
    normalized["initial_move_atr"] = entry_filter.initial_move_atr
    normalized["initial_retrace_atr"] = entry_filter.initial_retrace_atr
    normalized["adx_max"] = entry_filter.adx_max
    normalized["adx_period"] = entry_filter.adx_period
    normalized["entry_filter_label"] = entry_filter_label(entry_filter)

    results = normalized.get("results")
    if isinstance(results, dict):
        normalized["results"] = {
            direction: _normalize_direction_stats(stats)
            for direction, stats in results.items()
        }

    all_results = normalized.get("all_results")
    if isinstance(all_results, dict):
        normalized_all_results = {}
        for key, item in all_results.items():
            if isinstance(item, dict):
                normalized_item = dict(item)
                normalized_item["stats"] = {
                    direction: _normalize_direction_stats(stats)
                    for direction, stats in (item.get("stats") or {}).items()
                }
                normalized_all_results[key] = normalized_item
            else:
                normalized_all_results[key] = item
        normalized["all_results"] = normalized_all_results

    return normalized
