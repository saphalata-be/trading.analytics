from __future__ import annotations

STRATEGY_CACHE_VERSION = 4
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