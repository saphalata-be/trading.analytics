from __future__ import annotations

from collections import defaultdict


_TRACKED_LEVEL_DAYS_LIMIT = 3
_TRACKED_LEVEL_DAYS_MAX_RATE = 25.0


def _build_level_reach_stats(direction_cycles: list[dict]) -> list[dict]:
    total_cycles = len(direction_cycles)
    if total_cycles == 0:
        return []

    peak_level = max((cycle["max_levels"] for cycle in direction_cycles), default=0)
    if peak_level <= 0:
        return []

    level_hits: dict[int, int] = defaultdict(int)
    for cycle in direction_cycles:
        max_level = cycle.get("max_levels") or 0
        for level in range(1, max_level + 1):
            level_hits[level] += 1

    tracked_levels = [
        level
        for level in range(peak_level, 0, -1)
        if level_hits[level] < total_cycles
        and (level_hits[level] / total_cycles * 100) <= _TRACKED_LEVEL_DAYS_MAX_RATE
    ][:_TRACKED_LEVEL_DAYS_LIMIT]

    tracked_days: dict[int, set[str]] = {level: set() for level in tracked_levels}
    if tracked_days:
        for cycle in direction_cycles:
            reached_day = cycle.get("start_day")
            if reached_day is None:
                start_dt = cycle.get("start_dt")
                if start_dt is not None:
                    reached_day = start_dt.date().isoformat()
            if reached_day is None:
                continue
            max_level = cycle.get("max_levels") or 0
            for level in tracked_levels:
                if max_level >= level:
                    tracked_days[level].add(reached_day)

    return [
        {
            "level": level,
            "hits": level_hits[level],
            "hit_rate": level_hits[level] / total_cycles * 100,
            "reached_days": sorted(tracked_days.get(level, ())),
        }
        for level in range(peak_level, 0, -1)
    ]


def aggregate_cycles(cycles: list[dict], tp_atr: float = 0.5, level_atr: float = 1.0) -> dict:
    buckets: dict[str, list[dict]] = defaultdict(list)
    for cycle in cycles:
        buckets[cycle["direction"]].append(cycle)

    stats = {}
    for direction, direction_cycles in buckets.items():
        complete = [cycle for cycle in direction_cycles if cycle["completed"]]
        maxlevel = [cycle for cycle in direction_cycles if cycle["closed_max_levels"]]
        incomplete = [
            cycle
            for cycle in direction_cycles
            if not cycle["completed"] and not cycle["closed_max_levels"]
        ]
        total_closed = len(complete) + len(maxlevel)
        success_rate = len(complete) / total_closed * 100 if total_closed > 0 else None
        total_profit_atr = (
            len(complete) * tp_atr
            - sum(level_atr * cycle["max_levels"] * (cycle["max_levels"] + 1) / 2 for cycle in maxlevel)
        ) if (complete or maxlevel) else None

        stats[direction] = {
            "total": len(direction_cycles),
            "completed": len(complete),
            "max_levels_closed": len(maxlevel),
            "incomplete": len(incomplete),
            "level_reach": _build_level_reach_stats(direction_cycles),
            "success_rate": success_rate,
            "total_profit_atr": total_profit_atr,
            "peak_levels_complete": max((cycle["max_levels"] for cycle in complete), default=None),
            "avg_levels_complete": (
                sum(cycle["max_levels"] for cycle in complete) / len(complete) if complete else None
            ),
            "avg_duration_complete": (
                sum(cycle["duration_minutes"] for cycle in complete) / len(complete) if complete else None
            ),
            "peak_levels_incomplete": max((cycle["max_levels"] for cycle in incomplete), default=None),
            "avg_levels_incomplete": (
                sum(cycle["max_levels"] for cycle in incomplete) / len(incomplete) if incomplete else None
            ),
            "avg_duration_incomplete": (
                sum(cycle["duration_minutes"] for cycle in incomplete) / len(incomplete)
                if incomplete else None
            ),
            "peak_levels_all": max((cycle["max_levels"] for cycle in direction_cycles), default=None),
            "avg_levels_all": (
                sum(cycle["max_levels"] for cycle in direction_cycles) / len(direction_cycles)
                if direction_cycles else None
            ),
            "avg_duration_all": (
                sum(cycle["duration_minutes"] for cycle in direction_cycles) / len(direction_cycles)
                if direction_cycles else None
            ),
        }
    return stats