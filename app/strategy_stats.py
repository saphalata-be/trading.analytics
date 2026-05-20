from __future__ import annotations

from collections import defaultdict


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