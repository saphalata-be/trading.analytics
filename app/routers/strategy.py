"""
Router for the Grid Averaging Strategy backtest page.

Strategy rules (per cycle, direction = LONG or SHORT):
  - Entry at cycle start price P0, size = 1 unit each level
  - TP : cumulative profit of all open positions >= tp_atr * ATR50 (default 0.5)
      => for LONG  : sum_i(close - entry_i) >= tp_atr * ATR50
      => for SHORT : sum_i(entry_i - close) >= tp_atr * ATR50
  - New level added when price moves adversely >= level_atr * ATR50 (default 1.0)
    from the LAST level entry price (not from P0).
  - Cycles start every full hour on the 1min data.
  - ATR50 = average of (high-low) over the 50 daily candles strictly
    before the cycle start date.
  - Max levels cap is user-configurable.
  - Open cycles at end of data are kept and flagged as incomplete.
"""
from __future__ import annotations

import asyncio
import json
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.database import get_connection, get_cache_connection

# In-memory job store: job_id -> {"queue": asyncio.Queue, "result": dict | None}
_jobs: dict[str, dict] = {}
_STRATEGY_CACHE_VERSION = 3

_DASHBOARD_DIRECTIONS = [
    {"value": "BOTH", "label": "Long + Short"},
    {"value": "LONG", "label": "Long uniquement"},
    {"value": "SHORT", "label": "Short uniquement"},
]

_DASHBOARD_METRICS = [
    {
        "value": "profit_total",
        "label": "Profit total",
        "field": "total_profit_atr",
        "ascending": False,
        "suffix": "ATR",
    },
    {
        "value": "success_rate",
        "label": "Taux de réussite",
        "field": "success_rate",
        "ascending": False,
        "suffix": "%",
    },
    {
        "value": "avg_duration",
        "label": "Durée moyenne",
        "field": "avg_duration_all",
        "ascending": True,
        "suffix": "duration",
    },
    {
        "value": "avg_levels",
        "label": "Niveaux moyens",
        "field": "avg_levels_all",
        "ascending": True,
        "suffix": "levels",
    },
]

router = APIRouter(prefix="/strategy", tags=["strategy"])
templates = Jinja2Templates(directory="app/templates")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compute_atr50(con, symbol: str, exchange: str, before_dt: datetime) -> Optional[float]:
    """Return ATR50 (mean of daily high-low) from the 50 daily candles before before_dt."""
    rows = con.execute(
        """
        SELECT high, low
        FROM ohlcv
        WHERE symbol = ? AND exchange = ? AND timeframe = '1day'
          AND CAST(datetime AS DATE) < CAST(? AS DATE)
        ORDER BY datetime DESC
        LIMIT 50
        """,
        [symbol, exchange, before_dt],
    ).fetchall()
    if len(rows) < 50:
        return None
    return sum(h - l for h, l in rows) / len(rows)


def _simulate_cycles(
    symbol: str,
    exchange: str,
    max_levels: int,
    tp_atr: float,
    level_atr: float,
    con,
) -> list[dict]:
    """Run the full simulation and return a list of cycle result dicts."""
    # Fetch all 1min bars ordered chronologically
    bars = con.execute(
        """
        SELECT datetime, open, high, low, close
        FROM ohlcv
        WHERE symbol = ? AND exchange = ? AND timeframe = '1min'
        ORDER BY datetime ASC
        """,
        [symbol, exchange],
    ).fetchall()

    if not bars:
        return []

    # Identify all full-hour bar indices
    hourly_indices: list[int] = [
        i for i, b in enumerate(bars) if b[0].minute == 0
    ]

    # Cache daily ATR50 by date (to avoid recomputing for same date)
    atr_cache: dict[str, Optional[float]] = {}

    results: list[dict] = []

    for start_idx in hourly_indices:
        start_bar = bars[start_idx]
        start_dt: datetime = start_bar[0]
        start_price: float = start_bar[1]  # open of the start bar

        # ATR50 from daily data before start_dt
        date_key = start_dt.date().isoformat()
        if date_key not in atr_cache:
            atr_cache[date_key] = _compute_atr50(con, symbol, exchange, start_dt)
        atr50 = atr_cache[date_key]
        if atr50 is None or atr50 == 0:
            continue

        # Simulate LONG and SHORT cycles from this start point
        for direction in ("LONG", "SHORT"):
            cycle = _run_cycle(
                bars=bars,
                start_idx=start_idx,
                start_price=start_price,
                direction=direction,
                atr50=atr50,
                max_levels=max_levels,
                tp_atr=tp_atr,
                level_atr=level_atr,
            )
            cycle["symbol"] = symbol
            cycle["exchange"] = exchange
            cycle["start_dt"] = start_dt
            cycle["atr50"] = atr50
            cycle["direction"] = direction
            results.append(cycle)

    return results


def _run_cycle(
    bars: list,
    start_idx: int,
    start_price: float,
    direction: str,  # "LONG" or "SHORT"
    atr50: float,
    max_levels: int,
    tp_atr: float = 0.5,
    level_atr: float = 1.0,
) -> dict:
    """Simulate a single cycle. Returns result dict."""
    sign = 1 if direction == "LONG" else -1  # profit per bar = sign * (close - entry)

    # levels: list of entry prices
    levels: list[float] = [start_price]
    last_entry = start_price  # price of last level added

    completed = False
    closed_max_levels = False
    end_dt: Optional[datetime] = None
    end_idx: int = start_idx

    for i in range(start_idx, len(bars)):
        bar = bars[i]
        # Use the bar's high/low for threshold checks. If both TP and an adverse
        # trigger are reachable within the same bar, prefer the adverse path.
        # start_idx is included: entry is at its open, but the bar's range can
        # already trigger new levels or TP.
        bar_dt: datetime = bar[0]
        bar_high: float = bar[2]
        bar_low: float = bar[3]

        # Add ALL levels that this bar triggers adversely (a bar may skip several).
        # Adverse moves take priority over TP within the same bar.
        while True:
            if direction == "LONG":
                trigger = last_entry - level_atr * atr50
                adverse_hit = bar_low <= trigger
            else:
                trigger = last_entry + level_atr * atr50
                adverse_hit = bar_high >= trigger

            if not adverse_hit:
                break

            if len(levels) >= max_levels:
                # Max levels reached before the bar stopped.
                closed_max_levels = True
                end_dt = bar_dt
                end_idx = i
                break

            levels.append(trigger)
            last_entry = trigger

        if closed_max_levels:
            break

        # TP price (recalculated with all levels added this bar)
        # cum_profit = sum_i sign*(close - entry_i) = sign*n*close - sign*sum(entries)
        # = tp_atr*atr50  =>  close = (tp_atr*atr50 + sign*sum(entries)) / (sign*n)
        n = len(levels)
        sum_entries = sum(levels)
        tp_price = (tp_atr * atr50 / sign + sum_entries) / n  # valid for sign != 0

        if direction == "LONG":
            tp_hit = bar_high >= tp_price
        else:
            tp_hit = bar_low <= tp_price

        if tp_hit:
            completed = True
            end_dt = bar_dt
            end_idx = i
            break

        # Update last bar seen
        end_dt = bar_dt
        end_idx = i

    # Duration in minutes
    start_bar_dt: datetime = bars[start_idx][0]
    if end_dt is None:
        end_dt = start_bar_dt
    duration_minutes = int((end_dt - start_bar_dt).total_seconds() / 60)

    return {
        "max_levels": len(levels),
        "duration_minutes": duration_minutes,
        "completed": completed,
        "closed_max_levels": closed_max_levels,
    }


def _aggregate(cycles: list[dict], tp_atr: float = 0.5, level_atr: float = 1.0) -> dict:
    """Compute mean stats per direction, separating complete vs incomplete."""
    from collections import defaultdict

    buckets: dict[str, list[dict]] = defaultdict(list)
    for c in cycles:
        buckets[c["direction"]].append(c)

    stats = {}
    for direction, cycs in buckets.items():
        complete = [c for c in cycs if c["completed"]]
        maxlevel = [c for c in cycs if c["closed_max_levels"]]
        incomplete = [c for c in cycs if not c["completed"] and not c["closed_max_levels"]]
        total_closed = len(complete) + len(maxlevel)
        success_rate = len(complete) / total_closed * 100 if total_closed > 0 else None
        # Total profit in ATR units over closed cycles:
        #   completed cycles → +tp_atr ATR each (TP condition)
        #   max-levels cycles → −level_atr*n*(n+1)/2 ATR  (n equally-spaced levels, exit at n+1-th trigger)
        total_profit_atr = (
            len(complete) * tp_atr
            - sum(level_atr * c["max_levels"] * (c["max_levels"] + 1) / 2 for c in maxlevel)
        ) if (complete or maxlevel) else None

        stats[direction] = {
            "total": len(cycs),
            "completed": len(complete),
            "max_levels_closed": len(maxlevel),
            "incomplete": len(incomplete),
            "success_rate": success_rate,
            "total_profit_atr": total_profit_atr,
            "peak_levels_complete": max((c["max_levels"] for c in complete), default=None),
            "avg_levels_complete": (
                sum(c["max_levels"] for c in complete) / len(complete) if complete else None
            ),
            "avg_duration_complete": (
                sum(c["duration_minutes"] for c in complete) / len(complete) if complete else None
            ),
            "peak_levels_incomplete": max((c["max_levels"] for c in incomplete), default=None),
            "avg_levels_incomplete": (
                sum(c["max_levels"] for c in incomplete) / len(incomplete) if incomplete else None
            ),
            "avg_duration_incomplete": (
                sum(c["duration_minutes"] for c in incomplete) / len(incomplete) if incomplete else None
            ),
            "peak_levels_all": max((c["max_levels"] for c in cycs), default=None),
            "avg_levels_all": (
                sum(c["max_levels"] for c in cycs) / len(cycs) if cycs else None
            ),
            "avg_duration_all": (
                sum(c["duration_minutes"] for c in cycs) / len(cycs) if cycs else None
            ),
        }
    return stats


def _weighted_average(values: list[tuple[Optional[float], int]]) -> Optional[float]:
    total_weight = 0
    weighted_sum = 0.0
    for value, weight in values:
        if value is None or weight <= 0:
            continue
        weighted_sum += value * weight
        total_weight += weight
    if total_weight == 0:
        return None
    return weighted_sum / total_weight


def _dashboard_metric_spec(metric: str) -> dict:
    for spec in _DASHBOARD_METRICS:
        if spec["value"] == metric:
            return spec
    return _DASHBOARD_METRICS[0]


def _dashboard_direction_label(direction: str) -> str:
    for option in _DASHBOARD_DIRECTIONS:
        if option["value"] == direction:
            return option["label"]
    return _DASHBOARD_DIRECTIONS[0]["label"]


def _merge_direction_stats(stats_by_direction: dict, direction: str) -> dict | None:
    if direction in ("LONG", "SHORT"):
        stats = stats_by_direction.get(direction)
        if not stats:
            return None
        merged = dict(stats)
        merged["closed_total"] = (merged.get("completed") or 0) + (merged.get("max_levels_closed") or 0)
        return merged

    long_stats = stats_by_direction.get("LONG") or {}
    short_stats = stats_by_direction.get("SHORT") or {}

    total = (long_stats.get("total") or 0) + (short_stats.get("total") or 0)
    completed = (long_stats.get("completed") or 0) + (short_stats.get("completed") or 0)
    max_levels_closed = (long_stats.get("max_levels_closed") or 0) + (short_stats.get("max_levels_closed") or 0)
    incomplete = (long_stats.get("incomplete") or 0) + (short_stats.get("incomplete") or 0)
    closed_total = completed + max_levels_closed

    profit_values = [
        value
        for value in (
            long_stats.get("total_profit_atr"),
            short_stats.get("total_profit_atr"),
        )
        if value is not None
    ]

    return {
        "total": total,
        "completed": completed,
        "max_levels_closed": max_levels_closed,
        "incomplete": incomplete,
        "closed_total": closed_total,
        "success_rate": (completed / closed_total * 100) if closed_total else None,
        "total_profit_atr": sum(profit_values) if profit_values else None,
        "peak_levels_complete": max(
            value
            for value in (
                long_stats.get("peak_levels_complete"),
                short_stats.get("peak_levels_complete"),
            )
            if value is not None
        ) if any(
            value is not None
            for value in (
                long_stats.get("peak_levels_complete"),
                short_stats.get("peak_levels_complete"),
            )
        ) else None,
        "avg_levels_complete": _weighted_average([
            (long_stats.get("avg_levels_complete"), long_stats.get("completed") or 0),
            (short_stats.get("avg_levels_complete"), short_stats.get("completed") or 0),
        ]),
        "avg_duration_complete": _weighted_average([
            (long_stats.get("avg_duration_complete"), long_stats.get("completed") or 0),
            (short_stats.get("avg_duration_complete"), short_stats.get("completed") or 0),
        ]),
        "peak_levels_incomplete": max(
            value
            for value in (
                long_stats.get("peak_levels_incomplete"),
                short_stats.get("peak_levels_incomplete"),
            )
            if value is not None
        ) if any(
            value is not None
            for value in (
                long_stats.get("peak_levels_incomplete"),
                short_stats.get("peak_levels_incomplete"),
            )
        ) else None,
        "avg_levels_incomplete": _weighted_average([
            (long_stats.get("avg_levels_incomplete"), long_stats.get("incomplete") or 0),
            (short_stats.get("avg_levels_incomplete"), short_stats.get("incomplete") or 0),
        ]),
        "avg_duration_incomplete": _weighted_average([
            (long_stats.get("avg_duration_incomplete"), long_stats.get("incomplete") or 0),
            (short_stats.get("avg_duration_incomplete"), short_stats.get("incomplete") or 0),
        ]),
        "peak_levels_all": max(
            value
            for value in (
                long_stats.get("peak_levels_all"),
                short_stats.get("peak_levels_all"),
            )
            if value is not None
        ) if any(
            value is not None
            for value in (
                long_stats.get("peak_levels_all"),
                short_stats.get("peak_levels_all"),
            )
        ) else None,
        "avg_levels_all": _weighted_average([
            (long_stats.get("avg_levels_all"), long_stats.get("total") or 0),
            (short_stats.get("avg_levels_all"), short_stats.get("total") or 0),
        ]),
        "avg_duration_all": _weighted_average([
            (long_stats.get("avg_duration_all"), long_stats.get("total") or 0),
            (short_stats.get("avg_duration_all"), short_stats.get("total") or 0),
        ]),
    }


def _dashboard_sort_rows(rows: list[dict], metric_field: str, ascending: bool) -> list[dict]:
    def _sort_key(row: dict) -> tuple[int, float]:
        value = row.get(metric_field)
        if value is None:
            return (1, 0.0)
        return (0, value if ascending else -value)

    return sorted(rows, key=_sort_key)


def _dashboard_is_better(candidate: Optional[float], current: Optional[float], ascending: bool) -> bool:
    if candidate is None:
        return False
    if current is None:
        return True
    return candidate < current if ascending else candidate > current


def _dashboard_combo_matches(
    row: dict,
    focus_max_levels: int | None,
    focus_tp_atr: float | None,
    focus_level_atr: float | None,
) -> bool:
    if focus_max_levels is not None and row["max_levels"] != focus_max_levels:
        return False
    if focus_tp_atr is not None and not math.isclose(row["tp_atr"], focus_tp_atr, rel_tol=1e-9, abs_tol=1e-9):
        return False
    if focus_level_atr is not None and not math.isclose(row["level_atr"], focus_level_atr, rel_tol=1e-9, abs_tol=1e-9):
        return False
    return True


def _build_strategy_dashboard(
    direction: str,
    metric: str,
    focus_max_levels: int | None,
    focus_tp_atr: float | None,
    focus_level_atr: float | None,
) -> dict:
    metric_spec = _dashboard_metric_spec(metric)
    direction = direction if direction in {"BOTH", "LONG", "SHORT"} else "BOTH"

    con = get_cache_connection()
    try:
        rows = con.execute(
            """
            SELECT symbol, exchange, max_levels, tp_atr, level_atr, computed_at, result_json
            FROM strategy_cache
            ORDER BY computed_at DESC
            """
        ).fetchall()
    finally:
        con.close()

    entries: list[dict] = []
    instruments_seen: set[tuple[str, str]] = set()
    combo_seen: set[tuple[int, float, float]] = set()
    max_levels_values: set[int] = set()
    tp_atr_values: set[float] = set()
    level_atr_values: set[float] = set()
    latest_computed_at = None

    for symbol, exchange, max_levels, tp_atr, level_atr, computed_at, result_json in rows:
        try:
            payload = json.loads(result_json)
        except json.JSONDecodeError:
            continue

        if payload.get("cache_version") != _STRATEGY_CACHE_VERSION:
            continue
        if payload.get("error") or payload.get("results") is None:
            continue

        stats = _merge_direction_stats(payload["results"], direction)
        if not stats or (stats.get("total") or 0) == 0:
            continue

        metric_value = stats.get(metric_spec["field"])
        entry = {
            "symbol": symbol,
            "exchange": exchange,
            "instrument_key": f"{symbol}|{exchange}",
            "max_levels": max_levels,
            "tp_atr": tp_atr,
            "level_atr": level_atr,
            "combo_key": f"{max_levels}|{tp_atr}|{level_atr}",
            "computed_at": computed_at,
            "direction_cycles": stats.get("total") or 0,
            "overall_cycles": payload.get("total_cycles") or 0,
            "stats": stats,
            "metric_value": metric_value,
            metric_spec["field"]: metric_value,
        }
        entries.append(entry)

        instruments_seen.add((symbol, exchange))
        combo_seen.add((max_levels, tp_atr, level_atr))
        max_levels_values.add(max_levels)
        tp_atr_values.add(tp_atr)
        level_atr_values.add(level_atr)
        if latest_computed_at is None or computed_at > latest_computed_at:
            latest_computed_at = computed_at

    combo_map: dict[tuple[int, float, float], dict] = {}
    for entry in entries:
        key = (entry["max_levels"], entry["tp_atr"], entry["level_atr"])
        stats = entry["stats"]
        combo = combo_map.setdefault(
            key,
            {
                "max_levels": entry["max_levels"],
                "tp_atr": entry["tp_atr"],
                "level_atr": entry["level_atr"],
                "instruments_count": 0,
                "direction_cycles": 0,
                "overall_cycles": 0,
                "completed": 0,
                "max_levels_closed": 0,
                "incomplete": 0,
                "positive_instruments": 0,
                "negative_instruments": 0,
                "neutral_instruments": 0,
                "total_profit_atr": 0.0,
                "profit_count": 0,
                "avg_levels_num": 0.0,
                "avg_levels_den": 0,
                "avg_duration_num": 0.0,
                "avg_duration_den": 0,
                "latest_computed_at": entry["computed_at"],
            },
        )
        combo["instruments_count"] += 1
        combo["direction_cycles"] += entry["direction_cycles"]
        combo["overall_cycles"] += entry["overall_cycles"]
        combo["completed"] += stats.get("completed") or 0
        combo["max_levels_closed"] += stats.get("max_levels_closed") or 0
        combo["incomplete"] += stats.get("incomplete") or 0

        total_profit = stats.get("total_profit_atr")
        if total_profit is not None:
            combo["total_profit_atr"] += total_profit
            combo["profit_count"] += 1
            if total_profit > 0:
                combo["positive_instruments"] += 1
            elif total_profit < 0:
                combo["negative_instruments"] += 1
            else:
                combo["neutral_instruments"] += 1

        if stats.get("avg_levels_all") is not None and entry["direction_cycles"] > 0:
            combo["avg_levels_num"] += stats["avg_levels_all"] * entry["direction_cycles"]
            combo["avg_levels_den"] += entry["direction_cycles"]
        if stats.get("avg_duration_all") is not None and entry["direction_cycles"] > 0:
            combo["avg_duration_num"] += stats["avg_duration_all"] * entry["direction_cycles"]
            combo["avg_duration_den"] += entry["direction_cycles"]
        if entry["computed_at"] > combo["latest_computed_at"]:
            combo["latest_computed_at"] = entry["computed_at"]

    leaderboard: list[dict] = []
    for combo in combo_map.values():
        closed_total = combo["completed"] + combo["max_levels_closed"]
        row = {
            **combo,
            "success_rate": (combo["completed"] / closed_total * 100) if closed_total else None,
            "avg_levels_all": (
                combo["avg_levels_num"] / combo["avg_levels_den"]
                if combo["avg_levels_den"]
                else None
            ),
            "avg_duration_all": (
                combo["avg_duration_num"] / combo["avg_duration_den"]
                if combo["avg_duration_den"]
                else None
            ),
            "avg_profit_per_instrument": (
                combo["total_profit_atr"] / combo["profit_count"]
                if combo["profit_count"]
                else None
            ),
            "positive_share": (
                combo["positive_instruments"] / combo["profit_count"] * 100
                if combo["profit_count"]
                else None
            ),
        }
        row[metric_spec["field"]] = row.get(metric_spec["field"])
        leaderboard.append(row)

    leaderboard = _dashboard_sort_rows(leaderboard, metric_spec["field"], metric_spec["ascending"])

    focus_combo = None
    if leaderboard:
        for row in leaderboard:
            if _dashboard_combo_matches(row, focus_max_levels, focus_tp_atr, focus_level_atr):
                focus_combo = row
                break
        if focus_combo is None:
            focus_combo = leaderboard[0]

    focus_instruments: list[dict] = []
    if focus_combo is not None:
        focus_instruments = [
            entry
            for entry in entries
            if entry["max_levels"] == focus_combo["max_levels"]
            and math.isclose(entry["tp_atr"], focus_combo["tp_atr"], rel_tol=1e-9, abs_tol=1e-9)
            and math.isclose(entry["level_atr"], focus_combo["level_atr"], rel_tol=1e-9, abs_tol=1e-9)
        ]
        focus_instruments = _dashboard_sort_rows(
            focus_instruments,
            metric_spec["field"],
            metric_spec["ascending"],
        )

    best_by_instrument: dict[str, dict] = {}
    for entry in entries:
        current = best_by_instrument.get(entry["instrument_key"])
        if current is None or _dashboard_is_better(
            entry["metric_value"], current["metric_value"], metric_spec["ascending"]
        ):
            best_by_instrument[entry["instrument_key"]] = entry

    instrument_best = _dashboard_sort_rows(
        list(best_by_instrument.values()),
        metric_spec["field"],
        metric_spec["ascending"],
    )

    return {
        "direction": direction,
        "direction_label": _dashboard_direction_label(direction),
        "metric": metric_spec["value"],
        "metric_label": metric_spec["label"],
        "metric_field": metric_spec["field"],
        "metric_suffix": metric_spec["suffix"],
        "metric_ascending": metric_spec["ascending"],
        "direction_options": _DASHBOARD_DIRECTIONS,
        "metric_options": _DASHBOARD_METRICS,
        "summary": {
            "entries_count": len(entries),
            "instrument_count": len(instruments_seen),
            "combo_count": len(combo_seen),
            "latest_computed_at": latest_computed_at,
            "max_levels_values": sorted(max_levels_values),
            "tp_atr_values": sorted(tp_atr_values),
            "level_atr_values": sorted(level_atr_values),
        },
        "leaderboard": leaderboard[:25],
        "focus_combo": focus_combo,
        "focus_instruments": focus_instruments,
        "instrument_best": instrument_best[:40],
    }


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def _load_cache(
    symbol: str, exchange: str, max_levels: int, tp_atr: float, level_atr: float
) -> dict | None:
    """Return cached result dict (with from_cache/cached_at keys) or None."""
    con = get_cache_connection()
    try:
        row = con.execute(
            """
            SELECT result_json, computed_at FROM strategy_cache
            WHERE symbol=? AND exchange=? AND max_levels=? AND tp_atr=? AND level_atr=?
            """,
            [symbol, exchange, max_levels, tp_atr, level_atr],
        ).fetchone()
    finally:
        con.close()
    if row is None:
        return None
    result = json.loads(row[0])
    if result.get("cache_version") != _STRATEGY_CACHE_VERSION:
        return None
    result["from_cache"] = True
    result["cached_at"] = row[1]
    return result


def _get_mt5_swap_lookup() -> dict[str, dict[str, float | None]]:
    con = get_cache_connection()
    try:
        rows = con.execute(
            "SELECT name, swap_long, swap_short FROM mt5_symbols"
        ).fetchall()
    finally:
        con.close()

    return {
        str(name): {
            "swap_long": swap_long,
            "swap_short": swap_short,
        }
        for name, swap_long, swap_short in rows
    }


def _enrich_all_results_with_mt5_swaps(payload: dict) -> dict:
    all_results = payload.get("all_results")
    if not all_results:
        return payload

    swap_lookup = _get_mt5_swap_lookup()
    enriched_results: dict[str, dict] = {}

    for key, item in all_results.items():
        enriched_item = dict(item)
        swap_data = swap_lookup.get(item["symbol"], {})
        enriched_item["swap_long"] = swap_data.get("swap_long")
        enriched_item["swap_short"] = swap_data.get("swap_short")
        enriched_results[key] = enriched_item

    enriched_payload = dict(payload)
    enriched_payload["all_results"] = enriched_results
    return enriched_payload


def _load_all_instruments_from_cache(
    instruments: list[dict],
    max_levels: int,
    tp_atr: float,
    level_atr: float,
) -> dict | None:
    """Rebuild the aggregate view directly from per-instrument cache entries."""
    all_results: dict[str, dict] = {}
    total_cycles = 0
    latest_cached_at = None

    for inst in instruments:
        cached = _load_cache(inst["symbol"], inst["exchange"], max_levels, tp_atr, level_atr)
        if cached is None or cached.get("error") or cached.get("results") is None:
            return None

        all_results[f"{inst['symbol']}|{inst['exchange']}"] = {
            "symbol": inst["symbol"],
            "exchange": inst["exchange"],
            "stats": cached["results"],
            "total_cycles": cached.get("total_cycles", 0),
        }
        total_cycles += cached.get("total_cycles", 0)

        cached_at = cached.get("cached_at")
        if cached_at is not None and (latest_cached_at is None or cached_at > latest_cached_at):
            latest_cached_at = cached_at

    return {
        "all_results": all_results,
        "results": None,
        "error": None if all_results else "Aucune donnée 1min disponible.",
        "selected_symbol": "__ALL__",
        "selected_exchange": "",
        "max_levels": max_levels,
        "tp_atr": tp_atr,
        "level_atr": level_atr,
        "total_cycles": total_cycles,
        "from_cache": True,
        "cached_at": latest_cached_at,
    }


def _save_cache(
    symbol: str, exchange: str, max_levels: int, tp_atr: float, level_atr: float, result: dict
) -> None:
    """Persist result to strategy_cache, replacing any existing entry."""
    to_store = {
        k: v
        for k, v in result.items()
        if k not in ("from_cache", "cached_at")
    }
    to_store["cache_version"] = _STRATEGY_CACHE_VERSION
    result_json = json.dumps(to_store)
    con = get_cache_connection()
    try:
        con.execute(
            "DELETE FROM strategy_cache WHERE symbol=? AND exchange=? AND max_levels=? AND tp_atr=? AND level_atr=?",
            [symbol, exchange, max_levels, tp_atr, level_atr],
        )
        con.execute(
            """
            INSERT INTO strategy_cache
                (symbol, exchange, max_levels, tp_atr, level_atr, computed_at, result_json)
            VALUES (?, ?, ?, ?, ?, current_timestamp, ?)
            """,
            [symbol, exchange, max_levels, tp_atr, level_atr, result_json],
        )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Background simulation helpers
# ---------------------------------------------------------------------------


def _simulate_cycles_new_conn(symbol: str, exchange: str, max_levels: int, tp_atr: float, level_atr: float) -> list[dict]:
    """Thread-safe wrapper: opens its own DuckDB connection."""
    con = get_connection()
    try:
        return _simulate_cycles(symbol, exchange, max_levels, tp_atr, level_atr, con)
    finally:
        con.close()


async def _run_simulation_job(
    job_id: str,
    symbol: str,
    exchange: str,
    max_levels: int,
    tp_atr: float,
    level_atr: float,
    instruments: list[dict],
) -> None:
    """Background task: runs simulation and pushes SSE events to the job queue."""
    q: asyncio.Queue = _jobs[job_id]["queue"]
    loop = asyncio.get_running_loop()
    try:
        if symbol == "__ALL__":
            all_results: dict[str, dict] = {}
            total_cycles_all = 0
            total = len(instruments)
            for idx, inst in enumerate(instruments):
                cached = _load_cache(inst["symbol"], inst["exchange"], max_levels, tp_atr, level_atr)
                if cached is not None and not cached.get("error") and cached.get("results") is not None:
                    all_results[f"{inst['symbol']}|{inst['exchange']}"] = {
                        "symbol": inst["symbol"],
                        "exchange": inst["exchange"],
                        "stats": cached["results"],
                        "total_cycles": cached.get("total_cycles", 0),
                    }
                    total_cycles_all += cached.get("total_cycles", 0)
                    await q.put({
                        "type": "progress",
                        "current": idx + 1,
                        "total": total,
                        "label": inst["symbol"],
                    })
                    continue

                await q.put({
                    "type": "progress",
                    "current": idx + 1,
                    "total": total,
                    "label": inst["symbol"],
                })
                cycles = await loop.run_in_executor(
                    None, _simulate_cycles_new_conn, inst["symbol"], inst["exchange"], max_levels, tp_atr, level_atr
                )
                if cycles:
                    instrument_result = {
                        "all_results": None,
                        "results": _aggregate(cycles, tp_atr, level_atr),
                        "error": None,
                        "selected_symbol": inst["symbol"],
                        "selected_exchange": inst["exchange"],
                        "max_levels": max_levels,
                        "tp_atr": tp_atr,
                        "level_atr": level_atr,
                        "total_cycles": len(cycles),
                    }
                    try:
                        _save_cache(inst["symbol"], inst["exchange"], max_levels, tp_atr, level_atr, instrument_result)
                    except Exception:  # noqa: BLE001
                        pass

                    all_results[f"{inst['symbol']}|{inst['exchange']}"] = {
                        "symbol": inst["symbol"],
                        "exchange": inst["exchange"],
                        "stats": instrument_result["results"],
                        "total_cycles": len(cycles),
                    }
                    total_cycles_all += len(cycles)
            _jobs[job_id]["result"] = {
                "all_results": all_results,
                "results": None,
                "error": None if all_results else "Aucune donnée 1min disponible.",
                "selected_symbol": "__ALL__",
                "selected_exchange": "",
                "max_levels": max_levels,
                "tp_atr": tp_atr,
                "level_atr": level_atr,
                "total_cycles": total_cycles_all,
            }
        else:
            await q.put({"type": "progress", "current": 0, "total": 1, "label": symbol})
            cycles = await loop.run_in_executor(
                None, _simulate_cycles_new_conn, symbol, exchange, max_levels, tp_atr, level_atr
            )
            await q.put({"type": "progress", "current": 1, "total": 1, "label": symbol})
            if not cycles:
                _jobs[job_id]["result"] = {
                    "all_results": None,
                    "results": None,
                    "error": f"Aucune donnée 1min disponible pour {symbol} ({exchange}).",
                    "selected_symbol": symbol,
                    "selected_exchange": exchange,
                    "max_levels": max_levels,
                    "tp_atr": tp_atr,
                    "level_atr": level_atr,
                }
            else:
                _jobs[job_id]["result"] = {
                    "all_results": None,
                    "results": _aggregate(cycles, tp_atr, level_atr),
                    "error": None,
                    "selected_symbol": symbol,
                    "selected_exchange": exchange,
                    "max_levels": max_levels,
                    "tp_atr": tp_atr,
                    "level_atr": level_atr,
                    "total_cycles": len(cycles),
                }
    except Exception as exc:  # noqa: BLE001
        _jobs[job_id]["result"] = {
            "all_results": None,
            "results": None,
            "error": f"Erreur de simulation : {exc}",
        }
    finally:
        result = _jobs[job_id].get("result")
        if result and not result.get("error") and symbol != "__ALL__":
            try:
                _save_cache(symbol, exchange, max_levels, tp_atr, level_atr, result)
            except Exception:  # noqa: BLE001
                pass
        await q.put({"type": "done", "url": f"/strategy/result/{job_id}"})


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/", response_class=HTMLResponse)
async def strategy_index(request: Request):
    instruments = _get_instruments()
    return templates.TemplateResponse(
        "strategy.html",
        {
            "request": request,
            "instruments": instruments,
            "results": None,
            "error": None,
        },
    )


@router.get("/dashboard", response_class=HTMLResponse)
async def strategy_dashboard(
    request: Request,
    direction: str = "BOTH",
    metric: str = "profit_total",
    focus_max_levels: int | None = None,
    focus_tp_atr: float | None = None,
    focus_level_atr: float | None = None,
):
    return templates.TemplateResponse(
        "strategy_dashboard.html",
        {
            "request": request,
            **_build_strategy_dashboard(
                direction,
                metric,
                focus_max_levels,
                focus_tp_atr,
                focus_level_atr,
            ),
        },
    )


@router.post("/run", response_class=HTMLResponse)
async def strategy_run(
    request: Request,
    symbol: str = Form(...),
    exchange: str = Form(...),
    max_levels: int = Form(10),
    tp_atr: float = Form(0.5),
    level_atr: float = Form(1.0),
    force: bool = Form(False),
):
    if max_levels < 1:
        max_levels = 1
    if tp_atr <= 0:
        tp_atr = 0.5
    if level_atr <= 0:
        level_atr = 1.0

    instruments = _get_instruments()

    if not force:
        if symbol == "__ALL__":
            cached = _load_all_instruments_from_cache(instruments, max_levels, tp_atr, level_atr)
        else:
            cached = _load_cache(symbol, exchange, max_levels, tp_atr, level_atr)
        if cached is not None:
            cached = _enrich_all_results_with_mt5_swaps(cached)
            return templates.TemplateResponse(
                "strategy.html",
                {"request": request, "instruments": instruments, **cached},
            )

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"queue": asyncio.Queue(), "result": None}
    asyncio.create_task(_run_simulation_job(job_id, symbol, exchange, max_levels, tp_atr, level_atr, instruments))

    return templates.TemplateResponse(
        "strategy.html",
        {
            "request": request,
            "instruments": instruments,
            "results": None,
            "all_results": None,
            "error": None,
            "selected_symbol": symbol,
            "selected_exchange": exchange,
            "max_levels": max_levels,
            "tp_atr": tp_atr,
            "level_atr": level_atr,
            "job_id": job_id,
        },
    )


@router.get("/progress/{job_id}")
async def strategy_progress(job_id: str):
    """SSE endpoint that streams simulation progress events."""

    async def _not_found():
        yield 'data: {"type":"error","message":"Job introuvable"}\n\n'

    if job_id not in _jobs:
        return StreamingResponse(_not_found(), media_type="text/event-stream")

    async def generate():
        q: asyncio.Queue = _jobs[job_id]["queue"]
        while True:
            event = await q.get()
            yield f"data: {json.dumps(event)}\n\n"
            if event.get("type") == "done":
                break

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/result/{job_id}", response_class=HTMLResponse)
async def strategy_result(request: Request, job_id: str):
    """Serve final results once the background job has completed."""
    job = _jobs.pop(job_id, None)
    if not job or job["result"] is None:
        return templates.TemplateResponse(
            "strategy.html",
            {
                "request": request,
                "instruments": _get_instruments(),
                "results": None,
                "error": "Résultat introuvable ou expiré.",
            },
        )
    instruments = _get_instruments()
    result = _enrich_all_results_with_mt5_swaps(job["result"])
    return templates.TemplateResponse(
        "strategy.html",
        {"request": request, "instruments": instruments, **result},
    )


def _get_instruments() -> list[dict]:
    con = get_connection()
    try:
        rows = con.execute(
            """
            SELECT DISTINCT o.symbol, o.exchange, i.type
            FROM ohlcv o
            LEFT JOIN instruments i ON i.symbol = o.symbol AND i.exchange = o.exchange
            WHERE o.timeframe = '1min'
            ORDER BY o.symbol
            """
        ).fetchall()
    finally:
        con.close()
    return [{"symbol": r[0], "exchange": r[1], "type": r[2]} for r in rows]
