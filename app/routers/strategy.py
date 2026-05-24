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
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.database import get_connection, get_cache_connection
from app.strategy_filters import (
    EntryFilterConfig,
    DEFAULT_ENTRY_FILTER_ID,
    DEFAULT_INITIAL_MOVE_ATR,
    DEFAULT_INITIAL_RETRACE_ATR,
    entry_filter_cache_parts,
    entry_filter_label,
    entry_filter_payload,
    entry_filter_uses_sequential_entries,
    find_entry_for_arrays,
    find_sequential_entries_for_arrays,
    normalize_entry_filter,
)
from app.strategy_cache import STRATEGY_CACHE_VERSION, normalize_strategy_cache_payload
from app.strategy_atr import (
    DEFAULT_ATR_MODE,
    atr_mode_label,
    fixed_atr_price_value,
    infer_point_size,
    normalize_atr_mode,
)
from app.strategy_stats import aggregate_cycles
from app.trade_direction import (
    DEFAULT_TRADE_DIRECTION,
    TRADE_DIRECTION_OPTIONS,
    expand_trade_directions,
    normalize_trade_direction,
    trade_direction_label,
)

# In-memory job store: job_id -> {"queue": asyncio.Queue, "result": dict | None}
_jobs: dict[str, dict] = {}

_DASHBOARD_DIRECTIONS = TRADE_DIRECTION_OPTIONS

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


def _compute_adx_points(rows: list, period: int) -> list[tuple[datetime, float]]:
    """Return ADX values keyed by the source bar datetime."""
    if period < 2 or len(rows) < period * 2:
        return []

    true_ranges: list[float] = []
    plus_dm: list[float] = []
    minus_dm: list[float] = []
    for i in range(1, len(rows)):
        _, high, low, close = rows[i]
        _, prev_high, prev_low, prev_close = rows[i - 1]
        up_move = high - prev_high
        down_move = prev_low - low
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)

    if len(true_ranges) < period:
        return []

    smoothed_tr = sum(true_ranges[:period])
    smoothed_plus = sum(plus_dm[:period])
    smoothed_minus = sum(minus_dm[:period])
    dx_values: list[float] = []
    adx: float | None = None
    adx_points: list[tuple[datetime, float]] = []

    for idx in range(period - 1, len(true_ranges)):
        if idx > period - 1:
            smoothed_tr = smoothed_tr - (smoothed_tr / period) + true_ranges[idx]
            smoothed_plus = smoothed_plus - (smoothed_plus / period) + plus_dm[idx]
            smoothed_minus = smoothed_minus - (smoothed_minus / period) + minus_dm[idx]

        if smoothed_tr <= 0:
            dx = 0.0
        else:
            plus_di = 100.0 * smoothed_plus / smoothed_tr
            minus_di = 100.0 * smoothed_minus / smoothed_tr
            di_sum = plus_di + minus_di
            dx = 0.0 if di_sum <= 0 else 100.0 * abs(plus_di - minus_di) / di_sum

        dx_values.append(dx)
        if len(dx_values) < period:
            continue
        if len(dx_values) == period:
            adx = sum(dx_values) / period
        else:
            adx = ((adx or 0.0) * (period - 1) + dx) / period
        adx_points.append((rows[idx + 1][0], adx))

    return adx_points


def _load_adx_by_minute_idx(
    con,
    symbol: str,
    exchange: str,
    bars: list,
    period: int,
) -> dict[int, float]:
    hour_rows = con.execute(
        """
        SELECT datetime, high, low, close
        FROM ohlcv
        WHERE symbol = ? AND exchange = ? AND timeframe = '1h'
        ORDER BY datetime ASC
        """,
        [symbol, exchange],
    ).fetchall()
    adx_points = _compute_adx_points(hour_rows, period)
    if not adx_points:
        return {}

    adx_by_idx: dict[int, float] = {}
    point_idx = -1
    one_hour = timedelta(hours=1)
    for bar_idx, bar in enumerate(bars):
        bar_dt: datetime = bar[0]
        while (
            point_idx + 1 < len(adx_points)
            and adx_points[point_idx + 1][0] + one_hour <= bar_dt
        ):
            point_idx += 1
        if point_idx >= 0:
            adx_by_idx[bar_idx] = adx_points[point_idx][1]
    return adx_by_idx


def _simulate_cycles(
    symbol: str,
    exchange: str,
    max_levels: int,
    tp_atr: float,
    level_atr: float,
    con,
    direction_mode: str = DEFAULT_TRADE_DIRECTION,
    entry_filter_config: EntryFilterConfig | None = None,
    atr_mode: str = DEFAULT_ATR_MODE,
) -> list[dict]:
    """Run the full simulation and return a list of cycle result dicts."""
    entry_filter_config = entry_filter_config or normalize_entry_filter()
    atr_mode = normalize_atr_mode(atr_mode)
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

    bars_open = [bar[1] for bar in bars]
    bars_high = [bar[2] for bar in bars]
    bars_low = [bar[3] for bar in bars]
    fixed_atr = fixed_atr_price_value(atr_mode, infer_point_size(symbol, bars_open + bars_high + bars_low))
    adx_by_idx: dict[int, float] | None = None
    if entry_filter_config.adx_period is not None:
        adx_by_idx = _load_adx_by_minute_idx(
            con,
            symbol,
            exchange,
            bars,
            entry_filter_config.adx_period,
        )

    # Identify all full-hour bar indices
    hourly_indices: list[int] = [
        i for i, b in enumerate(bars) if b[0].minute == 0
    ]

    # Cache daily ATR50 by date (to avoid recomputing for same date)
    atr_cache: dict[str, Optional[float]] = {}

    results: list[dict] = []

    if entry_filter_uses_sequential_entries(entry_filter_config):
        atr50_by_idx: dict[int, float] = {}
        for idx, bar in enumerate(bars):
            bar_dt: datetime = bar[0]
            date_key = bar_dt.date().isoformat()
            if fixed_atr is not None:
                atr50 = fixed_atr
            elif date_key not in atr_cache:
                atr_cache[date_key] = _compute_atr50(con, symbol, exchange, bar_dt)
                atr50 = atr_cache[date_key]
            else:
                atr50 = atr_cache[date_key]
            if atr50 is not None and atr50 > 0:
                atr50_by_idx[idx] = atr50

        for direction in expand_trade_directions(direction_mode):
            for entry_idx, entry_price in find_sequential_entries_for_arrays(
                bars_open,
                bars_high,
                bars_low,
                atr50_by_idx,
                direction,
                entry_filter_config,
            ):
                atr50 = atr50_by_idx.get(entry_idx)
                if atr50 is None or atr50 <= 0:
                    continue
                cycle = _run_cycle(
                    bars=bars,
                    start_idx=entry_idx,
                    start_price=entry_price,
                    direction=direction,
                    atr50=atr50,
                    max_levels=max_levels,
                    tp_atr=tp_atr,
                    level_atr=level_atr,
                )
                cycle["symbol"] = symbol
                cycle["exchange"] = exchange
                cycle["start_dt"] = bars[entry_idx][0]
                cycle["signal_dt"] = bars[entry_idx][0]
                cycle["atr50"] = atr50
                cycle["direction"] = direction
                results.append(cycle)

        return results

    for start_idx in hourly_indices:
        start_bar = bars[start_idx]
        start_dt: datetime = start_bar[0]

        # ATR50 from daily data before start_dt
        date_key = start_dt.date().isoformat()
        if fixed_atr is not None:
            atr50 = fixed_atr
        elif date_key not in atr_cache:
            atr_cache[date_key] = _compute_atr50(con, symbol, exchange, start_dt)
            atr50 = atr_cache[date_key]
        else:
            atr50 = atr_cache[date_key]
        if atr50 is None or atr50 == 0:
            continue

        # Simulate LONG and SHORT cycles from this start point
        for direction in expand_trade_directions(direction_mode):
            entry = find_entry_for_arrays(
                bars_open,
                bars_high,
                bars_low,
                start_idx,
                atr50,
                direction,
                entry_filter_config,
                adx_by_idx,
            )
            if entry is None:
                continue
            entry_idx, entry_price = entry
            cycle = _run_cycle(
                bars=bars,
                start_idx=entry_idx,
                start_price=entry_price,
                direction=direction,
                atr50=atr50,
                max_levels=max_levels,
                tp_atr=tp_atr,
                level_atr=level_atr,
            )
            cycle["symbol"] = symbol
            cycle["exchange"] = exchange
            cycle["start_dt"] = bars[entry_idx][0]
            cycle["signal_dt"] = start_dt
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
    return aggregate_cycles(cycles, tp_atr, level_atr)


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


def _optional_float_matches(candidate: float | None, expected: float | None) -> bool:
    if candidate is None or expected is None:
        return candidate is None and expected is None
    return math.isclose(candidate, expected, rel_tol=1e-9, abs_tol=1e-9)


def _compute_historical_position_pct(
    historical_low: float | None,
    historical_high: float | None,
    current_price: float | None,
) -> float | None:
    if historical_low is None or historical_high is None or current_price is None:
        return None

    span = historical_high - historical_low
    if math.isclose(span, 0.0, abs_tol=1e-12):
        return None

    position_pct = (current_price - historical_low) / span * 100
    return max(0.0, min(100.0, position_pct))


def _dashboard_metric_spec(metric: str) -> dict:
    for spec in _DASHBOARD_METRICS:
        if spec["value"] == metric:
            return spec
    return _DASHBOARD_METRICS[0]


def _dashboard_direction_label(direction: str) -> str:
    return trade_direction_label(direction)


def _merge_level_reach_stats(long_stats: dict, short_stats: dict, total_cycles: int) -> list[dict]:
    if total_cycles <= 0:
        return []

    merged_levels: dict[int, dict] = {}
    for stats in (long_stats, short_stats):
        for level_stats in stats.get("level_reach") or []:
            level = level_stats.get("level")
            if level is None:
                continue
            merged_entry = merged_levels.setdefault(
                level,
                {"level": level, "hits": 0, "reached_days": set()},
            )
            merged_entry["hits"] += level_stats.get("hits") or 0
            merged_entry["reached_days"].update(level_stats.get("reached_days") or [])

    return [
        {
            "level": level,
            "hits": entry["hits"],
            "hit_rate": entry["hits"] / total_cycles * 100,
            "reached_days": sorted(entry["reached_days"]),
        }
        for level, entry in sorted(merged_levels.items(), reverse=True)
    ]


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
        "level_reach": _merge_level_reach_stats(long_stats, short_stats, total),
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
    focus_atr_mode: str | None,
    focus_trade_direction: str | None,
    focus_entry_filter_id: int | None,
    focus_initial_move_atr: float | None,
    focus_initial_retrace_atr: float | None,
) -> bool:
    if focus_max_levels is not None and row["max_levels"] != focus_max_levels:
        return False
    if focus_tp_atr is not None and not math.isclose(row["tp_atr"], focus_tp_atr, rel_tol=1e-9, abs_tol=1e-9):
        return False
    if focus_level_atr is not None and not math.isclose(row["level_atr"], focus_level_atr, rel_tol=1e-9, abs_tol=1e-9):
        return False
    if focus_atr_mode is not None and row["atr_mode"] != normalize_atr_mode(focus_atr_mode):
        return False
    if focus_trade_direction is not None and row["direction_mode"] != focus_trade_direction:
        return False
    if focus_entry_filter_id is not None and row["entry_filter_id"] != focus_entry_filter_id:
        return False
    if focus_initial_move_atr is not None and not _optional_float_matches(
        row["initial_move_atr"],
        focus_initial_move_atr,
    ):
        return False
    if focus_initial_retrace_atr is not None and not _optional_float_matches(
        row["initial_retrace_atr"],
        focus_initial_retrace_atr,
    ):
        return False
    return True


def _build_strategy_dashboard(
    direction: str,
    metric: str,
    focus_max_levels: int | None,
    focus_tp_atr: float | None,
    focus_level_atr: float | None,
    focus_atr_mode: str | None,
    focus_trade_direction: str | None,
    focus_entry_filter_id: int | None,
    focus_initial_move_atr: float | None,
    focus_initial_retrace_atr: float | None,
) -> dict:
    metric_spec = _dashboard_metric_spec(metric)
    direction = direction if direction in {"BOTH", "LONG", "SHORT"} else "BOTH"

    con = get_cache_connection()
    try:
        rows = con.execute(
            """
            SELECT
                symbol,
                exchange,
                max_levels,
                tp_atr,
                level_atr,
                atr_mode,
                entry_filter_id,
                initial_move_atr,
                initial_retrace_atr,
                computed_at,
                result_json
            FROM strategy_cache
            ORDER BY computed_at DESC
            """
        ).fetchall()
    finally:
        con.close()

    entries: list[dict] = []
    instruments_seen: set[tuple[str, str]] = set()
    combo_seen: set[tuple[int, float, float, str, str, int, float | None, float | None]] = set()
    max_levels_values: set[int] = set()
    tp_atr_values: set[float] = set()
    level_atr_values: set[float] = set()
    atr_mode_values: set[str] = set()
    entry_filter_values: set[str] = set()
    latest_computed_at = None

    for (
        symbol,
        exchange,
        max_levels,
        tp_atr,
        level_atr,
        atr_mode,
        entry_filter_id,
        initial_move_atr,
        initial_retrace_atr,
        computed_at,
        result_json,
    ) in rows:
        try:
            payload = normalize_strategy_cache_payload(json.loads(result_json))
        except json.JSONDecodeError:
            continue

        if payload is None:
            continue
        if payload.get("error") or payload.get("results") is None:
            continue

        direction_mode = normalize_trade_direction(payload.get("direction_mode"))
        atr_mode = normalize_atr_mode(payload.get("atr_mode", atr_mode))
        entry_filter_config = normalize_entry_filter(
            payload.get("entry_filter_id", entry_filter_id),
            payload.get("initial_move_atr", initial_move_atr),
            payload.get("initial_retrace_atr", initial_retrace_atr),
        )
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
            "atr_mode": atr_mode,
            "atr_mode_label": atr_mode_label(atr_mode),
            "entry_filter_id": entry_filter_config.filter_id,
            "initial_move_atr": entry_filter_config.initial_move_atr,
            "initial_retrace_atr": entry_filter_config.initial_retrace_atr,
            "entry_filter_label": entry_filter_label(entry_filter_config),
            "direction_mode": direction_mode,
            "direction_mode_label": trade_direction_label(direction_mode),
            "combo_key": (
                f"{max_levels}|{tp_atr}|{level_atr}|{atr_mode}|{direction_mode}|"
                f"{entry_filter_config.filter_id}|"
                f"{entry_filter_config.initial_move_atr}|{entry_filter_config.initial_retrace_atr}"
            ),
            "computed_at": computed_at,
            "direction_cycles": stats.get("total") or 0,
            "overall_cycles": payload.get("total_cycles") or 0,
            "stats": stats,
            "metric_value": metric_value,
            metric_spec["field"]: metric_value,
        }
        entries.append(entry)

        instruments_seen.add((symbol, exchange))
        combo_seen.add((
            max_levels,
            tp_atr,
            level_atr,
            atr_mode,
            direction_mode,
            entry_filter_config.filter_id,
            entry_filter_config.initial_move_atr,
            entry_filter_config.initial_retrace_atr,
        ))
        max_levels_values.add(max_levels)
        tp_atr_values.add(tp_atr)
        level_atr_values.add(level_atr)
        atr_mode_values.add(atr_mode)
        entry_filter_values.add(entry_filter_label(entry_filter_config))
        if latest_computed_at is None or computed_at > latest_computed_at:
            latest_computed_at = computed_at

    combo_map: dict[tuple[int, float, float, str, str, int, float | None, float | None], dict] = {}
    for entry in entries:
        key = (
            entry["max_levels"],
            entry["tp_atr"],
            entry["level_atr"],
            entry["atr_mode"],
            entry["direction_mode"],
            entry["entry_filter_id"],
            entry["initial_move_atr"],
            entry["initial_retrace_atr"],
        )
        stats = entry["stats"]
        combo = combo_map.setdefault(
            key,
            {
                "max_levels": entry["max_levels"],
                "tp_atr": entry["tp_atr"],
                "level_atr": entry["level_atr"],
                "atr_mode": entry["atr_mode"],
                "atr_mode_label": entry["atr_mode_label"],
                "entry_filter_id": entry["entry_filter_id"],
                "initial_move_atr": entry["initial_move_atr"],
                "initial_retrace_atr": entry["initial_retrace_atr"],
                "entry_filter_label": entry["entry_filter_label"],
                "direction_mode": entry["direction_mode"],
                "direction_mode_label": entry["direction_mode_label"],
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
        normalized_focus_trade_direction = (
            normalize_trade_direction(focus_trade_direction)
            if focus_trade_direction is not None
            else None
        )
        for row in leaderboard:
            if _dashboard_combo_matches(
                row,
                focus_max_levels,
                focus_tp_atr,
                focus_level_atr,
                focus_atr_mode,
                normalized_focus_trade_direction,
                focus_entry_filter_id,
                focus_initial_move_atr,
                focus_initial_retrace_atr,
            ):
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
            and entry["atr_mode"] == focus_combo["atr_mode"]
            and entry["direction_mode"] == focus_combo["direction_mode"]
            and entry["entry_filter_id"] == focus_combo["entry_filter_id"]
            and _optional_float_matches(entry["initial_move_atr"], focus_combo["initial_move_atr"])
            and _optional_float_matches(entry["initial_retrace_atr"], focus_combo["initial_retrace_atr"])
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
            "atr_mode_values": [atr_mode_label(value) for value in sorted(atr_mode_values)],
            "entry_filter_values": sorted(entry_filter_values),
            "trade_direction_values": [
                trade_direction_label(value)
                for value in sorted({entry["direction_mode"] for entry in entries})
            ],
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
    symbol: str,
    exchange: str,
    max_levels: int,
    tp_atr: float,
    level_atr: float,
    direction_mode: str,
    entry_filter_config: EntryFilterConfig | None = None,
    atr_mode: str = DEFAULT_ATR_MODE,
) -> dict | None:
    """Return cached result dict (with from_cache/cached_at keys) or None."""
    entry_filter_config = entry_filter_config or normalize_entry_filter()
    atr_mode = normalize_atr_mode(atr_mode)
    entry_filter_id, initial_move_atr, initial_retrace_atr = entry_filter_cache_parts(entry_filter_config)
    con = get_cache_connection()
    try:
        row = con.execute(
            """
            SELECT result_json, computed_at FROM strategy_cache
            WHERE symbol=?
              AND exchange=?
              AND max_levels=?
              AND tp_atr=?
              AND level_atr=?
              AND atr_mode=?
              AND entry_filter_id=?
              AND initial_move_atr=?
              AND initial_retrace_atr=?
            """,
            [
                symbol,
                exchange,
                max_levels,
                tp_atr,
                level_atr,
                atr_mode,
                entry_filter_id,
                initial_move_atr,
                initial_retrace_atr,
            ],
        ).fetchone()
    finally:
        con.close()
    if row is None:
        return None
    result = normalize_strategy_cache_payload(json.loads(row[0]))
    if result is None:
        return None
    if result.get("direction_mode") != normalize_trade_direction(direction_mode):
        return None
    if result.get("atr_mode") != atr_mode:
        return None
    if result.get("entry_filter_id") != entry_filter_config.filter_id:
        return None
    if not _optional_float_matches(result.get("initial_move_atr"), entry_filter_config.initial_move_atr):
        return None
    if not _optional_float_matches(result.get("initial_retrace_atr"), entry_filter_config.initial_retrace_atr):
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


def _get_historical_price_position_lookup(
    instruments: list[tuple[str, str]],
) -> dict[str, dict[str, float | None]]:
    if not instruments:
        return {}

    values_sql = ", ".join(["(?, ?)"] * len(instruments))
    params = [value for instrument in instruments for value in instrument]

    con = get_connection()
    try:
        rows = con.execute(
            f"""
            WITH requested(symbol, exchange) AS (
                VALUES {values_sql}
            )
            SELECT
                w.symbol,
                w.exchange,
                w.historical_low,
                w.historical_high,
                w.current_price,
                w.historical_position_pct,
                w.latest_atr50,
                w.atr50_eur_001_lot
            FROM watchlist w
            INNER JOIN requested r
                ON w.symbol = r.symbol
               AND w.exchange = r.exchange
            """,
            params,
        ).fetchall()
    finally:
        con.close()

    return {
        f"{symbol}|{exchange}": {
            "historical_low": historical_low,
            "historical_high": historical_high,
            "current_price": current_price,
            "historical_position_pct": historical_position_pct,
            "latest_atr50": latest_atr50,
            "atr50_eur_001_lot": atr50_eur_001_lot,
        }
        for symbol, exchange, historical_low, historical_high, current_price, historical_position_pct, latest_atr50, atr50_eur_001_lot in rows
    }


def _enrich_all_results_with_mt5_swaps(payload: dict) -> dict:
    all_results = payload.get("all_results")
    if not all_results:
        return payload

    swap_lookup = _get_mt5_swap_lookup()
    price_position_lookup = _get_historical_price_position_lookup(
        [(item["symbol"], item["exchange"]) for item in all_results.values()]
    )
    enriched_results: dict[str, dict] = {}

    for key, item in all_results.items():
        enriched_item = dict(item)
        swap_data = swap_lookup.get(item["symbol"], {})
        price_position = price_position_lookup.get(key, {})
        enriched_item["swap_long"] = swap_data.get("swap_long")
        enriched_item["swap_short"] = swap_data.get("swap_short")
        enriched_item["historical_low"] = price_position.get("historical_low")
        enriched_item["historical_high"] = price_position.get("historical_high")
        enriched_item["current_price"] = price_position.get("current_price")
        enriched_item["historical_position_pct"] = price_position.get("historical_position_pct")
        enriched_item["latest_atr50"] = price_position.get("latest_atr50")
        enriched_item["atr50_eur_001_lot"] = price_position.get("atr50_eur_001_lot")
        enriched_results[key] = enriched_item

    enriched_payload = dict(payload)
    enriched_payload["all_results"] = enriched_results
    return enriched_payload


def _load_all_instruments_from_cache(
    instruments: list[dict],
    max_levels: int,
    tp_atr: float,
    level_atr: float,
    entry_filter_config: EntryFilterConfig | None = None,
    atr_mode: str = DEFAULT_ATR_MODE,
) -> dict | None:
    """Rebuild the aggregate view directly from per-instrument cache entries."""
    entry_filter_config = entry_filter_config or normalize_entry_filter()
    atr_mode = normalize_atr_mode(atr_mode)
    all_results: dict[str, dict] = {}
    total_cycles = 0
    latest_cached_at = None

    for inst in instruments:
        direction_mode = inst.get("preferred_direction", DEFAULT_TRADE_DIRECTION)
        cached = _load_cache(
            inst["symbol"],
            inst["exchange"],
            max_levels,
            tp_atr,
            level_atr,
            direction_mode,
            entry_filter_config,
            atr_mode,
        )
        if cached is None or cached.get("error") or cached.get("results") is None:
            return None

        all_results[f"{inst['symbol']}|{inst['exchange']}"] = {
            "symbol": inst["symbol"],
            "exchange": inst["exchange"],
            "direction_mode": direction_mode,
            "direction_mode_label": trade_direction_label(direction_mode),
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
        "atr_mode": atr_mode,
        "atr_mode_label": atr_mode_label(atr_mode),
        **entry_filter_payload(entry_filter_config),
        "total_cycles": total_cycles,
        "from_cache": True,
        "cached_at": latest_cached_at,
    }


def _cache_selection_value(
    scope: str,
    symbol: str,
    exchange: str,
    max_levels: int,
    tp_atr: float,
    level_atr: float,
    atr_mode: str = DEFAULT_ATR_MODE,
    entry_filter_config: EntryFilterConfig | None = None,
) -> str:
    entry_filter_config = entry_filter_config or normalize_entry_filter()
    atr_mode = normalize_atr_mode(atr_mode)
    return "|".join(
        [
            scope,
            symbol,
            exchange,
            str(max_levels),
            f"{tp_atr:.12g}",
            f"{level_atr:.12g}",
            atr_mode,
            str(entry_filter_config.filter_id),
            (
                ""
                if entry_filter_config.initial_move_atr is None
                else f"{entry_filter_config.initial_move_atr:.12g}"
            ),
            (
                ""
                if entry_filter_config.initial_retrace_atr is None
                else f"{entry_filter_config.initial_retrace_atr:.12g}"
            ),
        ]
    )


def _build_cache_selection_options(instruments: list[dict]) -> list[dict]:
    con = get_cache_connection()
    try:
        rows = con.execute(
            """
            SELECT
                symbol,
                exchange,
                max_levels,
                tp_atr,
                level_atr,
                atr_mode,
                entry_filter_id,
                initial_move_atr,
                initial_retrace_atr,
                computed_at,
                result_json
            FROM strategy_cache
            ORDER BY computed_at DESC, symbol ASC, exchange ASC
            """
        ).fetchall()
    finally:
        con.close()

    combo_rows: dict[tuple[int, float, float, str, int, float | None, float | None], set[tuple[str, str, str]]] = {}
    combo_latest_at: dict[tuple[int, float, float, str, int, float | None, float | None], datetime] = {}
    combo_filters: dict[tuple[int, float, float, str, int, float | None, float | None], EntryFilterConfig] = {}
    required_combo_rows = {
        (
            inst["symbol"],
            inst["exchange"],
            inst.get("preferred_direction", DEFAULT_TRADE_DIRECTION),
        )
        for inst in instruments
    }

    for (
        symbol,
        exchange,
        max_levels,
        tp_atr,
        level_atr,
        atr_mode,
        entry_filter_id,
        initial_move_atr,
        initial_retrace_atr,
        computed_at,
        result_json,
    ) in rows:
        try:
            payload = normalize_strategy_cache_payload(json.loads(result_json))
        except json.JSONDecodeError:
            continue

        if payload is None or payload.get("error") or payload.get("results") is None:
            continue

        direction_mode = normalize_trade_direction(payload.get("direction_mode"))
        atr_mode = normalize_atr_mode(payload.get("atr_mode", atr_mode))
        entry_filter_config = normalize_entry_filter(
            payload.get("entry_filter_id", entry_filter_id),
            payload.get("initial_move_atr", initial_move_atr),
            payload.get("initial_retrace_atr", initial_retrace_atr),
        )
        combo_key = (
            max_levels,
            tp_atr,
            level_atr,
            atr_mode,
            entry_filter_config.filter_id,
            entry_filter_config.initial_move_atr,
            entry_filter_config.initial_retrace_atr,
        )
        combo_rows.setdefault(combo_key, set()).add((symbol, exchange, direction_mode))
        combo_filters[combo_key] = entry_filter_config

        latest_at = combo_latest_at.get(combo_key)
        if latest_at is None or computed_at > latest_at:
            combo_latest_at[combo_key] = computed_at

    options: list[dict] = []
    if required_combo_rows:
        for combo_key, available_rows in combo_rows.items():
            if not required_combo_rows.issubset(available_rows):
                continue

            max_levels, tp_atr, level_atr, atr_mode, *_ = combo_key
            entry_filter_config = combo_filters[combo_key]
            computed_at = combo_latest_at[combo_key]
            options.append(
                {
                    "value": _cache_selection_value(
                        "all",
                        "__ALL__",
                        "",
                        max_levels,
                        tp_atr,
                        level_atr,
                        atr_mode,
                        entry_filter_config,
                    ),
                    "scope": "all",
                    "symbol": "__ALL__",
                    "exchange": "",
                    "max_levels": max_levels,
                    "tp_atr": tp_atr,
                    "level_atr": level_atr,
                    "atr_mode": atr_mode,
                    "atr_mode_label": atr_mode_label(atr_mode),
                    **entry_filter_payload(entry_filter_config),
                    "direction_mode": None,
                    "direction_mode_label": "Selon chaque instrument",
                    "computed_at": computed_at,
                    "label": (
                        "Tous les instruments"
                        f" | Niveaux {max_levels} | TP {tp_atr:g} | Espacement {level_atr:g}"
                        f" | {atr_mode_label(atr_mode)}"
                        f" | {entry_filter_label(entry_filter_config)}"
                    ),
                }
            )

    options.sort(
        key=lambda option: (
            option["max_levels"],
            option["tp_atr"],
            option["level_atr"],
            option["atr_mode"],
            option["entry_filter_id"],
            option["initial_move_atr"] or 0.0,
            option["initial_retrace_atr"] or 0.0,
        ),
    )
    return options


def _resolve_cache_selection(instruments: list[dict], selected_value: str | None) -> tuple[list[dict], dict | None, dict | None]:
    options = _build_cache_selection_options(instruments)
    if not options:
        return options, None, None

    selected_option = next((option for option in options if option["value"] == selected_value), options[0])

    payload = _load_all_instruments_from_cache(
        instruments,
        selected_option["max_levels"],
        selected_option["tp_atr"],
        selected_option["level_atr"],
        normalize_entry_filter(
            selected_option["entry_filter_id"],
            selected_option["initial_move_atr"],
            selected_option["initial_retrace_atr"],
        ),
        atr_mode=selected_option["atr_mode"],
    )

    return options, selected_option, payload


def _save_cache(
    symbol: str,
    exchange: str,
    max_levels: int,
    tp_atr: float,
    level_atr: float,
    entry_filter_config: EntryFilterConfig,
    result: dict,
    atr_mode: str = DEFAULT_ATR_MODE,
) -> None:
    """Persist result to strategy_cache, replacing any existing entry."""
    entry_filter_id, initial_move_atr, initial_retrace_atr = entry_filter_cache_parts(entry_filter_config)
    atr_mode = normalize_atr_mode(atr_mode)
    to_store = {
        k: v
        for k, v in result.items()
        if k not in ("from_cache", "cached_at")
    }
    to_store.update(entry_filter_payload(entry_filter_config))
    to_store["atr_mode"] = atr_mode
    to_store["atr_mode_label"] = atr_mode_label(atr_mode)
    to_store["cache_version"] = STRATEGY_CACHE_VERSION
    result_json = json.dumps(to_store)
    con = get_cache_connection()
    try:
        con.execute(
            """
            DELETE FROM strategy_cache
            WHERE symbol=?
              AND exchange=?
              AND max_levels=?
              AND tp_atr=?
              AND level_atr=?
              AND atr_mode=?
              AND entry_filter_id=?
              AND initial_move_atr=?
              AND initial_retrace_atr=?
            """,
            [
                symbol,
                exchange,
                max_levels,
                tp_atr,
                level_atr,
                atr_mode,
                entry_filter_id,
                initial_move_atr,
                initial_retrace_atr,
            ],
        )
        con.execute(
            """
            INSERT INTO strategy_cache
                (
                    symbol,
                    exchange,
                    max_levels,
                    tp_atr,
                    level_atr,
                    atr_mode,
                    entry_filter_id,
                    initial_move_atr,
                    initial_retrace_atr,
                    computed_at,
                    result_json
                )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, ?)
            """,
            [
                symbol,
                exchange,
                max_levels,
                tp_atr,
                level_atr,
                atr_mode,
                entry_filter_id,
                initial_move_atr,
                initial_retrace_atr,
                result_json,
            ],
        )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Background simulation helpers
# ---------------------------------------------------------------------------


def _simulate_cycles_new_conn(
    symbol: str,
    exchange: str,
    max_levels: int,
    tp_atr: float,
    level_atr: float,
    direction_mode: str,
    entry_filter_config: EntryFilterConfig,
) -> list[dict]:
    """Thread-safe wrapper: opens its own DuckDB connection."""
    con = get_connection()
    try:
        return _simulate_cycles(
            symbol,
            exchange,
            max_levels,
            tp_atr,
            level_atr,
            con,
            direction_mode,
            entry_filter_config,
        )
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
    entry_filter_config: EntryFilterConfig | None = None,
) -> None:
    """Background task: runs simulation and pushes SSE events to the job queue."""
    entry_filter_config = entry_filter_config or normalize_entry_filter()
    q: asyncio.Queue = _jobs[job_id]["queue"]
    loop = asyncio.get_running_loop()
    try:
        if symbol == "__ALL__":
            all_results: dict[str, dict] = {}
            total_cycles_all = 0
            total = len(instruments)
            for idx, inst in enumerate(instruments):
                direction_mode = inst.get("preferred_direction", DEFAULT_TRADE_DIRECTION)
                cached = _load_cache(
                    inst["symbol"],
                    inst["exchange"],
                    max_levels,
                    tp_atr,
                    level_atr,
                    direction_mode,
                    entry_filter_config,
                )
                if cached is not None and not cached.get("error") and cached.get("results") is not None:
                    all_results[f"{inst['symbol']}|{inst['exchange']}"] = {
                        "symbol": inst["symbol"],
                        "exchange": inst["exchange"],
                        "direction_mode": direction_mode,
                        "direction_mode_label": trade_direction_label(direction_mode),
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
                    None,
                    _simulate_cycles_new_conn,
                    inst["symbol"],
                    inst["exchange"],
                    max_levels,
                    tp_atr,
                    level_atr,
                    direction_mode,
                    entry_filter_config,
                )
                if cycles:
                    instrument_result = {
                        "all_results": None,
                        "results": _aggregate(cycles, tp_atr, level_atr),
                        "error": None,
                        "selected_symbol": inst["symbol"],
                        "selected_exchange": inst["exchange"],
                        "direction_mode": direction_mode,
                        "max_levels": max_levels,
                        "tp_atr": tp_atr,
                        "level_atr": level_atr,
                        "atr_mode": DEFAULT_ATR_MODE,
                        "atr_mode_label": atr_mode_label(DEFAULT_ATR_MODE),
                        **entry_filter_payload(entry_filter_config),
                        "total_cycles": len(cycles),
                    }
                    try:
                        _save_cache(
                            inst["symbol"],
                            inst["exchange"],
                            max_levels,
                            tp_atr,
                            level_atr,
                            entry_filter_config,
                            instrument_result,
                        )
                    except Exception:  # noqa: BLE001
                        pass

                    all_results[f"{inst['symbol']}|{inst['exchange']}"] = {
                        "symbol": inst["symbol"],
                        "exchange": inst["exchange"],
                        "direction_mode": direction_mode,
                        "direction_mode_label": trade_direction_label(direction_mode),
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
                "atr_mode": DEFAULT_ATR_MODE,
                "atr_mode_label": atr_mode_label(DEFAULT_ATR_MODE),
                **entry_filter_payload(entry_filter_config),
                "total_cycles": total_cycles_all,
            }
        else:
            instrument = next(
                (
                    item
                    for item in instruments
                    if item["symbol"] == symbol and item["exchange"] == exchange
                ),
                None,
            )
            direction_mode = (
                instrument.get("preferred_direction", DEFAULT_TRADE_DIRECTION)
                if instrument is not None
                else DEFAULT_TRADE_DIRECTION
            )
            await q.put({"type": "progress", "current": 0, "total": 1, "label": symbol})
            cycles = await loop.run_in_executor(
                None,
                _simulate_cycles_new_conn,
                symbol,
                exchange,
                max_levels,
                tp_atr,
                level_atr,
                direction_mode,
                entry_filter_config,
            )
            await q.put({"type": "progress", "current": 1, "total": 1, "label": symbol})
            if not cycles:
                _jobs[job_id]["result"] = {
                    "all_results": None,
                    "results": None,
                    "error": f"Aucune donnée 1min disponible pour {symbol} ({exchange}).",
                    "selected_symbol": symbol,
                    "selected_exchange": exchange,
                    "direction_mode": direction_mode,
                    "max_levels": max_levels,
                    "tp_atr": tp_atr,
                    "level_atr": level_atr,
                    "atr_mode": DEFAULT_ATR_MODE,
                    "atr_mode_label": atr_mode_label(DEFAULT_ATR_MODE),
                    **entry_filter_payload(entry_filter_config),
                }
            else:
                _jobs[job_id]["result"] = {
                    "all_results": None,
                    "results": _aggregate(cycles, tp_atr, level_atr),
                    "error": None,
                    "selected_symbol": symbol,
                    "selected_exchange": exchange,
                    "direction_mode": direction_mode,
                    "max_levels": max_levels,
                    "tp_atr": tp_atr,
                    "level_atr": level_atr,
                    "atr_mode": DEFAULT_ATR_MODE,
                    "atr_mode_label": atr_mode_label(DEFAULT_ATR_MODE),
                    **entry_filter_payload(entry_filter_config),
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
                _save_cache(symbol, exchange, max_levels, tp_atr, level_atr, entry_filter_config, result)
            except Exception:  # noqa: BLE001
                pass
        await q.put({"type": "done", "url": f"/strategy/result/{job_id}"})


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/", response_class=HTMLResponse)
async def strategy_index(request: Request, selection: str | None = None):
    instruments = _get_instruments()
    cache_options, selected_option, cached = _resolve_cache_selection(instruments, selection)

    context = {
        "request": request,
        "instruments": instruments,
        "cache_options": cache_options,
        "selected_cache_option": selected_option["value"] if selected_option else None,
        "selected_cache_option_label": selected_option["label"] if selected_option else None,
        "results": None,
        "all_results": None,
        "error": None,
        "job_id": None,
    }

    if not cache_options:
        context["error"] = "Aucun résultat n'est disponible dans le cache."
    elif cached is None:
        context["error"] = "La combinaison sélectionnée n'est plus disponible dans le cache."
    else:
        cached = _enrich_all_results_with_mt5_swaps(cached)
        if cached.get("selected_symbol") and cached.get("selected_symbol") != "__ALL__":
            cached["selected_direction_mode_label"] = trade_direction_label(cached.get("direction_mode"))
        context.update(cached)

    return templates.TemplateResponse(
        "strategy.html",
        context,
    )


@router.get("/dashboard", response_class=HTMLResponse)
async def strategy_dashboard(
    request: Request,
    direction: str = "BOTH",
    metric: str = "profit_total",
    focus_max_levels: int | None = None,
    focus_tp_atr: float | None = None,
    focus_level_atr: float | None = None,
    focus_atr_mode: str | None = None,
    focus_trade_direction: str | None = None,
    focus_entry_filter_id: int | None = None,
    focus_initial_move_atr: float | None = None,
    focus_initial_retrace_atr: float | None = None,
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
                focus_atr_mode,
                focus_trade_direction,
                focus_entry_filter_id,
                focus_initial_move_atr,
                focus_initial_retrace_atr,
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
    entry_filter_id: int = Form(DEFAULT_ENTRY_FILTER_ID),
    initial_move_atr: float = Form(DEFAULT_INITIAL_MOVE_ATR),
    initial_retrace_atr: float = Form(DEFAULT_INITIAL_RETRACE_ATR),
    force: bool = Form(False),
):
    instruments = _get_instruments()
    entry_filter_config = normalize_entry_filter(
        entry_filter_id,
        initial_move_atr,
        initial_retrace_atr,
    )
    cache_options = _build_cache_selection_options(instruments)
    if symbol == "__ALL__":
        selected_cache_option = _cache_selection_value(
            "all",
            "__ALL__",
            "",
            max_levels,
            tp_atr,
            level_atr,
            entry_filter_config=entry_filter_config,
        )
        cached = _load_all_instruments_from_cache(
            instruments,
            max_levels,
            tp_atr,
            level_atr,
            entry_filter_config,
            atr_mode=DEFAULT_ATR_MODE,
        )
    else:
        instrument = next(
            (
                item
                for item in instruments
                if item["symbol"] == symbol and item["exchange"] == exchange
            ),
            None,
        )
        direction_mode = (
            instrument.get("preferred_direction", DEFAULT_TRADE_DIRECTION)
            if instrument is not None
            else DEFAULT_TRADE_DIRECTION
        )
        selected_cache_option = _cache_selection_value(
            "instrument",
            symbol,
            exchange,
            max_levels,
            tp_atr,
            level_atr,
            entry_filter_config=entry_filter_config,
        )
        cached = _load_cache(
            symbol,
            exchange,
            max_levels,
            tp_atr,
            level_atr,
            direction_mode,
            entry_filter_config,
            DEFAULT_ATR_MODE,
        )

    if cached is not None and not force:
        cached = _enrich_all_results_with_mt5_swaps(cached)
        if symbol != "__ALL__":
            cached["selected_direction_mode_label"] = trade_direction_label(cached.get("direction_mode"))
        return templates.TemplateResponse(
            "strategy.html",
            {
                "request": request,
                "instruments": instruments,
                "cache_options": cache_options,
                "selected_cache_option": selected_cache_option,
                "selected_cache_option_label": next(
                    (option["label"] for option in cache_options if option["value"] == selected_cache_option),
                    None,
                ),
                "job_id": None,
                **cached,
            },
        )

    return templates.TemplateResponse(
        "strategy.html",
        {
            "request": request,
            "instruments": instruments,
            "cache_options": cache_options,
            "selected_cache_option": selected_cache_option,
            "selected_cache_option_label": next(
                (option["label"] for option in cache_options if option["value"] == selected_cache_option),
                None,
            ),
            "results": None,
            "all_results": None,
            "error": "Le calcul manuel est désactivé sur cette page. Sélectionnez une combinaison déjà présente dans le cache.",
            "job_id": None,
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
    if result.get("selected_symbol") and result.get("selected_symbol") != "__ALL__":
        result["selected_direction_mode_label"] = trade_direction_label(result.get("direction_mode"))
    return templates.TemplateResponse(
        "strategy.html",
        {"request": request, "instruments": instruments, **result},
    )


def _get_instruments() -> list[dict]:
    con = get_connection()
    try:
        rows = con.execute(
            """
            SELECT w.symbol, w.exchange, i.type, COALESCE(w.preferred_direction, 'BOTH')
            FROM watchlist w
            INNER JOIN watchlist_timeframes wt
                ON wt.watchlist_id = w.id
               AND wt.timeframe = '1min'
            LEFT JOIN instruments i ON i.symbol = w.symbol AND i.exchange = w.exchange
            ORDER BY w.symbol
            """
        ).fetchall()
    finally:
        con.close()
    return [
        {
            "symbol": r[0],
            "exchange": r[1],
            "type": r[2],
            "preferred_direction": normalize_trade_direction(r[3]),
            "preferred_direction_label": trade_direction_label(r[3]),
        }
        for r in rows
    ]
