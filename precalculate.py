"""
Pre-calculate strategy backtest results for a range of parameter values.

Results are stored in the strategy_cache table and will be served instantly
from the web UI without re-running the simulation.

The main process is the ONLY process that opens the DuckDB file.
Workers receive pre-loaded bar data as plain Python objects and do
pure in-memory computation — no database access in subprocesses.

Usage examples
--------------
# Default ranges, all instruments, 4 workers, all ATR modes
python precalculate.py

# Custom ranges
python precalculate.py --max-levels 5 10 15 --tp-atr 0.3 0.5 0.7 --level-atr 0.5 1.0 1.5

# Specific instrument only
python precalculate.py --symbol EURUSD --exchange Forex

# Force re-computation even if a cached result already exists
python precalculate.py --force

# Change the number of parallel workers
python precalculate.py --workers 8
"""
from __future__ import annotations

import argparse
import bisect
import itertools
import json
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Bootstrap: make "app" importable from the project root
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from app.database import get_connection, get_cache_connection, init_cache_db
from app.strategy_filters import (
    DEFAULT_ENTRY_FILTER_ID,
    DEFAULT_INITIAL_MOVE_ATR,
    DEFAULT_INITIAL_RETRACE_ATR,
    EntryFilterConfig,
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
    ATR_MODE_OPTIONS,
    DEFAULT_ATR_MODE,
    atr_mode_label,
    fixed_atr_price_value,
    infer_point_size,
    normalize_atr_mode,
)
from app.strategy_stats import aggregate_cycles
from app.trade_direction import expand_trade_directions, normalize_trade_direction


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass
class ParamCombo:
    max_levels: int
    tp_atr: float
    level_atr: float
    atr_mode: str
    entry_filter: EntryFilterConfig


def _chunk_param_combos(combos: list[ParamCombo], workers: int) -> list[list[ParamCombo]]:
    """Split a symbol's parameter space into a few medium-sized tasks."""
    if not combos:
        return []

    # Keep task count high enough to avoid end-of-run worker starvation,
    # but low enough to avoid copying the same instrument data excessively.
    target_batches = max(1, min(6, workers))
    chunk_size = max(1, (len(combos) + target_batches - 1) // target_batches)
    return [combos[i : i + chunk_size] for i in range(0, len(combos), chunk_size)]


# ---------------------------------------------------------------------------
# Fast simulation helpers (primitive types only — no datetime in workers)
# ---------------------------------------------------------------------------


def _run_cycle_fast(
    bars_open: list,
    bars_high: list,
    bars_low: list,
    bar_ts_min: list,
    bar_days: list,
    start_idx: int,
    start_price: float,
    atr50: float,
    max_levels: int,
    tp_atr: float,
    level_atr: float,
    direction: str,
) -> dict:
    """Single-cycle simulation using primitive-only arrays."""
    sign = 1 if direction == "LONG" else -1
    levels: list[float] = [start_price]
    last_entry = start_price
    completed = False
    closed_max_levels = False
    end_idx = start_idx
    n_bars = len(bars_open)

    for i in range(start_idx, n_bars):
        bar_high = bars_high[i]
        bar_low = bars_low[i]

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
                closed_max_levels = True
                end_idx = i
                break
            levels.append(trigger)
            last_entry = trigger

        if closed_max_levels:
            break

        n = len(levels)
        tp_price = (tp_atr * atr50 / sign + sum(levels)) / n
        tp_hit = bar_high >= tp_price if direction == "LONG" else bar_low <= tp_price

        if tp_hit:
            completed = True
            end_idx = i
            break

        end_idx = i

    return {
        "max_levels": len(levels),
        "duration_minutes": bar_ts_min[end_idx] - bar_ts_min[start_idx],
        "completed": completed,
        "closed_max_levels": closed_max_levels,
        "start_day": bar_days[start_idx],
    }


def _compute_adx_by_minute_idx_fast(
    hour_ts_min: list,
    hour_high: list,
    hour_low: list,
    hour_close: list,
    bar_ts_min: list,
    period: int,
) -> dict[int, float]:
    if period < 2 or len(hour_ts_min) < period * 2:
        return {}

    true_ranges: list[float] = []
    plus_dm: list[float] = []
    minus_dm: list[float] = []
    for i in range(1, len(hour_ts_min)):
        high = hour_high[i]
        low = hour_low[i]
        prev_high = hour_high[i - 1]
        prev_low = hour_low[i - 1]
        prev_close = hour_close[i - 1]
        up_move = high - prev_high
        down_move = prev_low - low
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)

    smoothed_tr = sum(true_ranges[:period])
    smoothed_plus = sum(plus_dm[:period])
    smoothed_minus = sum(minus_dm[:period])
    dx_values: list[float] = []
    adx: float | None = None
    adx_points: list[tuple[int, float]] = []

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
        adx_points.append((hour_ts_min[idx + 1], adx))

    if not adx_points:
        return {}

    adx_by_idx: dict[int, float] = {}
    point_idx = -1
    for bar_idx, ts_min in enumerate(bar_ts_min):
        while point_idx + 1 < len(adx_points) and adx_points[point_idx + 1][0] + 60 <= ts_min:
            point_idx += 1
        if point_idx >= 0:
            adx_by_idx[bar_idx] = adx_points[point_idx][1]
    return adx_by_idx


def _simulate_fast(
    bars_open: list,
    bars_high: list,
    bars_low: list,
    bar_ts_min: list,
    bar_days: list,
    hourly_indices: list,
    atr50_by_idx: dict,
    max_levels: int,
    tp_atr: float,
    level_atr: float,
    direction_mode: str,
    entry_filter: EntryFilterConfig,
    adx_by_idx: dict[int, float] | None = None,
) -> list[dict]:
    """Full simulation over all hourly cycle starts. No DB, no datetime."""
    results = []
    if entry_filter_uses_sequential_entries(entry_filter):
        for direction in expand_trade_directions(direction_mode):
            for entry_idx, entry_price in find_sequential_entries_for_arrays(
                bars_open,
                bars_high,
                bars_low,
                atr50_by_idx,
                direction,
                entry_filter,
            ):
                atr50 = atr50_by_idx.get(entry_idx)
                if not atr50:
                    continue
                cycle = _run_cycle_fast(
                    bars_open, bars_high, bars_low, bar_ts_min, bar_days,
                    entry_idx, entry_price, atr50, max_levels, tp_atr, level_atr, direction,
                )
                cycle["direction"] = direction
                results.append(cycle)
        return results

    for start_idx in hourly_indices:
        atr50 = atr50_by_idx.get(start_idx)
        if not atr50:
            continue
        for direction in expand_trade_directions(direction_mode):
            entry = find_entry_for_arrays(
                bars_open,
                bars_high,
                bars_low,
                start_idx,
                atr50,
                direction,
                entry_filter,
                adx_by_idx,
            )
            if entry is None:
                continue
            entry_idx, entry_price = entry
            cycle = _run_cycle_fast(
                bars_open, bars_high, bars_low, bar_ts_min, bar_days,
                entry_idx, entry_price, atr50, max_levels, tp_atr, level_atr, direction,
            )
            cycle["direction"] = direction
            results.append(cycle)
    return results


def _aggregate_local(cycles: list[dict], tp_atr: float, level_atr: float) -> dict:
    return aggregate_cycles(cycles, tp_atr, level_atr)


def _atr_by_idx_for_mode(
    atr_mode: str,
    atr50_by_idx: dict,
    n_bars: int,
    point_size: float,
) -> dict:
    fixed_value = fixed_atr_price_value(atr_mode, point_size)
    if fixed_value is None:
        return atr50_by_idx
    return {idx: fixed_value for idx in range(n_bars)}


# Module-level queue; set by _worker_init() called as the ProcessPoolExecutor
# initializer — workers push each result immediately so the main process can
# save it without waiting for all combos of a symbol to complete.
_result_queue = None


def _worker_init(q) -> None:
    global _result_queue
    _result_queue = q


# ---------------------------------------------------------------------------
# Worker (runs in a subprocess — NO database, NO datetime, NO app imports)
# ---------------------------------------------------------------------------


def _worker_instrument(
    symbol: str,
    exchange: str,
    direction_mode: str,
    bars_open: list,
    bars_high: list,
    bars_low: list,
    bar_ts_min: list,
    bar_days: list,
    hourly_indices: list,
    atr50_by_idx: dict,
    point_size: float,
    hour_ts_min: list,
    hour_high: list,
    hour_low: list,
    hour_close: list,
    combos: list[ParamCombo],
) -> None:
    """
    Process a batch of parameter combos for one instrument.
    Receives only primitive types; no DB access, no datetime.
    Each result is pushed to _result_queue immediately so the main process
    can save it without waiting for the full symbol to complete.
    """
    atr_by_mode: dict[str, dict] = {DEFAULT_ATR_MODE: atr50_by_idx}
    adx_by_period: dict[int, dict[int, float]] = {}
    for combo in combos:
        try:
            atr_by_idx = atr_by_mode.get(combo.atr_mode)
            if atr_by_idx is None:
                atr_by_idx = _atr_by_idx_for_mode(
                    combo.atr_mode,
                    atr50_by_idx,
                    len(bars_open),
                    point_size,
                )
                atr_by_mode[combo.atr_mode] = atr_by_idx

            adx_by_idx = None
            if combo.entry_filter.adx_period is not None:
                adx_by_idx = adx_by_period.get(combo.entry_filter.adx_period)
                if adx_by_idx is None:
                    adx_by_idx = _compute_adx_by_minute_idx_fast(
                        hour_ts_min,
                        hour_high,
                        hour_low,
                        hour_close,
                        bar_ts_min,
                        combo.entry_filter.adx_period,
                    )
                    adx_by_period[combo.entry_filter.adx_period] = adx_by_idx

            cycles = _simulate_fast(
                bars_open, bars_high, bars_low, bar_ts_min, bar_days,
                hourly_indices, atr_by_idx,
                combo.max_levels,
                combo.tp_atr,
                combo.level_atr,
                direction_mode,
                combo.entry_filter,
                adx_by_idx,
            )
            if not cycles:
                _result_queue.put((symbol, exchange, combo, None, f"Aucune donnée 1min pour {symbol} ({exchange})"))
                continue
            result = {
                "all_results": None,
                "results": _aggregate_local(cycles, combo.tp_atr, combo.level_atr),
                "error": None,
                "selected_symbol": symbol,
                "selected_exchange": exchange,
                "direction_mode": normalize_trade_direction(direction_mode),
                "max_levels": combo.max_levels,
                "tp_atr": combo.tp_atr,
                "level_atr": combo.level_atr,
                "atr_mode": combo.atr_mode,
                "atr_mode_label": atr_mode_label(combo.atr_mode),
                **entry_filter_payload(combo.entry_filter),
                "total_cycles": len(cycles),
                "cache_version": STRATEGY_CACHE_VERSION,
            }
            _result_queue.put((symbol, exchange, combo, result, None))
        except Exception as exc:  # noqa: BLE001
            _result_queue.put((symbol, exchange, combo, None, str(exc)))


# ---------------------------------------------------------------------------
# Cache helpers (main-process only — never called from workers)
# ---------------------------------------------------------------------------


def _cache_exists(symbol: str, exchange: str, combo: ParamCombo, direction_mode: str) -> bool:
    entry_filter_id, initial_move_atr, initial_retrace_atr = entry_filter_cache_parts(combo.entry_filter)
    con = get_cache_connection()
    try:
        row = con.execute(
            """
            SELECT result_json FROM strategy_cache
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
                combo.max_levels,
                combo.tp_atr,
                combo.level_atr,
                combo.atr_mode,
                entry_filter_id,
                initial_move_atr,
                initial_retrace_atr,
            ],
        ).fetchone()
    finally:
        con.close()
    if row is None:
        return False
    try:
        payload = normalize_strategy_cache_payload(json.loads(row[0]))
        return (
            payload is not None
            and payload.get("direction_mode") == normalize_trade_direction(direction_mode)
            and payload.get("atr_mode") == combo.atr_mode
            and payload.get("entry_filter_id") == combo.entry_filter.filter_id
            and payload.get("initial_move_atr") == combo.entry_filter.initial_move_atr
            and payload.get("initial_retrace_atr") == combo.entry_filter.initial_retrace_atr
        )
    except Exception:
        return False


def _save(symbol: str, exchange: str, combo: ParamCombo, result: dict) -> None:
    to_store = {k: v for k, v in result.items() if k not in ("from_cache", "cached_at")}
    to_store.update(entry_filter_payload(combo.entry_filter))
    to_store["atr_mode"] = combo.atr_mode
    to_store["atr_mode_label"] = atr_mode_label(combo.atr_mode)
    to_store["cache_version"] = STRATEGY_CACHE_VERSION
    result_json = json.dumps(to_store)
    entry_filter_id, initial_move_atr, initial_retrace_atr = entry_filter_cache_parts(combo.entry_filter)
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
                combo.max_levels,
                combo.tp_atr,
                combo.level_atr,
                combo.atr_mode,
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
                combo.max_levels,
                combo.tp_atr,
                combo.level_atr,
                combo.atr_mode,
                entry_filter_id,
                initial_move_atr,
                initial_retrace_atr,
                result_json,
            ],
        )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Data loading (main process only)
# ---------------------------------------------------------------------------


def _get_instruments(con, symbol: str | None, exchange: str | None) -> list[tuple[str, str, str]]:
    if symbol and exchange:
        rows = con.execute(
            """
            SELECT DISTINCT o.symbol, o.exchange, COALESCE(w.preferred_direction, 'BOTH')
            FROM ohlcv o
            LEFT JOIN watchlist w ON w.symbol = o.symbol AND w.exchange = o.exchange
            WHERE o.symbol=? AND o.exchange=? AND o.timeframe='1min'
            """,
            [symbol, exchange],
        ).fetchall()
    elif symbol:
        rows = con.execute(
            """
            SELECT DISTINCT o.symbol, o.exchange, COALESCE(w.preferred_direction, 'BOTH')
            FROM ohlcv o
            LEFT JOIN watchlist w ON w.symbol = o.symbol AND w.exchange = o.exchange
            WHERE o.symbol=? AND o.timeframe='1min'
            ORDER BY o.symbol
            """,
            [symbol],
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT DISTINCT o.symbol, o.exchange, COALESCE(w.preferred_direction, 'BOTH')
            FROM ohlcv o
            LEFT JOIN watchlist w ON w.symbol = o.symbol AND w.exchange = o.exchange
            WHERE o.timeframe='1min'
            ORDER BY o.symbol
            """
        ).fetchall()
    return [(r[0], r[1], normalize_trade_direction(r[2])) for r in rows]


def _load_instrument_data(
    con, symbol: str, exchange: str
) -> tuple[list, list, list, list, list, list, dict, float, list, list, list, list]:
    """
    Load and pre-process instrument data into primitive-only structures.

    Returns
    -------
    bars_open, bars_high, bars_low : list[float]
    bar_ts_min                     : list[int]  (minutes since epoch, for duration)
    bar_days                       : list[str]  (YYYY-MM-DD, for reporting)
    hourly_indices                 : list[int]  (bar indices where minute == 0)
    atr50_by_idx                   : dict[int, float]  (ATR50 keyed by bar index)
    point_size                     : float      (price value of one point)
    hour_*                         : list       (1h bars for ADX)
    """
    min_rows = con.execute(
        """
        SELECT datetime, open, high, low
        FROM ohlcv
        WHERE symbol=? AND exchange=? AND timeframe='1min'
        ORDER BY datetime ASC
        """,
        [symbol, exchange],
    ).fetchall()

    if not min_rows:
        return [], [], [], [], [], [], {}, 1.0, [], [], [], []

    bars_open = [r[1] for r in min_rows]
    bars_high = [r[2] for r in min_rows]
    bars_low  = [r[3] for r in min_rows]
    point_size = infer_point_size(symbol, bars_open + bars_high + bars_low)
    bar_ts_min = [int(r[0].timestamp()) // 60 for r in min_rows]
    bar_days = [r[0].date().isoformat() for r in min_rows]
    hourly_indices = [i for i, r in enumerate(min_rows) if r[0].minute == 0]

    # Daily bars for ATR50 (ASC order for bisect)
    daily_rows = con.execute(
        """
        SELECT datetime, high, low
        FROM ohlcv
        WHERE symbol=? AND exchange=? AND timeframe='1day'
        ORDER BY datetime ASC
        """,
        [symbol, exchange],
    ).fetchall()

    daily_dates = [r[0].date() for r in daily_rows]
    daily_ranges = [(r[1], r[2]) for r in daily_rows]  # (high, low)

    # Pre-compute ATR50 for each unique minute date, then map to bar index.
    # Filtered entries are event-based and can trigger on any 1min bar.
    atr50_by_date: dict = {}
    for d in {row[0].date() for row in min_rows}:
        idx = bisect.bisect_left(daily_dates, d)  # first bar with date >= d
        slice_50 = daily_ranges[max(0, idx - 50) : idx]
        if len(slice_50) >= 50:
            atr50_by_date[d] = sum(h - l for h, l in slice_50) / 50

    atr50_by_idx = {
        i: atr50_by_date[row[0].date()]
        for i, row in enumerate(min_rows)
        if row[0].date() in atr50_by_date
    }

    hour_rows = con.execute(
        """
        SELECT datetime, high, low, close
        FROM ohlcv
        WHERE symbol=? AND exchange=? AND timeframe='1h'
        ORDER BY datetime ASC
        """,
        [symbol, exchange],
    ).fetchall()
    hour_ts_min = [int(row[0].timestamp()) // 60 for row in hour_rows]
    hour_high = [row[1] for row in hour_rows]
    hour_low = [row[2] for row in hour_rows]
    hour_close = [row[3] for row in hour_rows]

    return (
        bars_open,
        bars_high,
        bars_low,
        bar_ts_min,
        bar_days,
        hourly_indices,
        atr50_by_idx,
        point_size,
        hour_ts_min,
        hour_high,
        hour_low,
        hour_close,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pre-calculate strategy backtest cache entries."
    )
    parser.add_argument(
        "--max-levels",
        nargs="+",
        type=int,
        default=[6, 100],
        metavar="N",
        help="List of max_levels values to test (default: 6)",
    )
    parser.add_argument(
        "--tp-atr",
        nargs="+",
        type=float,
        default=[1.0, 2.0],
        metavar="F",
        help="List of tp_atr values to test (default: 1.0 1.5 2.0)",
    )
    parser.add_argument(
        "--level-atr",
        nargs="+",
        type=float,
        default=[1.0, 2.0],
        metavar="F",
        help="List of level_atr values to test (default: 1.0 1.5 2.0)",
    )
    parser.add_argument(
        "--atr-mode",
        nargs="+",
        choices=ATR_MODE_OPTIONS,
        default=list(ATR_MODE_OPTIONS),
        metavar="MODE",
        help="ATR modes to test: d1_50 fixed_500 fixed_1000 (default: all)",
    )
    parser.add_argument(
        "--entry-filter",
        nargs="+",
        type=int,
        default=[0],
        metavar="N",
        help="Entry filters to test: 0=no filter, 1=initial ATR move, 2=ADX range filter (default: 0 1 2)",
    )
    parser.add_argument(
        "--initial-move-atr",
        nargs="+",
        type=float,
        default=[2.0, 3.0],
        metavar="F",
        help="Initial adverse move in ATR for entry filter 1 (default: 2.0)",
    )
    parser.add_argument(
        "--initial-retrace-atr",
        nargs="+",
        type=float,
        default=[0.5],
        metavar="F",
        help="Retrace in ATR required after the initial move for entry filter 1 (default: 0.5)",
    )
    parser.add_argument(
        "--adx-max",
        nargs="+",
        type=float,
        default=[25.0],
        metavar="F",
        help="Maximum 1h ADX allowed for entry filter 2 (default: 25.0)",
    )
    parser.add_argument(
        "--adx-period",
        nargs="+",
        type=int,
        default=[14],
        metavar="N",
        help="ADX period for entry filter 2 (default: 14)",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default=None,
        help="Restrict to a single symbol",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default=None,
        help="Restrict to a specific exchange (use with --symbol)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=20,
        metavar="N",
        help="Number of parallel workers (default: 20)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-compute even if a valid cache entry already exists",
    )
    args = parser.parse_args()

    entry_filters: list[EntryFilterConfig] = []
    for filter_id in args.entry_filter:
        if filter_id == 0:
            entry_filters.append(normalize_entry_filter(filter_id))
            continue
        if filter_id == 1:
            for initial_move_atr, initial_retrace_atr in itertools.product(
                args.initial_move_atr,
                args.initial_retrace_atr,
            ):
                entry_filters.append(
                    normalize_entry_filter(filter_id, initial_move_atr, initial_retrace_atr)
                )
            continue
        if filter_id == 2:
            for adx_max, adx_period in itertools.product(args.adx_max, args.adx_period):
                entry_filters.append(normalize_entry_filter(filter_id, adx_max, adx_period))
            continue
        raise ValueError(f"Filtre d'entree inconnu: {filter_id}")

    all_combos = [
        ParamCombo(
            max_levels=ml,
            tp_atr=tp,
            level_atr=la,
            atr_mode=normalize_atr_mode(atr_mode),
            entry_filter=entry_filter,
        )
        for ml, tp, la, atr_mode, entry_filter in itertools.product(
            args.max_levels,
            args.tp_atr,
            args.level_atr,
            args.atr_mode,
            entry_filters,
        )
    ]
    n_combos = len(all_combos)

    init_cache_db()

    con = get_connection()
    try:
        instruments = _get_instruments(con, args.symbol, args.exchange)
    finally:
        con.close()

    if not instruments:
        print("Aucun instrument avec des données 1min trouvé.")
        sys.exit(1)

    total_combos = len(instruments) * n_combos
    print(f"Instruments                      : {len(instruments)}")
    print(f"Filtres d'entree                 : {', '.join(entry_filter_label(item) for item in entry_filters)}")
    print(f"Modes ATR                        : {', '.join(atr_mode_label(item) for item in args.atr_mode)}")
    print(f"Combinaisons de parametres       : {len(args.max_levels)} x {len(args.tp_atr)} x {len(args.level_atr)} x {len(args.atr_mode)} = {n_combos}")
    print(f"Total (instruments × paramètres) : {total_combos}")
    print(f"Workers parallèles               : {args.workers}")
    print()

    done_jobs = 0
    errors = 0
    total_jobs = 0  # refined once we know how many are skipped

    # Load data and dispatch workers eagerly: a few futures per instrument.
    # The main process is the only process that touches the DB.
    # A Manager queue lets workers push each result immediately so the main
    # process can save it without waiting for the full symbol to finish.
    _manager = Manager()
    result_queue = _manager.Queue()
    start_time = time.monotonic()
    try:
        with ProcessPoolExecutor(
            max_workers=args.workers,
            initializer=_worker_init,
            initargs=(result_queue,),
        ) as pool:
            futures: dict = {}
            try:
                for i, (symbol, exchange, direction_mode) in enumerate(instruments, 1):
                    if not args.force:
                        combos = [c for c in all_combos if not _cache_exists(symbol, exchange, c, direction_mode)]
                    else:
                        combos = list(all_combos)

                    skipped = n_combos - len(combos)
                    if skipped:
                        print(f"    → {skipped} déjà en cache, {len(combos)} à calculer", flush=True)
                    if not combos:
                        continue

                    print(
                        f"  Chargement [{i}/{len(instruments)}] {symbol} ({exchange}) [{direction_mode}]...",
                        flush=True,
                    )
                    # Open and close a connection per instrument so that trading.duckdb
                    # is only locked for the duration of the SQL queries (~seconds each).
                    # This lets the web app connect freely between instrument loads.
                    load_con = get_connection()
                    try:
                        (
                            bars_open,
                            bars_high,
                            bars_low,
                            bar_ts_min,
                            bar_days,
                            hourly_indices,
                            atr50_by_idx,
                            point_size,
                            hour_ts_min,
                            hour_high,
                            hour_low,
                            hour_close,
                        ) = \
                            _load_instrument_data(load_con, symbol, exchange)
                    finally:
                        load_con.close()

                    total_jobs += len(combos)
                    combo_batches = _chunk_param_combos(combos, args.workers)
                    if len(combo_batches) > 1:
                        print(
                            f"    → découpé en {len(combo_batches)} lots de ~{len(combo_batches[0])} calculs",
                            flush=True,
                        )
                    for combo_batch in combo_batches:
                        future = pool.submit(
                            _worker_instrument,
                            symbol, exchange, direction_mode,
                            bars_open, bars_high, bars_low,
                            bar_ts_min, bar_days, hourly_indices, atr50_by_idx, point_size,
                            hour_ts_min, hour_high, hour_low, hour_close,
                            combo_batch,
                        )
                        futures[future] = (symbol, exchange)

                if futures:
                    print(f"\nCalcul de {total_jobs} combinaisons en cours...\n")

                    while done_jobs < total_jobs:
                        try:
                            symbol, exchange, combo, result, error = result_queue.get(timeout=1.0)
                        except Exception:
                            # Timeout — bail out if all workers finished unexpectedly
                            if all(f.done() for f in futures):
                                break
                            continue

                        done_jobs += 1
                        elapsed = time.monotonic() - start_time
                        rate = done_jobs / elapsed if elapsed > 0 else 0
                        remaining = (total_jobs - done_jobs) / rate if rate > 0 else float("inf")
                        pct = done_jobs / total_jobs * 100

                        if error:
                            errors += 1
                            status = f"ERREUR  {error}"
                        else:
                            _save(symbol, exchange, combo, result)
                            status = "OK"

                        print(
                            f"[{pct:5.1f}%] {done_jobs}/{total_jobs}  "
                            f"{symbol:<12} ML={combo.max_levels:<3} "
                            f"TP={combo.tp_atr:<4} LA={combo.level_atr:<4} "
                            f"ATR={atr_mode_label(combo.atr_mode):<16} "
                            f"F={combo.entry_filter.filter_id:<2}  "
                            f"{status:<60}  "
                            f"~{remaining/60:.1f} min restantes",
                            flush=True,
                        )
                else:
                    print("\nTout est déjà en cache au niveau instrument.")
            finally:
                pass
    finally:
        _manager.shutdown()

    elapsed_total = time.monotonic() - start_time
    print()
    print(f"Terminé en {elapsed_total:.1f} s — {done_jobs - errors} succès, {errors} erreurs.")


if __name__ == "__main__":
    main()
