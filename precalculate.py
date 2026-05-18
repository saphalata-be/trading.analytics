"""
Pre-calculate strategy backtest results for a range of parameter values.

Results are stored in the strategy_cache table and will be served instantly
from the web UI without re-running the simulation.

The main process is the ONLY process that opens the DuckDB file.
Workers receive pre-loaded bar data as plain Python objects and do
pure in-memory computation — no database access in subprocesses.

Usage examples
--------------
# Default ranges, all instruments, 4 workers
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
from collections import defaultdict
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

from app.database import get_connection, get_cache_connection

# Keep in sync with app/routers/strategy.py _STRATEGY_CACHE_VERSION
_STRATEGY_CACHE_VERSION = 2


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass
class ParamCombo:
    max_levels: int
    tp_atr: float
    level_atr: float


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
    start_idx: int,
    atr50: float,
    max_levels: int,
    tp_atr: float,
    level_atr: float,
    direction: str,
) -> dict:
    """Single-cycle simulation using primitive-only arrays."""
    sign = 1 if direction == "LONG" else -1
    levels: list[float] = [bars_open[start_idx]]
    last_entry = bars_open[start_idx]
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
    }


def _simulate_fast(
    bars_open: list,
    bars_high: list,
    bars_low: list,
    bar_ts_min: list,
    hourly_indices: list,
    atr50_by_idx: dict,
    max_levels: int,
    tp_atr: float,
    level_atr: float,
) -> list[dict]:
    """Full simulation over all hourly cycle starts. No DB, no datetime."""
    results = []
    for start_idx in hourly_indices:
        atr50 = atr50_by_idx.get(start_idx)
        if not atr50:
            continue
        for direction in ("LONG", "SHORT"):
            cycle = _run_cycle_fast(
                bars_open, bars_high, bars_low, bar_ts_min,
                start_idx, atr50, max_levels, tp_atr, level_atr, direction,
            )
            cycle["direction"] = direction
            results.append(cycle)
    return results


def _aggregate_local(cycles: list[dict], tp_atr: float, level_atr: float) -> dict:
    """Identical logic to strategy.py _aggregate — duplicated to avoid subprocess imports."""
    buckets: dict[str, list] = defaultdict(list)
    for c in cycles:
        buckets[c["direction"]].append(c)

    stats = {}
    for direction, cycs in buckets.items():
        complete = [c for c in cycs if c["completed"]]
        maxlevel = [c for c in cycs if c["closed_max_levels"]]
        incomplete = [c for c in cycs if not c["completed"] and not c["closed_max_levels"]]
        total_closed = len(complete) + len(maxlevel)
        success_rate = len(complete) / total_closed * 100 if total_closed > 0 else None
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
            "avg_levels_complete": (
                sum(c["max_levels"] for c in complete) / len(complete) if complete else None
            ),
            "avg_duration_complete": (
                sum(c["duration_minutes"] for c in complete) / len(complete) if complete else None
            ),
            "avg_levels_incomplete": (
                sum(c["max_levels"] for c in incomplete) / len(incomplete) if incomplete else None
            ),
            "avg_duration_incomplete": (
                sum(c["duration_minutes"] for c in incomplete) / len(incomplete)
                if incomplete else None
            ),
            "avg_levels_all": (
                sum(c["max_levels"] for c in cycs) / len(cycs) if cycs else None
            ),
            "avg_duration_all": (
                sum(c["duration_minutes"] for c in cycs) / len(cycs) if cycs else None
            ),
        }
    return stats


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
    bars_open: list,
    bars_high: list,
    bars_low: list,
    bar_ts_min: list,
    hourly_indices: list,
    atr50_by_idx: dict,
    combos: list[ParamCombo],
) -> None:
    """
    Process a batch of parameter combos for one instrument.
    Receives only primitive types; no DB access, no datetime.
    Each result is pushed to _result_queue immediately so the main process
    can save it without waiting for the full symbol to complete.
    """
    for combo in combos:
        try:
            cycles = _simulate_fast(
                bars_open, bars_high, bars_low, bar_ts_min,
                hourly_indices, atr50_by_idx,
                combo.max_levels, combo.tp_atr, combo.level_atr,
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
                "max_levels": combo.max_levels,
                "tp_atr": combo.tp_atr,
                "level_atr": combo.level_atr,
                "total_cycles": len(cycles),
                "cache_version": _STRATEGY_CACHE_VERSION,
            }
            _result_queue.put((symbol, exchange, combo, result, None))
        except Exception as exc:  # noqa: BLE001
            _result_queue.put((symbol, exchange, combo, None, str(exc)))


# ---------------------------------------------------------------------------
# Cache helpers (main-process only — never called from workers)
# ---------------------------------------------------------------------------


def _cache_exists(symbol: str, exchange: str, combo: ParamCombo) -> bool:
    con = get_cache_connection()
    try:
        row = con.execute(
            """
            SELECT result_json FROM strategy_cache
            WHERE symbol=? AND exchange=? AND max_levels=? AND tp_atr=? AND level_atr=?
            """,
            [symbol, exchange, combo.max_levels, combo.tp_atr, combo.level_atr],
        ).fetchone()
    finally:
        con.close()
    if row is None:
        return False
    try:
        data = json.loads(row[0])
        return data.get("cache_version") == _STRATEGY_CACHE_VERSION
    except Exception:
        return False


def _save(symbol: str, exchange: str, combo: ParamCombo, result: dict) -> None:
    to_store = {k: v for k, v in result.items() if k not in ("from_cache", "cached_at")}
    to_store["cache_version"] = _STRATEGY_CACHE_VERSION
    result_json = json.dumps(to_store)
    con = get_cache_connection()
    try:
        con.execute(
            "DELETE FROM strategy_cache WHERE symbol=? AND exchange=? AND max_levels=? AND tp_atr=? AND level_atr=?",
            [symbol, exchange, combo.max_levels, combo.tp_atr, combo.level_atr],
        )
        con.execute(
            """
            INSERT INTO strategy_cache
                (symbol, exchange, max_levels, tp_atr, level_atr, computed_at, result_json)
            VALUES (?, ?, ?, ?, ?, current_timestamp, ?)
            """,
            [symbol, exchange, combo.max_levels, combo.tp_atr, combo.level_atr, result_json],
        )
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Data loading (main process only)
# ---------------------------------------------------------------------------


def _get_instruments(con, symbol: str | None, exchange: str | None) -> list[tuple[str, str]]:
    if symbol and exchange:
        rows = con.execute(
            "SELECT DISTINCT symbol, exchange FROM ohlcv WHERE symbol=? AND exchange=? AND timeframe='1min'",
            [symbol, exchange],
        ).fetchall()
    elif symbol:
        rows = con.execute(
            "SELECT DISTINCT symbol, exchange FROM ohlcv WHERE symbol=? AND timeframe='1min'",
            [symbol],
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT DISTINCT symbol, exchange FROM ohlcv WHERE timeframe='1min' ORDER BY symbol"
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _load_instrument_data(
    con, symbol: str, exchange: str
) -> tuple[list, list, list, list, list, dict]:
    """
    Load and pre-process instrument data into primitive-only structures.

    Returns
    -------
    bars_open, bars_high, bars_low : list[float]
    bar_ts_min                     : list[int]  (minutes since epoch, for duration)
    hourly_indices                 : list[int]  (bar indices where minute == 0)
    atr50_by_idx                   : dict[int, float]  (ATR50 keyed by bar index)
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
        return [], [], [], [], [], {}

    bars_open = [r[1] for r in min_rows]
    bars_high = [r[2] for r in min_rows]
    bars_low  = [r[3] for r in min_rows]
    bar_ts_min = [int(r[0].timestamp()) // 60 for r in min_rows]
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

    # Pre-compute ATR50 for each unique cycle-start date, then map to bar index
    atr50_by_date: dict = {}
    for d in {min_rows[i][0].date() for i in hourly_indices}:
        idx = bisect.bisect_left(daily_dates, d)  # first bar with date >= d
        slice_50 = daily_ranges[max(0, idx - 50) : idx]
        if len(slice_50) >= 50:
            atr50_by_date[d] = sum(h - l for h, l in slice_50) / 50

    atr50_by_idx = {
        i: atr50_by_date[min_rows[i][0].date()]
        for i in hourly_indices
        if min_rows[i][0].date() in atr50_by_date
    }

    return bars_open, bars_high, bars_low, bar_ts_min, hourly_indices, atr50_by_idx


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
        default=[5, 10, 15, 20, 30, 100],
        metavar="N",
        help="List of max_levels values to test (default: 5 10 15 20 30 100)",
    )
    parser.add_argument(
        "--tp-atr",
        nargs="+",
        type=float,
        default=[0.25, 0.5, 0.75, 1.0],
        metavar="F",
        help="List of tp_atr values to test (default: 0.25 0.5 0.75 1.0)",
    )
    parser.add_argument(
        "--level-atr",
        nargs="+",
        type=float,
        default=[0.5, 1.0, 1.5, 2.0],
        metavar="F",
        help="List of level_atr values to test (default: 0.5 1.0 1.5 2.0)",
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
        default=24,
        metavar="N",
        help="Number of parallel workers (default: 4)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-compute even if a valid cache entry already exists",
    )
    args = parser.parse_args()

    all_combos = [
        ParamCombo(max_levels=ml, tp_atr=tp, level_atr=la)
        for ml, tp, la in itertools.product(args.max_levels, args.tp_atr, args.level_atr)
    ]
    n_combos = len(all_combos)

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
    print(f"Combinaisons de paramètres       : {len(args.max_levels)} × {len(args.tp_atr)} × {len(args.level_atr)} = {n_combos}")
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
                for i, (symbol, exchange) in enumerate(instruments, 1):
                    if not args.force:
                        combos = [c for c in all_combos if not _cache_exists(symbol, exchange, c)]
                    else:
                        combos = list(all_combos)

                    skipped = n_combos - len(combos)
                    if skipped:
                        print(f"    → {skipped} déjà en cache, {len(combos)} à calculer", flush=True)
                    if not combos:
                        continue

                    print(f"  Chargement [{i}/{len(instruments)}] {symbol} ({exchange})...", flush=True)
                    # Open and close a connection per instrument so that trading.duckdb
                    # is only locked for the duration of the SQL queries (~seconds each).
                    # This lets the web app connect freely between instrument loads.
                    load_con = get_connection()
                    try:
                        bars_open, bars_high, bars_low, bar_ts_min, hourly_indices, atr50_by_idx = \
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
                            symbol, exchange,
                            bars_open, bars_high, bars_low,
                            bar_ts_min, hourly_indices, atr50_by_idx,
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
                            f"TP={combo.tp_atr:<4} LA={combo.level_atr:<4}  "
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
