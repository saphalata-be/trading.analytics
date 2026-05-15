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

from app.database import get_connection

# In-memory job store: job_id -> {"queue": asyncio.Queue, "result": dict | None}
_jobs: dict[str, dict] = {}

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
          AND datetime < ?
        ORDER BY datetime DESC
        LIMIT 50
        """,
        [symbol, exchange, before_dt],
    ).fetchall()
    if len(rows) < 1:
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

    # Index bars by position for fast lookup
    # Build a dict: datetime -> index
    dt_index: dict[datetime, int] = {b[0]: i for i, b in enumerate(bars)}

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
        start_price: float = start_bar[4]  # close of the start bar

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

    for i in range(start_idx + 1, len(bars)):
        bar = bars[i]
        # Use the bar's low/high and close for intra-bar check
        bar_dt: datetime = bar[0]
        bar_low: float = bar[3]
        bar_high: float = bar[4] if len(bar) < 5 else bar[4]  # close
        bar_close: float = bar[4]

        # Check TP first: can the TP be hit during this bar?
        # TP price where cumulative profit = tp_atr * atr50
        n = len(levels)
        # cum_profit = sum_i sign*(close - entry_i) = sign*n*close - sign*sum(entries)
        # = tp_atr*atr50  =>  close = (tp_atr*atr50 + sign*sum(entries)) / (sign*n)
        sum_entries = sum(levels)
        tp_price = (tp_atr * atr50 / sign + sum_entries) / n  # valid for sign != 0

        # Check if TP is reachable within this bar
        tp_hit = False
        if direction == "LONG" and bar_high >= tp_price and tp_price > last_entry - level_atr * atr50:
            tp_hit = True
        elif direction == "SHORT" and bar_low <= tp_price and tp_price < last_entry + level_atr * atr50:
            tp_hit = True

        if tp_hit:
            completed = True
            end_dt = bar_dt
            end_idx = i
            break

        # Check if a new level should be added
        # New level when price moves adversely >= level_atr * atr50 from last_entry
        new_level_price: Optional[float] = None
        if direction == "LONG":
            trigger = last_entry - level_atr * atr50
            if bar_low <= trigger:
                new_level_price = trigger
        else:
            trigger = last_entry + level_atr * atr50
            if bar_high >= trigger:
                new_level_price = trigger

        if new_level_price is not None:
            if len(levels) < max_levels:
                levels.append(new_level_price)
                last_entry = new_level_price
            else:
                # Max levels reached — close cycle immediately
                closed_max_levels = True
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
                sum(c["duration_minutes"] for c in incomplete) / len(incomplete) if incomplete else None
            ),
            "avg_levels_all": (
                sum(c["max_levels"] for c in cycs) / len(cycs) if cycs else None
            ),
            "avg_duration_all": (
                sum(c["duration_minutes"] for c in cycs) / len(cycs) if cycs else None
            ),
        }
    return stats


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def _load_cache(
    symbol: str, exchange: str, max_levels: int, tp_atr: float, level_atr: float
) -> dict | None:
    """Return cached result dict (with from_cache/cached_at keys) or None."""
    con = get_connection()
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
    result["from_cache"] = True
    result["cached_at"] = row[1]
    return result


def _save_cache(
    symbol: str, exchange: str, max_levels: int, tp_atr: float, level_atr: float, result: dict
) -> None:
    """Persist result to strategy_cache, replacing any existing entry."""
    to_store = {k: v for k, v in result.items() if k not in ("from_cache", "cached_at")}
    result_json = json.dumps(to_store)
    con = get_connection()
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
                    all_results[f"{inst['symbol']}|{inst['exchange']}"] = {
                        "symbol": inst["symbol"],
                        "exchange": inst["exchange"],
                        "stats": _aggregate(cycles, tp_atr, level_atr),
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
        if result and not result.get("error"):
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
        cached = _load_cache(symbol, exchange, max_levels, tp_atr, level_atr)
        if cached is not None:
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
    return templates.TemplateResponse(
        "strategy.html",
        {"request": request, "instruments": instruments, **job["result"]},
    )


def _get_instruments() -> list[dict]:
    con = get_connection()
    try:
        rows = con.execute(
            """
            SELECT DISTINCT o.symbol, o.exchange, w.instrument_type
            FROM ohlcv o
            JOIN watchlist w ON w.symbol = o.symbol AND w.exchange = o.exchange
            WHERE o.timeframe = '1min'
            ORDER BY o.symbol
            """
        ).fetchall()
    finally:
        con.close()
    return [{"symbol": r[0], "exchange": r[1], "type": r[2]} for r in rows]
