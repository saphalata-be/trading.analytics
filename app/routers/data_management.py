"""
Router for the Data Management page.
Handles:
  - Instrument search (HTMX)
  - Add to watchlist
  - Watchlist display
  - Timeframe management per watchlist entry
  - OHLCV download (SSE streaming progress + sequential queue)
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.config import TIMEFRAMES
from app.database import get_connection
from app.services.twelvedata import (
    TwelveDataError,
    fetch_full_history,
    get_earliest_date,
    search_instruments,
)

router = APIRouter(prefix="/data", tags=["data"])
templates = Jinja2Templates(directory="app/templates")

# ---------------------------------------------------------------------------
# Download queue — ensures downloads run one at a time
# ---------------------------------------------------------------------------

@dataclass
class DownloadJob:
    watchlist_id: int
    timeframe: str
    symbol: str
    exchange: str
    # Each job has its own asyncio.Queue to stream SSE events back to the client
    events: asyncio.Queue = field(default_factory=asyncio.Queue)


# Global FIFO queue of DownloadJob objects
_download_queue: asyncio.Queue[DownloadJob] = asyncio.Queue()
_queue_worker_started: bool = False
_worker_busy: bool = False  # True while the worker is actively processing a job


async def _queue_worker() -> None:
    """Background worker: processes download jobs one at a time."""
    global _worker_busy
    while True:
        job: DownloadJob = await _download_queue.get()
        _worker_busy = True
        try:
            await _run_download_job(job)
        except Exception as exc:
            await job.events.put({"type": "error", "message": f"Erreur inattendue: {exc}"})
        finally:
            # Signal the SSE generator that the job is finished
            await job.events.put(None)
            _worker_busy = False
            _download_queue.task_done()


async def _run_download_job(job: DownloadJob) -> None:
    """Execute a single download job, streaming events into job.events."""
    loop = asyncio.get_event_loop()

    _update_tf_status(job.watchlist_id, job.timeframe, "downloading")
    await job.events.put({"type": "status", "message": "Démarrage du téléchargement..."})

    # --- Check DB for existing data ---
    con = get_connection()
    row = con.execute(
        "SELECT last_date FROM watchlist_timeframes WHERE watchlist_id = ? AND timeframe = ?",
        [job.watchlist_id, job.timeframe],
    ).fetchone()
    con.close()

    start_date: str | None = None
    if row and row[0]:
        # Existing data: resume from the bar AFTER the last known one.
        # fetch_full_history uses start_date as a >= cutoff, so we add 1 minute
        # to avoid re-fetching the last bar already in the DB.
        last_date_raw = row[0]
        if isinstance(last_date_raw, datetime):
            last_dt = last_date_raw
        else:
            last_dt = datetime.strptime(str(last_date_raw)[:19], "%Y-%m-%d %H:%M:%S")
        start_date = (last_dt + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        await job.events.put({
            "type": "status",
            "message": f"Reprise depuis {start_date}...",
        })
    else:
        await job.events.put({
            "type": "status",
            "message": "Téléchargement depuis le 01/01/2010...",
        })

    # --- Run blocking download + incremental DB save in thread pool ---
    fetched_count: list[int] = [0]
    error_container: list[str] = []

    def _progress(fetched: int, total: int) -> None:
        # Fire-and-forget: schedule a status event on the event loop
        asyncio.run_coroutine_threadsafe(
            job.events.put({"type": "status", "message": f"{total} barres récupérées..."}),
            loop,
        )

    def run_download() -> None:
        con = get_connection()
        try:
            def _save_batch(batch: list[dict]) -> None:
                con.executemany(
                    """
                    INSERT INTO ohlcv (symbol, exchange, timeframe, datetime, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (symbol, exchange, timeframe, datetime) DO UPDATE SET
                        open   = excluded.open,
                        high   = excluded.high,
                        low    = excluded.low,
                        close  = excluded.close,
                        volume = excluded.volume
                    """,
                    [
                        (job.symbol, job.exchange, job.timeframe,
                         b["datetime"], b["open"], b["high"], b["low"], b["close"], b["volume"])
                        for b in batch
                    ],
                )
                fetched_count[0] += len(batch)

            fetch_full_history(
                job.symbol,
                job.exchange,
                job.timeframe,
                start_date=start_date,
                progress_callback=_progress,
                batch_callback=_save_batch,
            )
        except TwelveDataError as exc:
            error_container.append(str(exc))
        except Exception as exc:
            error_container.append(f"Erreur inattendue: {exc}")
        finally:
            con.close()

    await loop.run_in_executor(None, run_download)

    if error_container:
        _update_tf_status(job.watchlist_id, job.timeframe, "error")
        await job.events.put({"type": "error", "message": error_container[0]})
        return

    if fetched_count[0] == 0:
        # No new bars (already up to date)
        _update_tf_status(job.watchlist_id, job.timeframe, "done")
        await job.events.put({"type": "status", "message": "Données déjà à jour."})
        # Fetch current totals from DB to report back
        con2 = get_connection()
        meta = con2.execute(
            "SELECT first_date, last_date, total_bars FROM watchlist_timeframes WHERE watchlist_id = ? AND timeframe = ?",
            [job.watchlist_id, job.timeframe],
        ).fetchone()
        con2.close()
        if meta:
            await job.events.put({
                "type": "done",
                "total_bars": meta[2] or 0,
                "first_date": str(meta[0]) if meta[0] else "",
                "last_date": str(meta[1]) if meta[1] else "",
            })
        else:
            await job.events.put({"type": "done", "total_bars": 0, "first_date": "", "last_date": ""})
        return

    # Bars already saved to DB page-by-page — recompute final stats in thread pool
    def _finalize() -> tuple:
        con = get_connection()
        try:
            agg = con.execute(
                """
                SELECT MIN(datetime), MAX(datetime), COUNT(*)
                FROM ohlcv
                WHERE symbol = ? AND exchange = ? AND timeframe = ?
                """,
                [job.symbol, job.exchange, job.timeframe],
            ).fetchone()
            fd = str(agg[0]) if agg and agg[0] else ""
            ld = str(agg[1]) if agg and agg[1] else ""
            tb = agg[2] if agg else 0
            now = datetime.now().isoformat(sep=" ", timespec="seconds")
            con.execute(
                """
                UPDATE watchlist_timeframes
                SET first_date = ?, last_date = ?, total_bars = ?,
                    last_download = ?, status = 'done'
                WHERE watchlist_id = ? AND timeframe = ?
                """,
                [fd, ld, tb, now, job.watchlist_id, job.timeframe],
            )
        finally:
            con.close()
        return fd, ld, tb

    first_date, last_date, total_bars = await loop.run_in_executor(None, _finalize)

    await job.events.put({
        "type": "done",
        "total_bars": total_bars,
        "first_date": first_date,
        "last_date": last_date,
    })


# ---------------------------------------------------------------------------
# Main page
# ---------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def data_page(request: Request):
    con = get_connection()
    watchlist = con.execute("""
        SELECT w.id, w.symbol, w.exchange, w.instrument_type,
               w.added_at
        FROM watchlist w
        ORDER BY w.added_at DESC
    """).fetchall()

    watchlist_rows = []
    for row in watchlist:
        wid, symbol, exchange, itype, added_at = row
        timeframes = con.execute("""
            SELECT timeframe, first_date, last_date, total_bars, last_download, status
            FROM watchlist_timeframes
            WHERE watchlist_id = ?
            ORDER BY timeframe
        """, [wid]).fetchall()
        watchlist_rows.append({
            "id": wid,
            "symbol": symbol,
            "exchange": exchange,
            "type": itype,
            "added_at": added_at,
            "timeframes": [
                {
                    "timeframe": t[0],
                    "first_date": t[1],
                    "last_date": t[2],
                    "total_bars": t[3],
                    "last_download": t[4],
                    "status": t[5],
                }
                for t in timeframes
            ],
        })
    con.close()

    return templates.TemplateResponse("data_management.html", {
        "request": request,
        "watchlist": watchlist_rows,
        "timeframes": TIMEFRAMES,
    })


# ---------------------------------------------------------------------------
# Instrument search (HTMX)
# ---------------------------------------------------------------------------

@router.get("/search", response_class=HTMLResponse)
async def search(request: Request, q: str = "", type: str = ""):
    if len(q) < 2:
        return HTMLResponse("")
    try:
        results = search_instruments(q, type)
    except TwelveDataError as e:
        return HTMLResponse(f'<p class="text-red-500 text-sm">{e}</p>')

    return templates.TemplateResponse("partials/search_results.html", {
        "request": request,
        "results": results,
    })


# ---------------------------------------------------------------------------
# Add to watchlist
# ---------------------------------------------------------------------------

@router.post("/watchlist/add", response_class=HTMLResponse)
async def add_to_watchlist(
    request: Request,
    symbol: str = Form(...),
    exchange: str = Form(...),
    name: str = Form(""),
    instrument_type: str = Form(""),
    currency: str = Form(""),
    country: str = Form(""),
):
    try:
        get_earliest_date(symbol, exchange, "1day")
    except TwelveDataError as exc:
        return HTMLResponse(f'<p class="text-red-500 text-sm">{exc}</p>', status_code=400)

    con = get_connection()

    # Upsert instrument
    con.execute("""
        INSERT INTO instruments (symbol, name, type, currency, exchange, country)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (symbol, exchange) DO UPDATE SET
            name = excluded.name,
            type = excluded.type,
            currency = excluded.currency,
            country = excluded.country
    """, [symbol, name, instrument_type, currency, exchange, country])

    # Check if already in watchlist
    existing = con.execute(
        "SELECT id FROM watchlist WHERE symbol = ? AND exchange = ?",
        [symbol, exchange]
    ).fetchone()

    if not existing:
        con.execute("""
            INSERT INTO watchlist (id, symbol, exchange, instrument_type)
            VALUES (nextval('watchlist_id_seq'), ?, ?, ?)
        """, [symbol, exchange, instrument_type])
        new_id = con.execute(
            "SELECT id FROM watchlist WHERE symbol = ? AND exchange = ?",
            [symbol, exchange]
        ).fetchone()[0]
        for tf in ["1day", "1h", "1min"]:
            con.execute("""
                INSERT INTO watchlist_timeframes (watchlist_id, timeframe, status)
                VALUES (?, ?, 'pending')
            """, [new_id, tf])

    # Return updated watchlist partial
    watchlist = _get_watchlist(con)
    con.close()

    return templates.TemplateResponse("partials/watchlist.html", {
        "request": request,
        "watchlist": watchlist,
        "timeframes": TIMEFRAMES,
    })


# ---------------------------------------------------------------------------
# Remove from watchlist
# ---------------------------------------------------------------------------

@router.delete("/watchlist/{watchlist_id}", response_class=HTMLResponse)
async def remove_from_watchlist(request: Request, watchlist_id: int):
    con = get_connection()
    con.execute("DELETE FROM watchlist_timeframes WHERE watchlist_id = ?", [watchlist_id])
    con.execute("DELETE FROM watchlist WHERE id = ?", [watchlist_id])
    watchlist = _get_watchlist(con)
    con.close()

    return templates.TemplateResponse("partials/watchlist.html", {
        "request": request,
        "watchlist": watchlist,
        "timeframes": TIMEFRAMES,
    })


# ---------------------------------------------------------------------------
# Add timeframe to a watchlist entry
# ---------------------------------------------------------------------------

@router.post("/watchlist/{watchlist_id}/timeframe", response_class=HTMLResponse)
async def add_timeframe(
    request: Request,
    watchlist_id: int,
    timeframe: str = Form(...),
):
    con = get_connection()
    existing = con.execute(
        "SELECT 1 FROM watchlist_timeframes WHERE watchlist_id = ? AND timeframe = ?",
        [watchlist_id, timeframe]
    ).fetchone()

    if not existing:
        con.execute("""
            INSERT INTO watchlist_timeframes (watchlist_id, timeframe, status)
            VALUES (?, ?, 'pending')
        """, [watchlist_id, timeframe])

    watchlist = _get_watchlist(con)
    con.close()

    return templates.TemplateResponse("partials/watchlist.html", {
        "request": request,
        "watchlist": watchlist,
        "timeframes": TIMEFRAMES,
    })


# ---------------------------------------------------------------------------
# Remove timeframe
# ---------------------------------------------------------------------------

@router.delete("/watchlist/{watchlist_id}/timeframe/{timeframe}", response_class=HTMLResponse)
async def remove_timeframe(
    request: Request,
    watchlist_id: int,
    timeframe: str,
):
    con = get_connection()
    # Also delete OHLCV data for this entry
    row = con.execute("SELECT symbol, exchange FROM watchlist WHERE id = ?", [watchlist_id]).fetchone()
    if row:
        symbol, exchange = row
        con.execute(
            "DELETE FROM ohlcv WHERE symbol = ? AND exchange = ? AND timeframe = ?",
            [symbol, exchange, timeframe]
        )
    con.execute(
        "DELETE FROM watchlist_timeframes WHERE watchlist_id = ? AND timeframe = ?",
        [watchlist_id, timeframe]
    )
    watchlist = _get_watchlist(con)
    con.close()

    return templates.TemplateResponse("partials/watchlist.html", {
        "request": request,
        "watchlist": watchlist,
        "timeframes": TIMEFRAMES,
    })


# ---------------------------------------------------------------------------
# Download history (SSE + queue)
# ---------------------------------------------------------------------------

@router.get("/watchlist/{watchlist_id}/download/{timeframe}")
async def download_history(request: Request, watchlist_id: int, timeframe: str):
    """
    Server-Sent Events endpoint.
    Enqueues the download job and streams progress events back to the client.
    Downloads are processed sequentially by the background worker.
    """
    global _queue_worker_started

    con = get_connection()
    row = con.execute(
        "SELECT symbol, exchange FROM watchlist WHERE id = ?", [watchlist_id]
    ).fetchone()
    con.close()

    if not row:
        async def error_stream():
            yield f"data: {json.dumps({'type': 'error', 'message': 'Watchlist entry not found'})}\n\n"
        return StreamingResponse(error_stream(), media_type="text/event-stream")

    symbol, exchange = row

    # Start the background worker once (on first download request)
    if not _queue_worker_started:
        _queue_worker_started = True
        asyncio.create_task(_queue_worker())

    # Build the job and enqueue it
    job = DownloadJob(
        watchlist_id=watchlist_id,
        timeframe=timeframe,
        symbol=symbol,
        exchange=exchange,
    )

    # jobs_ahead = items waiting in queue + 1 if worker is actively processing another job
    jobs_ahead = _download_queue.qsize() + (1 if _worker_busy else 0)
    await _download_queue.put(job)

    async def event_stream():
        # Always send an immediate event so the browser never stays stuck at "Connexion..."
        if jobs_ahead > 0:
            _update_tf_status(watchlist_id, timeframe, "pending")
            yield f"data: {json.dumps({'type': 'status', 'message': f'En attente ({jobs_ahead} téléchargement(s) avant vous)...'})}\n\n"
        else:
            # Worker is idle — job will start almost immediately; still acknowledge receipt
            yield f"data: {json.dumps({'type': 'status', 'message': 'Démarrage du téléchargement...'})}\n\n"

        # Stream events produced by the worker
        while True:
            event = await job.events.get()
            if event is None:
                # Worker signals end of job
                break
            yield f"data: {json.dumps(event)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _update_tf_status(watchlist_id: int, timeframe: str, status: str):
    con = get_connection()
    con.execute(
        "UPDATE watchlist_timeframes SET status = ? WHERE watchlist_id = ? AND timeframe = ?",
        [status, watchlist_id, timeframe]
    )
    con.close()


def _get_watchlist(con) -> list[dict]:
    rows = con.execute("""
        SELECT id, symbol, exchange, instrument_type, added_at
        FROM watchlist
        ORDER BY added_at DESC
    """).fetchall()

    result = []
    for row in rows:
        wid, symbol, exchange, itype, added_at = row
        tfs = con.execute("""
            SELECT timeframe, first_date, last_date, total_bars, last_download, status
            FROM watchlist_timeframes
            WHERE watchlist_id = ?
            ORDER BY timeframe
        """, [wid]).fetchall()
        result.append({
            "id": wid,
            "symbol": symbol,
            "exchange": exchange,
            "type": itype,
            "added_at": added_at,
            "timeframes": [
                {
                    "timeframe": t[0],
                    "first_date": t[1],
                    "last_date": t[2],
                    "total_bars": t[3],
                    "last_download": t[4],
                    "status": t[5],
                }
                for t in tfs
            ],
        })
    return result
