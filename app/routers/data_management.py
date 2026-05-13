"""
Router for the Data Management page.
Handles:
  - Instrument search (HTMX)
  - Add to watchlist
  - Watchlist display
  - Timeframe management per watchlist entry
  - OHLCV download (SSE streaming progress)
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.config import TIMEFRAMES
from app.database import get_connection
from app.services.twelvedata import (
    TwelveDataError,
    fetch_full_history,
    search_instruments,
)

router = APIRouter(prefix="/data", tags=["data"])
templates = Jinja2Templates(directory="app/templates")


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
# Download history (SSE)
# ---------------------------------------------------------------------------

@router.get("/watchlist/{watchlist_id}/download/{timeframe}")
async def download_history(watchlist_id: int, timeframe: str):
    """
    Server-Sent Events endpoint.
    Streams progress events while downloading OHLCV history.
    """
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

    async def event_stream():
        loop = asyncio.get_event_loop()

        # Mark as downloading
        _update_tf_status(watchlist_id, timeframe, "downloading")
        yield f"data: {json.dumps({'type': 'status', 'message': 'Démarrage du téléchargement...'})}\n\n"

        bars_container: list[list[dict]] = [[]]
        error_container: list[str] = []

        def progress_cb(fetched: int, total: int):
            pass  # We'll report after each page in the thread

        def run_download():
            try:
                bars = fetch_full_history(symbol, exchange, timeframe)
                bars_container[0] = bars
            except TwelveDataError as e:
                error_container.append(str(e))
            except Exception as e:
                error_container.append(f"Erreur inattendue: {e}")

        # Run blocking download in thread pool
        await loop.run_in_executor(None, run_download)

        if error_container:
            _update_tf_status(watchlist_id, timeframe, "error")
            yield f"data: {json.dumps({'type': 'error', 'message': error_container[0]})}\n\n"
            return

        bars = bars_container[0]
        if not bars:
            _update_tf_status(watchlist_id, timeframe, "error")
            yield f"data: {json.dumps({'type': 'error', 'message': 'Aucune donnée disponible'})}\n\n"
            return

        # Save to DB
        yield f"data: {json.dumps({'type': 'status', 'message': f'Sauvegarde de {len(bars)} barres...'})}\n\n"

        con2 = get_connection()
        # Upsert: insert or replace on conflict to avoid duplicate key errors
        con2.executemany(
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
                (symbol, exchange, timeframe,
                 b["datetime"], b["open"], b["high"], b["low"], b["close"], b["volume"])
                for b in bars
            ]
        )

        first_date = bars[0]["datetime"]
        last_date = bars[-1]["datetime"]
        now = datetime.now().isoformat(sep=" ", timespec="seconds")

        con2.execute("""
            UPDATE watchlist_timeframes
            SET first_date = ?, last_date = ?, total_bars = ?,
                last_download = ?, status = 'done'
            WHERE watchlist_id = ? AND timeframe = ?
        """, [first_date, last_date, len(bars), now, watchlist_id, timeframe])
        con2.close()

        yield f"data: {json.dumps({'type': 'done', 'total_bars': len(bars), 'first_date': first_date, 'last_date': last_date})}\n\n"

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
