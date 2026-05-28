"""
Router for the file-based data management page.
Handles:
  - TickStory source inspection
  - Full database refresh from discovered CSV files
  - Imported watchlist display
"""
from __future__ import annotations

from starlette.concurrency import run_in_threadpool

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.config import HISTORY_FILES_EXCHANGE, HISTORY_FILES_PATH
from app.database import get_cache_connection, get_connection
from app.services.history_files import (
    HistoryFilesError,
    import_history_files,
    load_ic_markets_symbol_names,
    scan_history_files,
)
from app.trade_direction import TRADE_DIRECTION_OPTIONS, normalize_trade_direction, trade_direction_label

router = APIRouter(prefix="/data", tags=["data"])
templates = Jinja2Templates(directory="app/templates")


@router.get("/", response_class=HTMLResponse)
async def data_page(request: Request):
    return templates.TemplateResponse("data_management.html", _build_context(request))


@router.post("/sync", response_class=HTMLResponse)
async def sync_data(request: Request, update_mode: bool = Form(False)):
    try:
        summary = await run_in_threadpool(import_history_files, update_mode=update_mode)
    except HistoryFilesError as exc:
        context = _build_context(request, error=str(exc))
    except Exception as exc:
        context = _build_context(request, error=f"Import impossible: {exc}")
    else:
        import_label = "Mise à jour terminée" if summary.update_mode else "Import terminé"
        skipped_label = ""
        if summary.skipped_symbols:
            skipped_label = (
                f" {len(summary.skipped_symbols)} symbole(s) ignoré(s) car absents d'IC Markets: "
                f"{', '.join(summary.skipped_symbols)}."
            )
        rows_label = f"{summary.rows:,}".replace(",", " ")
        context = _build_context(
            request,
            message=(
                f"{import_label}: {summary.instruments} instruments, "
                f"{summary.timeframes} fichiers, {rows_label} barres."
                f"{skipped_label}"
            ),
        )

    return templates.TemplateResponse("data_management.html", context)


@router.post("/trade-direction", response_class=HTMLResponse)
async def update_trade_direction(
    request: Request,
    watchlist_id: int = Form(...),
    preferred_direction: str = Form(...),
):
    direction = normalize_trade_direction(preferred_direction)

    con = get_connection()
    try:
        row = con.execute(
            "SELECT symbol, exchange FROM watchlist WHERE id = ?",
            [watchlist_id],
        ).fetchone()
        if row is not None:
            con.execute(
                "UPDATE watchlist SET preferred_direction = ? WHERE id = ?",
                [direction, watchlist_id],
            )
    finally:
        con.close()

    if row is None:
        context = _build_context(request, error="Instrument introuvable.")
    else:
        _invalidate_strategy_cache_for_instrument(row[0], row[1])
        context = _build_context(
            request,
            message=(
                f"{row[0]}: sens de trading mis à jour ({trade_direction_label(direction).lower()})."
            ),
        )

    return templates.TemplateResponse("data_management.html", context)


def _build_context(request: Request, message: str = "", error: str = "") -> dict:
    watchlist: list[dict] = []
    db_summary = {"instruments": 0, "timeframes": 0, "rows": 0}

    con = get_connection()
    try:
        watchlist = _get_watchlist(con)
        instruments, timeframes, rows = con.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM watchlist),
                (SELECT COUNT(*) FROM watchlist_timeframes),
                (SELECT COUNT(*) FROM ohlcv)
            """
        ).fetchone()
        db_summary = {
            "instruments": instruments or 0,
            "timeframes": timeframes or 0,
            "rows": rows or 0,
        }
    finally:
        con.close()

    mt5_swaps = _get_mt5_swap_lookup()
    for item in watchlist:
        swap_data = mt5_swaps.get(item["symbol"], {})
        item["swap_long"] = swap_data.get("swap_long")
        item["swap_short"] = swap_data.get("swap_short")

    try:
        available_files = scan_history_files()
        source_error = ""
    except HistoryFilesError as exc:
        available_files = []
        source_error = str(exc)

    try:
        ic_markets_symbols = load_ic_markets_symbol_names()
    except Exception:  # noqa: BLE001
        ic_markets_symbols = set()

    available_symbols = sorted({item.symbol for item in available_files})
    ic_markets_symbols_loaded = bool(ic_markets_symbols)
    available_symbol_statuses = [
        {
            "symbol": symbol,
            "exists_in_ic_markets": (
                symbol in ic_markets_symbols
                if ic_markets_symbols_loaded
                else None
            ),
        }
        for symbol in available_symbols
    ]
    unavailable_symbols = [
        item["symbol"]
        for item in available_symbol_statuses
        if item["exists_in_ic_markets"] is False
    ]
    available_timeframes = sorted(
        {item.timeframe for item in available_files},
        key=lambda item: {"1min": 0, "1h": 1, "1day": 2}.get(item, 99),
    )

    return {
        "request": request,
        "watchlist": watchlist,
        "source_path": str(HISTORY_FILES_PATH),
        "source_exchange": HISTORY_FILES_EXCHANGE,
        "available_files_count": len(available_files),
        "available_instruments_count": len(available_symbols),
        "available_symbol_statuses": available_symbol_statuses,
        "unavailable_symbols_count": len(unavailable_symbols),
        "ic_markets_symbols_loaded": ic_markets_symbols_loaded,
        "available_timeframes": available_timeframes,
        "db_summary": db_summary,
        "trade_direction_options": TRADE_DIRECTION_OPTIONS,
        "message": message,
        "error": error or source_error,
    }


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


def _invalidate_strategy_cache_for_instrument(symbol: str, exchange: str) -> None:
    con = get_cache_connection()
    try:
        con.execute(
            "DELETE FROM strategy_cache WHERE symbol = ? AND exchange = ?",
            [symbol, exchange],
        )
    finally:
        con.close()


def _get_watchlist(con) -> list[dict]:
    rows = con.execute(
        """
        SELECT id, symbol, exchange, instrument_type, preferred_direction, added_at
        FROM watchlist
        ORDER BY symbol ASC
        """
    ).fetchall()

    watchlist: list[dict] = []
    for row in rows:
        watchlist_id, symbol, exchange, instrument_type, preferred_direction, added_at = row
        normalized_direction = normalize_trade_direction(preferred_direction)
        timeframes = con.execute(
            """
            SELECT timeframe, first_date, last_date, total_bars, last_download, status
            FROM watchlist_timeframes
            WHERE watchlist_id = ?
            ORDER BY CASE timeframe WHEN '1min' THEN 1 WHEN '1h' THEN 2 WHEN '1day' THEN 3 ELSE 99 END
            """,
            [watchlist_id],
        ).fetchall()
        watchlist.append(
            {
                "id": watchlist_id,
                "symbol": symbol,
                "exchange": exchange,
                "type": instrument_type,
                "preferred_direction": normalized_direction,
                "preferred_direction_label": trade_direction_label(normalized_direction),
                "added_at": added_at,
                "timeframes": [
                    {
                        "timeframe": timeframe,
                        "first_date": first_date,
                        "last_date": last_date,
                        "total_bars": total_bars,
                        "last_download": last_download,
                        "status": status,
                    }
                    for timeframe, first_date, last_date, total_bars, last_download, status in timeframes
                ],
            }
        )
    return watchlist
