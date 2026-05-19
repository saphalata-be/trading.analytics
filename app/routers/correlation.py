from __future__ import annotations

import asyncio
import json
import math
import statistics
import uuid
from itertools import combinations
from typing import Any, Callable

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.database import get_connection

router = APIRouter(prefix="/correlation", tags=["correlation"])
templates = Jinja2Templates(directory="app/templates")

_jobs: dict[str, dict[str, Any]] = {}
_LOT_SIZE = 100_000.0
_TOP_LIMIT = 5
_SINGLETON_BASKET_NAME = "Panier corrélation"


def _is_fx_symbol(symbol: str) -> bool:
    return len(symbol) == 6 and symbol.isalpha()


def _split_symbol(symbol: str) -> tuple[str, str]:
    return symbol[:3], symbol[3:]


def _normalize_side(side: str) -> str:
    return "SELL" if str(side).strip().upper() == "SELL" else "BUY"


def _side_label(side: str) -> str:
    return "Achat" if side == "BUY" else "Vente"


def _parse_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _get_fx_instruments() -> list[dict[str, str | None]]:
    con = get_connection()
    try:
        rows = con.execute(
            """
            SELECT DISTINCT o.symbol, o.exchange, i.type
            FROM ohlcv o
            LEFT JOIN instruments i ON i.symbol = o.symbol AND i.exchange = o.exchange
            WHERE o.timeframe = '1h'
            ORDER BY o.symbol ASC, o.exchange ASC
            """
        ).fetchall()
    finally:
        con.close()

    instruments: list[dict[str, str | None]] = []
    for symbol, exchange, instrument_type in rows:
        if not _is_fx_symbol(symbol):
            continue
        if instrument_type and str(instrument_type).lower() not in {"forex", "fx"}:
            continue
        instruments.append({"symbol": symbol, "exchange": exchange, "type": instrument_type})
    return instruments


def _add_or_update_basket_item(
    basket_items: list[dict[str, str]],
    available_lookup: set[tuple[str, str]],
    symbol: str,
    exchange: str,
    side: str,
) -> tuple[list[dict[str, str]], str | None, str | None]:
    key = (symbol, exchange)
    if key not in available_lookup:
        return basket_items, None, f"{symbol} ({exchange}) n'est pas disponible en 1H pour la corrélation."

    normalized_side = _normalize_side(side)
    updated_items = [dict(item) for item in basket_items]

    for item in updated_items:
        if item["symbol"] != symbol or item["exchange"] != exchange:
            continue
        previous_side = item["side"]
        item["side"] = normalized_side
        if previous_side == normalized_side:
            return updated_items, f"{symbol} est déjà présent en {_side_label(normalized_side).lower()}.", None
        return updated_items, f"{symbol} mis à jour en {_side_label(normalized_side).lower()} dans le panier.", None

    updated_items.append(
        {
            "symbol": symbol,
            "exchange": exchange,
            "side": normalized_side,
        }
    )
    return updated_items, f"{symbol} ajouté au panier en {_side_label(normalized_side).lower()}.", None


def _get_singleton_basket() -> dict[str, Any]:
    con = get_connection()
    try:
        basket = con.execute(
            """
            SELECT id, name, created_at, updated_at
            FROM correlation_baskets
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

        if basket is None:
            basket_id = con.execute(
                "SELECT nextval('correlation_basket_id_seq')"
            ).fetchone()[0]
            con.execute(
                """
                INSERT INTO correlation_baskets (id, name, created_at, updated_at)
                VALUES (?, ?, current_timestamp, current_timestamp)
                """,
                [basket_id, _SINGLETON_BASKET_NAME],
            )
            basket = con.execute(
                """
                SELECT id, name, created_at, updated_at
                FROM correlation_baskets
                WHERE id = ?
                """,
                [basket_id],
            ).fetchone()

        items = con.execute(
            """
            SELECT symbol, exchange, side, position
            FROM correlation_basket_items
            WHERE basket_id = ?
            ORDER BY position ASC, symbol ASC
            """,
            [basket[0]],
        ).fetchall()
    finally:
        con.close()

    return {
        "id": basket[0],
        "name": basket[1],
        "created_at": basket[2],
        "updated_at": basket[3],
        "items": [
            {
                "symbol": row[0],
                "exchange": row[1],
                "side": _normalize_side(row[2]),
                "position": row[3],
            }
            for row in items
        ],
    }


def _save_singleton_basket(basket_items: list[dict[str, str]]) -> dict[str, Any]:
    con = get_connection()
    try:
        basket = con.execute(
            """
            SELECT id
            FROM correlation_baskets
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

        if basket is None:
            basket_id = con.execute(
                "SELECT nextval('correlation_basket_id_seq')"
            ).fetchone()[0]
            con.execute(
                """
                INSERT INTO correlation_baskets (id, name, created_at, updated_at)
                VALUES (?, ?, current_timestamp, current_timestamp)
                """,
                [basket_id, _SINGLETON_BASKET_NAME],
            )
        else:
            basket_id = basket[0]
            created_at = con.execute(
                "SELECT created_at FROM correlation_baskets WHERE id = ?",
                [basket_id],
            ).fetchone()[0]
            con.execute(
                "DELETE FROM correlation_basket_items WHERE basket_id = ?",
                [basket_id],
            )
            con.execute(
                """
                DELETE FROM correlation_baskets
                WHERE id = ?
                """,
                [basket_id],
            )
            con.execute(
                """
                INSERT INTO correlation_baskets (id, name, created_at, updated_at)
                VALUES (?, ?, ?, current_timestamp)
                """,
                [basket_id, _SINGLETON_BASKET_NAME, created_at],
            )

        for index, item in enumerate(basket_items, start=1):
            con.execute(
                """
                INSERT INTO correlation_basket_items (basket_id, symbol, exchange, side, position)
                VALUES (?, ?, ?, ?, ?)
                """,
                [basket_id, item["symbol"], item["exchange"], item["side"], index],
            )
    finally:
        con.close()

    return _get_singleton_basket()


def _remove_basket_item(
    basket_items: list[dict[str, str]],
    symbol: str,
    exchange: str,
) -> tuple[list[dict[str, str]], str]:
    updated_items = [
        dict(item)
        for item in basket_items
        if item["symbol"] != symbol or item["exchange"] != exchange
    ]
    if len(updated_items) == len(basket_items):
        return updated_items, f"{symbol} n'était pas présent dans le panier."
    return updated_items, f"{symbol} retiré du panier."


def _find_usd_conversion(
    currency: str,
    preferred_exchange: str,
    available_pairs: set[tuple[str, str]],
) -> dict[str, str] | None:
    if currency == "USD":
        return None

    direct_symbol = f"{currency}USD"
    inverse_symbol = f"USD{currency}"

    candidates = [
        (direct_symbol, preferred_exchange, "multiply"),
        (inverse_symbol, preferred_exchange, "divide"),
    ]

    for symbol, exchange in available_pairs:
        if symbol == direct_symbol and exchange != preferred_exchange:
            candidates.append((symbol, exchange, "multiply"))
        elif symbol == inverse_symbol and exchange != preferred_exchange:
            candidates.append((symbol, exchange, "divide"))

    for symbol, exchange, mode in candidates:
        if (symbol, exchange) in available_pairs:
            return {"symbol": symbol, "exchange": exchange, "mode": mode}
    return None


def _build_conversion_map(
    basket_items: list[dict[str, str]],
    available_pairs: set[tuple[str, str]],
) -> tuple[dict[str, dict[str, str]], list[str]]:
    conversions: dict[str, dict[str, str]] = {}
    missing: list[str] = []

    for item in basket_items:
        _, quote_currency = _split_symbol(item["symbol"])
        if quote_currency == "USD" or quote_currency in conversions:
            continue

        conversion = _find_usd_conversion(quote_currency, item["exchange"], available_pairs)
        if conversion is None:
            missing.append(quote_currency)
            continue
        conversions[quote_currency] = conversion

    return conversions, sorted(set(missing))


def _load_hourly_series(required_pairs: set[tuple[str, str]]) -> dict[tuple[str, str], dict[str, Any]]:
    con = get_connection()
    try:
        dataset: dict[tuple[str, str], dict[str, Any]] = {}
        for symbol, exchange in required_pairs:
            rows = con.execute(
                """
                SELECT datetime, close
                FROM ohlcv
                WHERE symbol = ? AND exchange = ? AND timeframe = '1h'
                ORDER BY datetime ASC
                """,
                [symbol, exchange],
            ).fetchall()
            closes = {row[0]: float(row[1]) for row in rows if row[1] is not None}
            dataset[(symbol, exchange)] = {
                "closes": closes,
                "timestamps": set(closes.keys()),
            }
        return dataset
    finally:
        con.close()


def _convert_quote_pnl_to_usd(
    pnl_quote: float,
    quote_currency: str,
    current_dt: datetime,
    dataset: dict[tuple[str, str], dict[str, Any]],
    conversions: dict[str, dict[str, str]],
) -> float | None:
    if quote_currency == "USD":
        return pnl_quote

    conversion = conversions.get(quote_currency)
    if conversion is None:
        return None

    closes = dataset[(conversion["symbol"], conversion["exchange"])]["closes"]
    rate = closes.get(current_dt)
    if rate in (None, 0):
        return None

    if conversion["mode"] == "multiply":
        return pnl_quote * rate
    return pnl_quote / rate


def _combo_members(combo_items: tuple[dict[str, str], ...]) -> list[str]:
    return [f"{item['symbol']} {_side_label(item['side'])}" for item in combo_items]


def _combo_key(combo_items: tuple[dict[str, str], ...]) -> str:
    return "|".join(f"{item['symbol']}:{item['exchange']}:{item['side']}" for item in combo_items)


def _evaluate_combo(
    combo_items: tuple[dict[str, str], ...],
    dataset: dict[tuple[str, str], dict[str, Any]],
    conversions: dict[str, dict[str, str]],
) -> dict[str, Any] | None:
    required_pairs = {(item["symbol"], item["exchange"]) for item in combo_items}
    for item in combo_items:
        _, quote_currency = _split_symbol(item["symbol"])
        conversion = conversions.get(quote_currency)
        if conversion is not None:
            required_pairs.add((conversion["symbol"], conversion["exchange"]))

    common_timestamps: set[datetime] | None = None
    for pair_key in required_pairs:
        pair_data = dataset.get(pair_key)
        if not pair_data or not pair_data["timestamps"]:
            return None
        if common_timestamps is None:
            common_timestamps = set(pair_data["timestamps"])
        else:
            common_timestamps &= pair_data["timestamps"]
        if not common_timestamps:
            return None

    if not common_timestamps or len(common_timestamps) < 2:
        return None

    ordered_times = sorted(common_timestamps)
    entry_dt = ordered_times[0]
    entry_prices = {
        (item["symbol"], item["exchange"]): dataset[(item["symbol"], item["exchange"])]["closes"][entry_dt]
        for item in combo_items
    }

    curve_labels: list[str] = []
    curve_values: list[float] = []

    for current_dt in ordered_times:
        total_equity = 0.0
        for item in combo_items:
            pair_key = (item["symbol"], item["exchange"])
            current_close = dataset[pair_key]["closes"].get(current_dt)
            if current_close is None:
                return None

            side_sign = 1.0 if item["side"] == "BUY" else -1.0
            pnl_quote = _LOT_SIZE * side_sign * (current_close - entry_prices[pair_key])
            _, quote_currency = _split_symbol(item["symbol"])
            pnl_usd = _convert_quote_pnl_to_usd(pnl_quote, quote_currency, current_dt, dataset, conversions)
            if pnl_usd is None:
                return None
            total_equity += pnl_usd

        curve_labels.append(current_dt.strftime("%Y-%m-%d %H:%M"))
        curve_values.append(total_equity)

    equity_range = max(curve_values) - min(curve_values)
    equity_stddev = statistics.pstdev(curve_values) if len(curve_values) > 1 else 0.0

    return {
        "combo_key": _combo_key(combo_items),
        "members": _combo_members(combo_items),
        "size": len(combo_items),
        "bars": len(curve_values),
        "equity_range": equity_range,
        "equity_stddev": equity_stddev,
        "ending_equity": curve_values[-1],
        "labels": curve_labels,
        "values": [round(value, 2) for value in curve_values],
    }


def _combo_sort_key(result: dict[str, Any]) -> tuple[float, float, float, str]:
    return (
        result["equity_range"],
        result["equity_stddev"],
        abs(result["ending_equity"]),
        result["combo_key"],
    )


def _summarize_combo(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "combo_key": result["combo_key"],
        "members": result["members"],
        "size": result["size"],
        "bars": result["bars"],
        "equity_range": round(result["equity_range"], 2),
        "equity_stddev": round(result["equity_stddev"], 2),
        "ending_equity": round(result["ending_equity"], 2),
    }


def _count_combinations(item_count: int, min_size: int, max_size: int) -> int:
    return sum(math.comb(item_count, size) for size in range(min_size, max_size + 1))


def _run_search(
    basket_name: str,
    basket_items: list[dict[str, str]],
    min_size: int,
    max_size: int,
    available_instruments: list[dict[str, Any]],
    publish: Callable[[dict[str, Any]], None],
) -> dict[str, Any]:
    available_pairs = {(item["symbol"], item["exchange"]) for item in available_instruments}
    conversions, missing_currencies = _build_conversion_map(basket_items, available_pairs)
    if missing_currencies:
        joined = ", ".join(missing_currencies)
        return {
            "error": f"Conversion USD introuvable pour: {joined}.",
            "basket_name": basket_name,
            "basket_items": basket_items,
        }

    required_pairs = {(item["symbol"], item["exchange"]) for item in basket_items}
    for conversion in conversions.values():
        required_pairs.add((conversion["symbol"], conversion["exchange"]))

    publish({
        "type": "progress",
        "current": 0,
        "total": 1,
        "label": "Chargement des séries 1H…",
        "top_rows": [],
    })

    dataset = _load_hourly_series(required_pairs)
    total_combinations = _count_combinations(len(basket_items), min_size, max_size)
    top_results: list[dict[str, Any]] = []
    valid_combinations = 0
    skipped_combinations = 0
    processed = 0

    publish({
        "type": "progress",
        "current": 0,
        "total": total_combinations,
        "label": "Recherche des meilleures combinaisons…",
        "top_rows": [],
    })

    for size in range(min_size, max_size + 1):
        for combo_items in combinations(basket_items, size):
            processed += 1
            evaluated = _evaluate_combo(combo_items, dataset, conversions)
            if evaluated is None:
                skipped_combinations += 1
            else:
                valid_combinations += 1
                top_results.append(evaluated)
                top_results.sort(key=_combo_sort_key)
                top_results = top_results[:_TOP_LIMIT]

            publish({
                "type": "progress",
                "current": processed,
                "total": total_combinations,
                "label": " + ".join(member for member in _combo_members(combo_items)),
                "top_rows": [_summarize_combo(item) for item in top_results],
            })

    if not top_results:
        return {
            "error": "Aucune combinaison exploitable n'a pu être calculée sur l'historique 1H commun.",
            "basket_name": basket_name,
            "basket_items": basket_items,
            "tested_combinations": total_combinations,
            "valid_combinations": 0,
            "skipped_combinations": skipped_combinations,
        }

    best_result = top_results[0]
    return {
        "error": None,
        "basket_name": basket_name,
        "basket_items": basket_items,
        "tested_combinations": total_combinations,
        "valid_combinations": valid_combinations,
        "skipped_combinations": skipped_combinations,
        "min_size": min_size,
        "max_size": max_size,
        "top_combinations": [_summarize_combo(item) for item in top_results],
        "best_combination": _summarize_combo(best_result),
        "best_curve_labels": best_result["labels"],
        "best_curve_values": best_result["values"],
    }


async def _run_search_job(
    job_id: str,
    basket_name: str,
    basket_items: list[dict[str, str]],
    min_size: int,
    max_size: int,
    available_instruments: list[dict[str, Any]],
) -> None:
    queue: asyncio.Queue = _jobs[job_id]["queue"]
    loop = asyncio.get_running_loop()

    def publish(event: dict[str, Any]) -> None:
        asyncio.run_coroutine_threadsafe(queue.put(event), loop).result()

    try:
        result = await loop.run_in_executor(
            None,
            _run_search,
            basket_name,
            basket_items,
            min_size,
            max_size,
            publish,
        )
        result["selected_basket_id"] = basket_id
        _jobs[job_id]["result"] = result
        _jobs[job_id]["result"] = {
            "error": f"Erreur de calcul : {exc}",
            "selected_basket_id": basket_id,
            "basket_name": basket_name,
            "basket_items": basket_items,
            "min_size": min_size,
            "max_size": max_size,
        }
    finally:
        await queue.put({"type": "done", "url": f"/correlation/result/{job_id}"})


def _build_context(
    request: Request,
    *,
    basket_items: list[dict[str, str]] | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
    job_id: str | None = None,
    result: dict[str, Any] | None = None,
    error: str | None = None,
    notice: str | None = None,
) -> dict[str, Any]:
    available_instruments = _get_fx_instruments()
    saved_basket = _get_singleton_basket()

    if basket_items is None:
        basket_items = saved_basket["items"]
    if min_size is None:
        min_size = 2 if len(basket_items) >= 2 else 1
    if max_size is None:
        max_size = len(basket_items) if basket_items else 2
    if max_size < 1:
        max_size = 1

    return {
        "request": request,
        "basket_label": saved_basket["name"],
        "basket_items": basket_items,
        "available_instruments": available_instruments,
        "available_instruments_json": [
            {
                "symbol": item["symbol"],
                "exchange": item["exchange"],
                "label": f"{item['symbol']} - {item['exchange']}",
            }
            for item in available_instruments
        ],
        "min_size": min_size,
        "max_size": max_size,
        "job_id": job_id,
        "result": result,
        "error": error,
        "notice": notice,
    }


@router.get("/", response_class=HTMLResponse)
async def correlation_index(request: Request):
    return templates.TemplateResponse(
        "correlation.html",
        _build_context(request),
    )


@router.post("/basket/items")
async def correlation_basket_add_or_update_item(request: Request):
    form = await request.form()
    symbol = str(form.get("symbol") or "").strip()
    exchange = str(form.get("exchange") or "").strip()
    side = str(form.get("side") or "BUY")

    if not symbol or not exchange:
        return JSONResponse({"ok": False, "message": "Symbole ou marché manquant."}, status_code=400)

    available_instruments = _get_fx_instruments()
    available_lookup = {(item["symbol"], item["exchange"]) for item in available_instruments}
    basket = _get_singleton_basket()
    basket_items, notice, error = _add_or_update_basket_item(
        basket["items"],
        available_lookup,
        symbol,
        exchange,
        side,
    )

    if error:
        return JSONResponse({"ok": False, "message": error}, status_code=400)

    saved_basket = _save_singleton_basket(basket_items)
    return JSONResponse(
        {
            "ok": True,
            "message": notice,
            "basket_items": saved_basket["items"],
        }
    )


@router.post("/basket/items/remove")
async def correlation_basket_remove_item(request: Request):
    form = await request.form()
    symbol = str(form.get("symbol") or "").strip()
    exchange = str(form.get("exchange") or "").strip()

    if not symbol or not exchange:
        return JSONResponse({"ok": False, "message": "Symbole ou marché manquant."}, status_code=400)

    basket = _get_singleton_basket()
    basket_items, notice = _remove_basket_item(basket["items"], symbol, exchange)
    saved_basket = _save_singleton_basket(basket_items)
    return JSONResponse(
        {
            "ok": True,
            "message": notice,
            "basket_items": saved_basket["items"],
        }
    )


@router.post("/run", response_class=HTMLResponse)
async def correlation_run(request: Request):
    form = await request.form()
    available_instruments = _get_fx_instruments()
    available_lookup = {(item["symbol"], item["exchange"]) for item in available_instruments}

    min_size = max(1, _parse_int(form.get("min_size"), 2))
    symbols = form.getlist("item_symbol")
    exchanges = form.getlist("item_exchange")
    sides = form.getlist("item_side")

    basket_items: list[dict[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for symbol, exchange, side in zip(symbols, exchanges, sides):
        key = (str(symbol), str(exchange))
        if key in seen_pairs or key not in available_lookup:
            continue
        seen_pairs.add(key)
        basket_items.append(
            {
                "symbol": key[0],
                "exchange": key[1],
                "side": _normalize_side(str(side)),
            }
        )

    if len(basket_items) < 2:
        return templates.TemplateResponse(
            "correlation.html",
            _build_context(
                request,
                basket_items=basket_items,
                min_size=min_size,
                max_size=max(len(basket_items), 1),
                error="Sélectionnez au moins deux paires pour lancer le calcul.",
            ),
        )

    max_size = _parse_int(form.get("max_size"), len(basket_items))
    max_size = min(max(1, max_size), len(basket_items))
    min_size = min(min_size, max_size)

    saved_basket = _save_singleton_basket(basket_items)

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"queue": asyncio.Queue(), "result": None}
    asyncio.create_task(
        _run_search_job(
            job_id,
            saved_basket["name"],
            [
                {
                    "symbol": item["symbol"],
                    "exchange": item["exchange"],
                    "side": item["side"],
                }
                for item in saved_basket["items"]
            ],
            min_size,
            max_size,
            available_instruments,
        )
    )

    return templates.TemplateResponse(
        "correlation.html",
        _build_context(
            request,
            basket_items=[
                {
                    "symbol": item["symbol"],
                    "exchange": item["exchange"],
                    "side": item["side"],
                }
                for item in saved_basket["items"]
            ],
            min_size=min_size,
            max_size=max_size,
            job_id=job_id,
        ),
    )


@router.get("/progress/{job_id}")
async def correlation_progress(job_id: str):
    async def _not_found():
        yield f"data: {json.dumps({'type': 'error', 'message': 'Job introuvable.'})}\n\n"

    if job_id not in _jobs:
        return StreamingResponse(_not_found(), media_type="text/event-stream")

    async def event_stream():
        queue: asyncio.Queue = _jobs[job_id]["queue"]
        while True:
            event = await queue.get()
            yield f"data: {json.dumps(event)}\n\n"
            if event.get("type") in {"done", "error"}:
                break

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@router.get("/result/{job_id}", response_class=HTMLResponse)
async def correlation_result(request: Request, job_id: str):
    job = _jobs.pop(job_id, None)
    if job is None or job.get("result") is None:
        return templates.TemplateResponse(
            "correlation.html",
            _build_context(request, error="Résultat introuvable ou expiré."),
        )

    result = job["result"]
    return templates.TemplateResponse(
        "correlation.html",
        _build_context(
            request,
            basket_items=result.get("basket_items") or [],
            min_size=result.get("min_size"),
            max_size=result.get("max_size"),
            result=result,
            error=result.get("error"),
        ),
    )