"""
Router for the IC Markets / MetaTrader 5 symbols page.
"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.database import get_cache_connection

router = APIRouter(prefix="/icmarkets", tags=["icmarkets"])
templates = Jinja2Templates(directory="app/templates")

_SWAP_MODE_LABELS: dict[int, str] = {
    0: "Désactivé",
    1: "Points",
    2: "Devise de base",
    3: "Devise de marge",
    4: "Devise du dépôt",
    5: "Intérêts (cours actuel)",
    6: "Intérêts (prix d'ouverture)",
    7: "Réouverture (clôture)",
    8: "Réouverture (bid)",
}


def _row_to_dict(r) -> dict:
    swap_mode = r[4]
    return {
        "name": r[0],
        "path": r[1],
        "description": r[2],
        "volume_min": r[3],
        "swap_mode": swap_mode,
        "swap_mode_label": _SWAP_MODE_LABELS.get(swap_mode, str(swap_mode)),
        "swap_long": r[5],
        "swap_short": r[6],
        "updated_at": r[7],
    }


def _group_symbols(symbols: list[dict]) -> dict[str, list[dict]]:
    """Group symbols by the first segment of their MT5 path."""
    groups: dict[str, list[dict]] = {}
    for s in symbols:
        raw_path = s.get("path") or "Autre"
        category = raw_path.replace("\\", "/").split("/")[0].strip() or "Autre"
        groups.setdefault(category, []).append(s)
    return dict(sorted(groups.items()))


@router.get("/", response_class=HTMLResponse)
async def icmarkets_page(request: Request):
    con = get_cache_connection()
    rows = con.execute(
        "SELECT name, path, description, volume_min, swap_mode, swap_long, swap_short, updated_at"
        " FROM mt5_symbols ORDER BY path, name"
    ).fetchall()
    con.close()

    symbols = [_row_to_dict(r) for r in rows]
    groups = _group_symbols(symbols)
    updated_at = rows[0][7] if rows else None

    return templates.TemplateResponse(
        "icmarkets.html",
        {
            "request": request,
            "groups": groups,
            "symbol_count": len(symbols),
            "updated_at": updated_at,
        },
    )


@router.post("/refresh", response_class=HTMLResponse)
async def icmarkets_refresh(request: Request):
    """Connect to MT5, fetch all symbols, persist to DB, return updated partial."""
    try:
        import MetaTrader5 as mt5
    except ImportError:
        return HTMLResponse(
            '<p class="text-red-400 p-4">Erreur : bibliothèque MetaTrader5 non installée.</p>',
            status_code=500,
        )

    if not mt5.initialize():
        err = mt5.last_error()
        mt5.shutdown()
        return HTMLResponse(
            f'<p class="text-red-400 p-4">Échec de connexion à MetaTrader 5 : {err}</p>',
            status_code=500,
        )

    try:
        raw_symbols = mt5.symbols_get()
        if raw_symbols is None:
            raise RuntimeError("Aucun symbole retourné par MetaTrader 5.")

        now = datetime.utcnow()
        insert_rows = [
            (
                s.name,
                getattr(s, "path", ""),
                getattr(s, "description", ""),
                s.volume_min,
                s.swap_mode,
                s.swap_long,
                s.swap_short,
                now,
            )
            for s in raw_symbols
        ]
    except Exception as exc:
        mt5.shutdown()
        return HTMLResponse(
            f'<p class="text-red-400 p-4">Erreur lors de la récupération des symboles : {exc}</p>',
            status_code=500,
        )
    finally:
        mt5.shutdown()

    con = get_cache_connection()
    con.execute("DELETE FROM mt5_symbols")
    con.executemany(
        "INSERT INTO mt5_symbols"
        " (name, path, description, volume_min, swap_mode, swap_long, swap_short, updated_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        insert_rows,
    )
    db_rows = con.execute(
        "SELECT name, path, description, volume_min, swap_mode, swap_long, swap_short, updated_at"
        " FROM mt5_symbols ORDER BY path, name"
    ).fetchall()
    con.close()

    symbols = [_row_to_dict(r) for r in db_rows]
    groups = _group_symbols(symbols)

    return templates.TemplateResponse(
        "partials/icmarkets_symbols.html",
        {
            "request": request,
            "groups": groups,
            "symbol_count": len(symbols),
            "updated_at": now,
        },
    )
