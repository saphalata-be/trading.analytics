from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from app.config import HISTORY_FILES_EXCHANGE, HISTORY_FILES_PATH
from app.database import (
    get_cache_connection,
    get_connection,
    refresh_watchlist_market_metrics,
    reset_strategy_cache,
    reset_trading_tables,
)
from app.trade_direction import DEFAULT_TRADE_DIRECTION, normalize_trade_direction

_TIMEFRAME_MAP = {
    "1m": "1min",
    "1h": "1h",
    "1d": "1day",
}

_TIMEFRAME_ORDER = {"1min": 0, "1h": 1, "1day": 2}


@dataclass(frozen=True)
class HistoryFile:
    symbol: str
    timeframe: str
    source_path: Path
    instrument_type: str


@dataclass(frozen=True)
class ImportSummary:
    instruments: int
    timeframes: int
    rows: int
    skipped_symbols: tuple[str, ...] = ()
    update_mode: bool = False


class HistoryFilesError(Exception):
    pass


def _infer_instrument_type(symbol: str) -> str:
    if len(symbol) == 6 and symbol.isalpha():
        return "Forex"
    if "IDX" in symbol:
        return "Index"
    if "CMD" in symbol:
        return "Commodity"
    return ""


def _parse_history_filename(file_path: Path) -> HistoryFile | None:
    stem = file_path.stem
    if "_" not in stem:
        return None

    symbol, raw_timeframe = stem.rsplit("_", 1)
    timeframe = _TIMEFRAME_MAP.get(raw_timeframe.lower())
    if not symbol or timeframe is None:
        return None

    return HistoryFile(
        symbol=symbol.upper(),
        timeframe=timeframe,
        source_path=file_path,
        instrument_type=_infer_instrument_type(symbol.upper()),
    )


def scan_history_files() -> list[HistoryFile]:
    if not HISTORY_FILES_PATH.exists():
        raise HistoryFilesError(
            f"Répertoire introuvable: {HISTORY_FILES_PATH}"
        )

    history_files: list[HistoryFile] = []
    for file_path in HISTORY_FILES_PATH.glob("*.csv"):
        parsed = _parse_history_filename(file_path)
        if parsed is not None:
            history_files.append(parsed)

    history_files.sort(key=lambda item: (item.symbol, _TIMEFRAME_ORDER.get(item.timeframe, 99), item.timeframe))
    return history_files


def load_ic_markets_symbol_names() -> set[str]:
    con = get_cache_connection()
    try:
        rows = con.execute("SELECT name FROM mt5_symbols").fetchall()
    finally:
        con.close()

    return {str(row[0]).upper() for row in rows}


def reset_imported_data() -> None:
    reset_trading_tables()
    reset_strategy_cache()


def _invalidate_strategy_cache_for_symbols(symbols: set[str]) -> None:
    if not symbols:
        return

    con = get_cache_connection()
    try:
        con.executemany(
            "DELETE FROM strategy_cache WHERE symbol = ? AND exchange = ?",
            [(symbol, HISTORY_FILES_EXCHANGE) for symbol in sorted(symbols)],
        )
    finally:
        con.close()


def _load_existing_preferred_directions() -> dict[tuple[str, str], str]:
    con = get_connection()
    try:
        rows = con.execute(
            "SELECT symbol, exchange, preferred_direction FROM watchlist"
        ).fetchall()
    except Exception:  # noqa: BLE001
        return {}
    finally:
        con.close()

    return {
        (symbol, exchange): normalize_trade_direction(preferred_direction)
        for symbol, exchange, preferred_direction in rows
    }


def _delete_symbols_from_trading_tables(symbols: set[str]) -> None:
    if not symbols:
        return

    con = get_connection()
    try:
        rows = con.execute(
            """
            SELECT id
            FROM watchlist
            WHERE exchange = ? AND symbol IN (SELECT unnest(?))
            """,
            [HISTORY_FILES_EXCHANGE, sorted(symbols)],
        ).fetchall()
        watchlist_ids = [row[0] for row in rows]

        if watchlist_ids:
            con.execute(
                "DELETE FROM watchlist_timeframes WHERE watchlist_id IN (SELECT unnest(?))",
                [watchlist_ids],
            )
        con.execute(
            "DELETE FROM watchlist WHERE exchange = ? AND symbol IN (SELECT unnest(?))",
            [HISTORY_FILES_EXCHANGE, sorted(symbols)],
        )
        con.execute(
            "DELETE FROM instruments WHERE exchange = ? AND symbol IN (SELECT unnest(?))",
            [HISTORY_FILES_EXCHANGE, sorted(symbols)],
        )
        con.execute(
            "DELETE FROM ohlcv WHERE exchange = ? AND symbol IN (SELECT unnest(?))",
            [HISTORY_FILES_EXCHANGE, sorted(symbols)],
        )
    finally:
        con.close()


def _next_watchlist_id(con) -> int:
    max_id = con.execute("SELECT COALESCE(MAX(id), 0) FROM watchlist").fetchone()[0]
    return int(max_id) + 1


def import_history_files(
    progress_callback: Callable[[int, int, str], None] | None = None,
    *,
    update_mode: bool = False,
) -> ImportSummary:
    history_files = scan_history_files()
    if not history_files:
        raise HistoryFilesError(
            f"Aucun fichier CSV reconnu dans {HISTORY_FILES_PATH}"
        )

    ic_markets_symbols = load_ic_markets_symbol_names()
    if not ic_markets_symbols:
        raise HistoryFilesError(
            "Liste IC Markets vide. Actualisez les symboles IC Markets avant d'importer."
        )

    source_symbols = {item.symbol for item in history_files}
    skipped_symbols = tuple(sorted(source_symbols - ic_markets_symbols))
    importable_files = [
        item for item in history_files
        if item.symbol in ic_markets_symbols
    ]
    if not importable_files:
        raise HistoryFilesError(
            "Aucun fichier CSV ne correspond aux symboles disponibles chez IC Markets."
        )

    preferred_directions = _load_existing_preferred_directions()
    if update_mode:
        _delete_symbols_from_trading_tables(source_symbols)
        _invalidate_strategy_cache_for_symbols(source_symbols)
    else:
        reset_imported_data()

    con = get_connection()
    try:
        grouped: dict[str, list[HistoryFile]] = {}
        for item in importable_files:
            grouped.setdefault(item.symbol, []).append(item)

        total_rows = 0
        next_watchlist_id = _next_watchlist_id(con)
        for offset, symbol in enumerate(sorted(grouped)):
            watchlist_id = next_watchlist_id + offset
            files_for_symbol = grouped[symbol]
            instrument_type = next((item.instrument_type for item in files_for_symbol if item.instrument_type), "")

            con.execute(
                """
                INSERT INTO instruments (symbol, name, type, currency, exchange, country)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [symbol, symbol, instrument_type, "", HISTORY_FILES_EXCHANGE, ""],
            )
            con.execute(
                """
                INSERT INTO watchlist (id, symbol, exchange, instrument_type, preferred_direction)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    watchlist_id,
                    symbol,
                    HISTORY_FILES_EXCHANGE,
                    instrument_type,
                    preferred_directions.get(
                        (symbol, HISTORY_FILES_EXCHANGE),
                        DEFAULT_TRADE_DIRECTION,
                    ),
                ],
            )

            for file_index, item in enumerate(files_for_symbol, start=1):
                current_index = sum(len(grouped[s]) for s in sorted(grouped) if s < symbol) + file_index
                if progress_callback is not None:
                    progress_callback(current_index, len(importable_files), f"Import {item.symbol} {item.timeframe}")

                csv_path = item.source_path.as_posix().replace("'", "''")
                con.execute(
                    f"""
                    INSERT INTO ohlcv (symbol, exchange, timeframe, datetime, open, high, low, close, volume)
                    SELECT
                        ?,
                        ?,
                        ?,
                        STRPTIME(Date || ' ' || \"Timestamp\", '%Y%m%d %H:%M:%S'),
                        CAST(Open AS DOUBLE),
                        CAST(High AS DOUBLE),
                        CAST(Low AS DOUBLE),
                        CAST(Close AS DOUBLE),
                        CAST(Volume AS DOUBLE)
                    FROM read_csv_auto('{csv_path}', HEADER = TRUE, ALL_VARCHAR = TRUE)
                    """,
                    [item.symbol, HISTORY_FILES_EXCHANGE, item.timeframe],
                )

                first_date, last_date, bars = con.execute(
                    """
                    SELECT MIN(datetime), MAX(datetime), COUNT(*)
                    FROM ohlcv
                    WHERE symbol = ? AND exchange = ? AND timeframe = ?
                    """,
                    [item.symbol, HISTORY_FILES_EXCHANGE, item.timeframe],
                ).fetchone()
                total_rows += bars
                con.execute(
                    """
                    INSERT INTO watchlist_timeframes (
                        watchlist_id, timeframe, first_date, last_date, total_bars, last_download, status
                    )
                    VALUES (?, ?, ?, ?, ?, current_timestamp, 'done')
                    """,
                    [watchlist_id, item.timeframe, first_date, last_date, bars],
                )

        refresh_watchlist_market_metrics(con)
    finally:
        con.close()

    return ImportSummary(
        instruments=len({item.symbol for item in importable_files}),
        timeframes=len(importable_files),
        rows=total_rows,
        skipped_symbols=skipped_symbols,
        update_mode=update_mode,
    )
