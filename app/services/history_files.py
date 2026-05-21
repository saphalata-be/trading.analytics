from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from app.config import HISTORY_FILES_EXCHANGE, HISTORY_FILES_PATH
from app.database import get_connection, reset_strategy_cache, reset_trading_tables
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


def reset_imported_data() -> None:
    reset_trading_tables()
    reset_strategy_cache()


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


def import_history_files(
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> ImportSummary:
    history_files = scan_history_files()
    if not history_files:
        raise HistoryFilesError(
            f"Aucun fichier CSV reconnu dans {HISTORY_FILES_PATH}"
        )

    preferred_directions = _load_existing_preferred_directions()
    reset_imported_data()

    con = get_connection()
    try:
        grouped: dict[str, list[HistoryFile]] = {}
        for item in history_files:
            grouped.setdefault(item.symbol, []).append(item)

        total_rows = 0
        for watchlist_id, symbol in enumerate(sorted(grouped), start=1):
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
                    progress_callback(current_index, len(history_files), f"Import {item.symbol} {item.timeframe}")

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
    finally:
        con.close()

    return ImportSummary(
        instruments=len({item.symbol for item in history_files}),
        timeframes=len(history_files),
        rows=total_rows,
    )
