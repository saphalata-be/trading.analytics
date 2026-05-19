import duckdb
from app.config import DATABASE_PATH, CACHE_DATABASE_PATH


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DATABASE_PATH))


def get_cache_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(CACHE_DATABASE_PATH))


def init_db() -> None:
    """Create tables if they don't exist."""
    con = get_connection()
    con.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            symbol      VARCHAR NOT NULL,
            name        VARCHAR,
            type        VARCHAR,   -- forex, index, commodity, etf, stock, ...
            currency    VARCHAR,
            exchange    VARCHAR,
            country     VARCHAR,
            PRIMARY KEY (symbol, exchange)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            id              INTEGER PRIMARY KEY,
            symbol          VARCHAR NOT NULL,
            exchange        VARCHAR NOT NULL,
            instrument_type VARCHAR,
            added_at        TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS watchlist_id_seq START 1
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS watchlist_timeframes (
            watchlist_id    INTEGER NOT NULL REFERENCES watchlist(id),
            timeframe       VARCHAR NOT NULL,
            first_date      TIMESTAMP,
            last_date       TIMESTAMP,
            total_bars      BIGINT DEFAULT 0,
            last_download   TIMESTAMP,
            status          VARCHAR DEFAULT 'pending',  -- pending, downloading, done, error
            PRIMARY KEY (watchlist_id, timeframe)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            symbol      VARCHAR NOT NULL,
            exchange    VARCHAR NOT NULL,
            timeframe   VARCHAR NOT NULL,
            datetime    TIMESTAMP NOT NULL,
            open        DOUBLE,
            high        DOUBLE,
            low         DOUBLE,
            close       DOUBLE,
            volume      DOUBLE,
            PRIMARY KEY (symbol, exchange, timeframe, datetime)
        )
    """)

    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS correlation_basket_id_seq START 1
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS correlation_baskets (
            id          INTEGER PRIMARY KEY,
            name        VARCHAR NOT NULL UNIQUE,
            created_at  TIMESTAMP DEFAULT current_timestamp,
            updated_at  TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS correlation_basket_items (
            basket_id    INTEGER NOT NULL REFERENCES correlation_baskets(id),
            symbol       VARCHAR NOT NULL,
            exchange     VARCHAR NOT NULL,
            side         VARCHAR NOT NULL,
            position     INTEGER NOT NULL,
            PRIMARY KEY (basket_id, symbol, exchange)
        )
    """)

    con.close()


def reset_trading_tables() -> None:
    con = get_connection()
    try:
        con.execute("DELETE FROM watchlist_timeframes")
        con.execute("DELETE FROM watchlist")
        con.execute("DELETE FROM instruments")
        con.execute("DELETE FROM ohlcv")
        con.execute("DROP TABLE IF EXISTS strategy_cache")
    finally:
        con.close()


def init_cache_db() -> None:
    """Create strategy_cache table in the dedicated cache DB.
    Legacy migration from trading.duckdb is intentionally disabled."""
    con = get_cache_connection()
    con.execute("""
        CREATE TABLE IF NOT EXISTS mt5_symbols (
            name        VARCHAR PRIMARY KEY,
            path        VARCHAR,
            description VARCHAR,
            volume_min  DOUBLE,
            swap_mode   INTEGER,
            swap_long   DOUBLE,
            swap_short  DOUBLE,
            updated_at  TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS strategy_cache (
            symbol      VARCHAR NOT NULL,
            exchange    VARCHAR NOT NULL,
            max_levels  INTEGER NOT NULL,
            tp_atr      DOUBLE NOT NULL,
            level_atr   DOUBLE NOT NULL,
            computed_at TIMESTAMP NOT NULL,
            result_json VARCHAR NOT NULL,
            PRIMARY KEY (symbol, exchange, max_levels, tp_atr, level_atr)
        )
    """)

    con.close()


def reset_strategy_cache() -> None:
    con = get_cache_connection()
    try:
        con.execute("DELETE FROM strategy_cache")
    finally:
        con.close()
