import duckdb
from app.config import DATABASE_PATH


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DATABASE_PATH))


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

    con.close()
