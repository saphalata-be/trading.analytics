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

    con.close()


def init_cache_db() -> None:
    """Create strategy_cache table in the dedicated cache DB.
    On first run, migrates existing rows from trading.duckdb if any."""
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

    # One-time migration: copy rows from trading.duckdb if the cache DB is empty
    count = con.execute("SELECT COUNT(*) FROM strategy_cache").fetchone()[0]
    if count == 0:
        try:
            old_con = get_connection()
            rows = old_con.execute(
                "SELECT symbol, exchange, max_levels, tp_atr, level_atr, computed_at, result_json"
                " FROM strategy_cache"
            ).fetchall()
            old_con.close()
            if rows:
                con.executemany("INSERT INTO strategy_cache VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
                print(f"[init_cache_db] {len(rows)} entrées migrées depuis trading.duckdb")
        except Exception as exc:
            print(f"[init_cache_db] Migration ignorée : {exc}")

    con.close()
