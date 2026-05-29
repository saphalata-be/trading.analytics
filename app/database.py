from collections import deque

import duckdb

from app.config import DATABASE_PATH, CACHE_DATABASE_PATH
from app.trade_direction import DEFAULT_TRADE_DIRECTION

_FX_CURRENCIES = {
    "AED", "AUD", "BGN", "BRL", "CAD", "CHF", "CNH", "CZK", "DKK", "EUR",
    "GBP", "HKD", "HUF", "JPY", "MXN", "NOK", "NZD", "PLN", "RON", "SEK",
    "SGD", "THB", "TRY", "USD", "ZAR",
}


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DATABASE_PATH))


def get_cache_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(CACHE_DATABASE_PATH))


def _is_fx_symbol(symbol: str) -> bool:
    if len(symbol) != 6 or not symbol.isalpha():
        return False
    base_currency = symbol[:3].upper()
    quote_currency = symbol[3:].upper()
    return base_currency in _FX_CURRENCIES and quote_currency in _FX_CURRENCIES


def _compute_historical_position_pct(
    historical_low: float | None,
    historical_high: float | None,
    current_price: float | None,
) -> float | None:
    if historical_low is None or historical_high is None or current_price is None:
        return None
    if historical_high == historical_low:
        return None
    return max(0.0, min(100.0, (current_price - historical_low) / (historical_high - historical_low) * 100))


def _build_currency_conversion_graph(price_rows: list[tuple[str, float]]) -> dict[str, list[tuple[str, float]]]:
    graph: dict[str, list[tuple[str, float]]] = {}
    for symbol, current_price in price_rows:
        if not _is_fx_symbol(symbol) or current_price in (None, 0):
            continue
        base_currency = symbol[:3].upper()
        quote_currency = symbol[3:].upper()
        price = float(current_price)
        graph.setdefault(base_currency, []).append((quote_currency, price))
        graph.setdefault(quote_currency, []).append((base_currency, 1.0 / price))
    return graph


def _find_conversion_factor(
    graph: dict[str, list[tuple[str, float]]],
    source_currency: str,
    target_currency: str,
) -> float | None:
    if source_currency == target_currency:
        return 1.0

    queue: deque[tuple[str, float]] = deque([(source_currency, 1.0)])
    visited = {source_currency}

    while queue:
        currency, factor = queue.popleft()
        for next_currency, rate in graph.get(currency, []):
            if next_currency in visited:
                continue
            next_factor = factor * rate
            if next_currency == target_currency:
                return next_factor
            visited.add(next_currency)
            queue.append((next_currency, next_factor))

    return None


def _compute_atr50_eur_001_lot(
    symbol: str,
    latest_atr50: float | None,
    conversion_graph: dict[str, list[tuple[str, float]]],
) -> float | None:
    if latest_atr50 is None or not _is_fx_symbol(symbol):
        return None

    quote_currency = symbol[3:].upper()
    quote_to_eur = _find_conversion_factor(conversion_graph, quote_currency, "EUR")
    if quote_to_eur is None:
        return None

    return latest_atr50 * 1000.0 * quote_to_eur


def refresh_watchlist_market_metrics(
    con: duckdb.DuckDBPyConnection | None = None,
    *,
    only_missing: bool = False,
) -> None:
    owns_connection = con is None
    if con is None:
        con = get_connection()

    try:
        target_rows = con.execute(
            """
            SELECT id
            FROM watchlist
            WHERE NOT ?
               OR historical_low IS NULL
               OR historical_high IS NULL
               OR current_price IS NULL
               OR historical_position_pct IS NULL
               OR latest_atr50 IS NULL
               OR atr50_eur_001_lot IS NULL
            ORDER BY id
            """,
            [only_missing],
        ).fetchall()
        if not target_rows:
            return

        target_ids = [row[0] for row in target_rows]
        placeholders = ", ".join(["?"] * len(target_ids))
        metrics_rows = con.execute(
            f"""
            WITH target_watchlist AS (
                SELECT id, symbol, exchange
                FROM watchlist
                WHERE id IN ({placeholders})
            ),
            minute_filtered AS (
                SELECT tw.id, tw.symbol, tw.exchange, o.datetime, o.high, o.low, o.close
                FROM target_watchlist tw
                LEFT JOIN ohlcv o
                    ON o.symbol = tw.symbol
                   AND o.exchange = tw.exchange
                   AND o.timeframe = '1min'
            ),
            minute_stats AS (
                SELECT id, MIN(low) AS historical_low, MAX(high) AS historical_high
                FROM minute_filtered
                GROUP BY id
            ),
            latest_minute AS (
                SELECT id, close AS current_price
                FROM (
                    SELECT
                        id,
                        close,
                        ROW_NUMBER() OVER (
                            PARTITION BY id
                            ORDER BY datetime DESC
                        ) AS row_num
                    FROM minute_filtered
                    WHERE datetime IS NOT NULL
                ) ranked
                WHERE row_num = 1
            ),
            daily_ranked AS (
                SELECT
                    tw.id,
                    o.high,
                    o.low,
                    ROW_NUMBER() OVER (
                        PARTITION BY tw.id
                        ORDER BY o.datetime DESC
                    ) AS row_num
                FROM target_watchlist tw
                LEFT JOIN ohlcv o
                    ON o.symbol = tw.symbol
                   AND o.exchange = tw.exchange
                   AND o.timeframe = '1day'
                WHERE o.datetime IS NOT NULL
            ),
            daily_atr AS (
                SELECT
                    id,
                    CASE WHEN COUNT(*) = 50 THEN AVG(high - low) ELSE NULL END AS latest_atr50
                FROM daily_ranked
                WHERE row_num <= 50
                GROUP BY id
            )
            SELECT
                tw.id,
                tw.symbol,
                ms.historical_low,
                ms.historical_high,
                lm.current_price,
                da.latest_atr50
            FROM target_watchlist tw
            LEFT JOIN minute_stats ms ON ms.id = tw.id
            LEFT JOIN latest_minute lm ON lm.id = tw.id
            LEFT JOIN daily_atr da ON da.id = tw.id
            ORDER BY tw.id
            """,
            target_ids,
        ).fetchall()

        current_price_rows = con.execute(
            "SELECT symbol, current_price FROM watchlist WHERE current_price IS NOT NULL"
        ).fetchall()
        current_price_rows.extend(
            (symbol, current_price)
            for _, symbol, _, _, current_price, _ in metrics_rows
            if current_price is not None
        )
        conversion_graph = _build_currency_conversion_graph(current_price_rows)

        for watchlist_id, symbol, historical_low, historical_high, current_price, latest_atr50 in metrics_rows:
            con.execute(
                """
                UPDATE watchlist
                SET historical_low = ?,
                    historical_high = ?,
                    current_price = ?,
                    historical_position_pct = ?,
                    latest_atr50 = ?,
                    atr50_eur_001_lot = ?
                WHERE id = ?
                """,
                [
                    historical_low,
                    historical_high,
                    current_price,
                    _compute_historical_position_pct(historical_low, historical_high, current_price),
                    latest_atr50,
                    _compute_atr50_eur_001_lot(symbol, latest_atr50, conversion_graph),
                    watchlist_id,
                ],
            )
    finally:
        if owns_connection:
            con.close()


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
            preferred_direction VARCHAR DEFAULT 'BOTH',
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
            PRIMARY KEY (basket_id, symbol, exchange, side)
        )
    """)

    correlation_basket_item_pk = [
        row[1]
        for row in con.execute("PRAGMA table_info('correlation_basket_items')").fetchall()
        if row[5]
    ]
    if correlation_basket_item_pk == ["basket_id", "symbol", "exchange"]:
        con.execute("""
            CREATE TABLE correlation_basket_items__new (
                basket_id    INTEGER NOT NULL REFERENCES correlation_baskets(id),
                symbol       VARCHAR NOT NULL,
                exchange     VARCHAR NOT NULL,
                side         VARCHAR NOT NULL,
                position     INTEGER NOT NULL,
                PRIMARY KEY (basket_id, symbol, exchange, side)
            )
        """)
        con.execute("""
            INSERT INTO correlation_basket_items__new (basket_id, symbol, exchange, side, position)
            SELECT basket_id, symbol, exchange, side, position
            FROM correlation_basket_items
        """)
        con.execute("DROP TABLE correlation_basket_items")
        con.execute("ALTER TABLE correlation_basket_items__new RENAME TO correlation_basket_items")

    watchlist_columns = {
        row[1]
        for row in con.execute("PRAGMA table_info('watchlist')").fetchall()
    }
    if "preferred_direction" not in watchlist_columns:
        con.execute(
            f"ALTER TABLE watchlist ADD COLUMN preferred_direction VARCHAR DEFAULT '{DEFAULT_TRADE_DIRECTION}'"
        )
    con.execute(
        "UPDATE watchlist SET preferred_direction = ? WHERE preferred_direction IS NULL",
        [DEFAULT_TRADE_DIRECTION],
    )

    if "historical_low" not in watchlist_columns:
        con.execute("ALTER TABLE watchlist ADD COLUMN historical_low DOUBLE")
    if "historical_high" not in watchlist_columns:
        con.execute("ALTER TABLE watchlist ADD COLUMN historical_high DOUBLE")
    if "current_price" not in watchlist_columns:
        con.execute("ALTER TABLE watchlist ADD COLUMN current_price DOUBLE")
    if "historical_position_pct" not in watchlist_columns:
        con.execute("ALTER TABLE watchlist ADD COLUMN historical_position_pct DOUBLE")
    if "latest_atr50" not in watchlist_columns:
        con.execute("ALTER TABLE watchlist ADD COLUMN latest_atr50 DOUBLE")
    if "atr50_eur_001_lot" not in watchlist_columns:
        con.execute("ALTER TABLE watchlist ADD COLUMN atr50_eur_001_lot DOUBLE")

    refresh_watchlist_market_metrics(con, only_missing=True)

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

    def create_strategy_cache_table() -> None:
        con.execute("""
            CREATE TABLE strategy_cache (
                symbol              VARCHAR NOT NULL,
                exchange            VARCHAR NOT NULL,
                max_levels          INTEGER NOT NULL,
                tp_atr              DOUBLE NOT NULL,
                level_atr           DOUBLE NOT NULL,
                atr_mode            VARCHAR NOT NULL DEFAULT 'd1_1month',
                entry_filter_id     INTEGER NOT NULL DEFAULT 0,
                initial_move_atr    DOUBLE NOT NULL DEFAULT 0.0,
                initial_retrace_atr DOUBLE NOT NULL DEFAULT 0.0,
                computed_at         TIMESTAMP NOT NULL,
                result_json         VARCHAR NOT NULL,
                PRIMARY KEY (
                    symbol,
                    exchange,
                    max_levels,
                    tp_atr,
                    level_atr,
                    atr_mode,
                    entry_filter_id,
                    initial_move_atr,
                    initial_retrace_atr
                )
            )
        """)

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

    strategy_cache_exists = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'strategy_cache'
        """
    ).fetchone()[0] > 0
    expected_strategy_cache_pk = [
        "symbol",
        "exchange",
        "max_levels",
        "tp_atr",
        "level_atr",
        "atr_mode",
        "entry_filter_id",
        "initial_move_atr",
        "initial_retrace_atr",
    ]

    if not strategy_cache_exists:
        create_strategy_cache_table()
    else:
        strategy_cache_info = con.execute("PRAGMA table_info('strategy_cache')").fetchall()
        strategy_cache_pk = [row[1] for row in strategy_cache_info if row[5]]
        strategy_cache_columns = {row[1] for row in strategy_cache_info}
        expected_strategy_cache_columns = {
            "symbol",
            "exchange",
            "max_levels",
            "tp_atr",
            "level_atr",
            "atr_mode",
            "entry_filter_id",
            "initial_move_atr",
            "initial_retrace_atr",
            "computed_at",
            "result_json",
        }
        if (
            strategy_cache_pk != expected_strategy_cache_pk
            or not expected_strategy_cache_columns.issubset(strategy_cache_columns)
        ):
            con.execute("DROP TABLE strategy_cache")
            create_strategy_cache_table()

    con.close()


def reset_strategy_cache() -> None:
    con = get_cache_connection()
    try:
        con.execute("DELETE FROM strategy_cache")
    finally:
        con.close()
