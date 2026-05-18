# AGENTS.md — trading.analytics

## Start the app

```bash
python run.py
# or
uvicorn app.main:app --reload
```

Runs on `http://127.0.0.1:8000`. Root redirects to `/data/`.  
Python interpreter is managed by **Miniconda** (`C:/Users/David/miniconda3`). Minimum Python **3.10** (uses `str | None` union syntax, built-in generics).

## Environment

Python environment is located here : "C:\Users\David\miniconda3"

Copy `.env.example` to `.env` before first run:

```
DATABASE_PATH=data/trading.duckdb   # optional, this is the default
HISTORY_FILES_PATH=E:/Forex/History/TickStory
HISTORY_FILES_EXCHANGE=TickStory
```

## Architecture

Single-package FastAPI app. All routes live under the `/data` prefix in one file.

```
app/
  main.py                  # FastAPI app, startup hook calls init_db()
  config.py                # Loads .env, exposes DB paths + TickStory source settings
  database.py              # DuckDB connect + init_db() — creates 4 tables on startup
  routers/
    data_management.py     # Data page: TickStory source summary + full reimport
  services/
    history_files.py       # TickStory scan + full CSV import into DuckDB
  templates/
    base.html              # Tailwind CDN + HTMX CDN, dark theme
    partials/watchlist.html  # Imported symbols/timeframes summary
```

## Database

**DuckDB** file at `data/trading.duckdb` (no ORM, raw SQL). Tables:

| Table | Key |
|---|---|
| `instruments` | `(symbol, exchange)` |
| `watchlist` | `id` (sequence `watchlist_id_seq`) |
| `watchlist_timeframes` | `(watchlist_id, timeframe)` — tracks imported coverage and date range |
| `ohlcv` | `(symbol, exchange, timeframe, datetime)` |

`get_connection()` opens a **new connection every call** — always `.close()` it. DuckDB is file-based; concurrent writes from multiple connections can conflict.

## TickStory import (`app/services/history_files.py`)

- Reads all `*.csv` files in `HISTORY_FILES_PATH` matching `<symbol>_<timeframe>.csv`.
- Supported filename suffixes: `1m -> 1min`, `1H -> 1h`, `1D -> 1day`.
- A full reimport clears `watchlist`, `watchlist_timeframes`, `instruments`, `ohlcv`, and `strategy_cache` before loading fresh data.
- CSV ingestion is delegated to DuckDB via `read_csv_auto(...)`, so data is loaded directly into `ohlcv` without buffering the full file set in Python.

## Data management (`app/routers/data_management.py`)

- The `/data/` page no longer offers symbol search or per-timeframe downloads.
- `POST /data/sync` performs a full database refresh from the TickStory directory and returns the updated summary page.

## Frontend

- **HTMX** (CDN, 1.9.12) for partial page updates — no JS build step.
- **Tailwind CSS** (CDN) — no build step.
- UI language is **French**.

## No tests, no CI, no linting

There are no test files, no `.github/workflows/`, no pre-commit hooks, and no linter/formatter config. Verify changes by running the app manually.

## Install dependencies

```bash
pip install -r requirements.txt
```
