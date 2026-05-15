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
TWELVEDATA_API_KEY=your_api_key_here
DATABASE_PATH=data/trading.duckdb   # optional, this is the default
```

`TWELVEDATA_API_KEY` is required for instrument search and OHLCV downloads.

## Architecture

Single-package FastAPI app. All routes live under the `/data` prefix in one file.

```
app/
  main.py                  # FastAPI app, startup hook calls init_db()
  config.py                # Loads .env, exposes TWELVEDATA_API_KEY, DATABASE_PATH, TIMEFRAMES
  database.py              # DuckDB connect + init_db() — creates 4 tables on startup
  routers/
    data_management.py     # All routes: watchlist CRUD, search, SSE download queue
  services/
    twelvedata.py          # TwelveData HTTP client (httpx, not the twelvedata SDK)
  templates/
    base.html              # Tailwind CDN + HTMX CDN, dark theme
    partials/watchlist.html  # SSE download JS lives here (inline <script>)
```

## Database

**DuckDB** file at `data/trading.duckdb` (no ORM, raw SQL). Tables:

| Table | Key |
|---|---|
| `instruments` | `(symbol, exchange)` |
| `watchlist` | `id` (sequence `watchlist_id_seq`) |
| `watchlist_timeframes` | `(watchlist_id, timeframe)` — tracks download status and date range |
| `ohlcv` | `(symbol, exchange, timeframe, datetime)` |

`get_connection()` opens a **new connection every call** — always `.close()` it. DuckDB is file-based; concurrent writes from multiple connections can conflict.

## TwelveData service (`app/services/twelvedata.py`)

- Uses **`httpx`** directly — the `twelvedata==1.2.11` SDK in `requirements.txt` is unused.
- Rate limit: 1.5 s between requests (`_REQUEST_INTERVAL`), enforced via a module-level `time.sleep`.
- `fetch_full_history()` paginates backwards (`order=DESC`, `end_date` cursor), 5000 bars/page.
- History is capped at **2010-01-01** (`HISTORY_START_DATE`). Pass `start_date=` for incremental updates.
- No retry logic, no 429 handling — a `TwelveDataError` is raised on API errors.

## Download queue (`app/routers/data_management.py`)

Downloads are **sequential**: a single `asyncio.Queue` (`_download_queue`) feeds one background worker (`_queue_worker`), started lazily on the first download request via `asyncio.create_task`.

Each `DownloadJob` carries its own `asyncio.Queue` for SSE events. The SSE endpoint (`GET /data/watchlist/{id}/download/{tf}`) enqueues the job immediately and streams events as the worker produces them.

Before fetching, the worker reads `last_date` from `watchlist_timeframes`:
- If present → incremental fetch from that date.
- If absent → full fetch from 2010-01-01.

After saving, `first_date`/`last_date`/`total_bars` are recomputed from the full `ohlcv` table (not just the new batch).

## Frontend

- **HTMX** (CDN, 1.9.12) for partial page updates — no JS build step.
- **Tailwind CSS** (CDN) — no build step.
- UI language is **French**.
- SSE progress handling is in an inline `<script>` at the bottom of `app/templates/partials/watchlist.html`.

## No tests, no CI, no linting

There are no test files, no `.github/workflows/`, no pre-commit hooks, and no linter/formatter config. Verify changes by running the app manually.

## Install dependencies

```bash
pip install -r requirements.txt
```
