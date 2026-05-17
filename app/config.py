from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
DATABASE_PATH = BASE_DIR / os.getenv("DATABASE_PATH", "data/trading.duckdb")
CACHE_DATABASE_PATH = BASE_DIR / os.getenv("CACHE_DATABASE_PATH", "data/strategy_cache.duckdb")

TIMEFRAMES = [
    {"value": "1min",   "label": "1 minute"},
    {"value": "5min",   "label": "5 minutes"},
    {"value": "15min",  "label": "15 minutes"},
    {"value": "30min",  "label": "30 minutes"},
    {"value": "45min",  "label": "45 minutes"},
    {"value": "1h",     "label": "1 heure"},
    {"value": "2h",     "label": "2 heures"},
    {"value": "4h",     "label": "4 heures"},
    {"value": "1day",   "label": "1 jour"},
    {"value": "1week",  "label": "1 semaine"},
    {"value": "1month", "label": "1 mois"},
]
