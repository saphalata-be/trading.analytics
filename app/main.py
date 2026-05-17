from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from app.database import init_db, init_cache_db
from app.routers import data_management, strategy, icmarkets

app = FastAPI(title="Trading Analytics")

app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(data_management.router)
app.include_router(strategy.router)
app.include_router(icmarkets.router)


def _fmt_duration(minutes: float) -> str:
    """Format a duration in minutes as Xh YYm or YYm."""
    m = int(round(minutes))
    if m >= 60:
        return f"{m // 60}h {m % 60:02d}m"
    return f"{m}m"


# Make helper available in all Jinja2 templates
from fastapi.templating import Jinja2Templates as _J2T  # noqa: E402
_all_templates = _J2T(directory="app/templates")
_all_templates.env.globals["_fmt_duration"] = _fmt_duration

# Patch the templates instance used by each router
data_management.templates.env.globals["_fmt_duration"] = _fmt_duration
strategy.templates.env.globals["_fmt_duration"] = _fmt_duration
icmarkets.templates.env.globals["_fmt_duration"] = _fmt_duration


@app.on_event("startup")
async def startup():
    init_db()
    init_cache_db()


@app.get("/")
async def root():
    return RedirectResponse(url="/data/")
