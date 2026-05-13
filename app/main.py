from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse

from app.database import init_db
from app.routers import data_management

app = FastAPI(title="Trading Analytics")

app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(data_management.router)


@app.on_event("startup")
async def startup():
    init_db()


@app.get("/")
async def root():
    return RedirectResponse(url="/data/")
