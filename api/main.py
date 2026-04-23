"""FastAPI entry point: wires routes, websocket, logging, and CORS."""

from __future__ import annotations

import os
import sys

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from .routes import router as api_router
from .websocket import router as ws_router


def configure_logging() -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <7}</level> | "
        "<cyan>api</cyan> - <level>{message}</level>",
    )


configure_logging()

app = FastAPI(
    title="Streaming Demo API",
    version="1.0.0",
    description="WebSocket + REST surface for the Spark streaming + batch demo.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
app.include_router(ws_router)


@app.on_event("startup")
async def on_startup() -> None:
    logger.info(f"api starting bronze={os.getenv('BRONZE_PATH')} gold={os.getenv('GOLD_PATH')}")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
