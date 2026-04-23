"""REST routes serving Bronze / Silver counts and Gold aggregates via DuckDB.

We use DuckDB with the ``delta`` extension so the API never needs a running
Spark JVM just to read aggregate tables. This keeps the container light and
responses fast.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

import duckdb
from fastapi import APIRouter, HTTPException
from loguru import logger

router = APIRouter()

BRONZE_PATH: str = os.getenv("BRONZE_PATH", "/data/bronze")
SILVER_PATH: str = os.getenv("SILVER_PATH", "/data/silver")
GOLD_PATH: str = os.getenv("GOLD_PATH", "/data/gold")


@lru_cache(maxsize=1)
def _duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL delta; LOAD delta;")
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"duckdb delta extension load failed err={exc}")
    return con


def _read_delta(path: str) -> list[dict[str, Any]]:
    if not os.path.isdir(path):
        return []
    try:
        con = _duck()
        cursor = con.execute(f"SELECT * FROM delta_scan('{path}')")
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"delta_scan failed path={path} err={exc}")
        return []


def _count_delta(path: str) -> int:
    if not os.path.isdir(path):
        return 0
    try:
        con = _duck()
        result = con.execute(f"SELECT COUNT(*) FROM delta_scan('{path}')").fetchone()
        return int(result[0]) if result else 0
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"delta count failed path={path} err={exc}")
        return 0


@router.get("/stats/orders")
async def orders_stats() -> dict[str, Any]:
    return {
        "revenue_per_product": _read_delta(os.path.join(GOLD_PATH, "orders_revenue_per_product")),
        "orders_by_status": _read_delta(os.path.join(GOLD_PATH, "orders_by_status")),
        "revenue_per_minute": _read_delta(os.path.join(GOLD_PATH, "orders_revenue_per_minute")),
    }


@router.get("/stats/clicks")
async def clicks_stats() -> dict[str, Any]:
    return {
        "top_pages": _read_delta(os.path.join(GOLD_PATH, "clicks_top_pages")),
        "top_actions": _read_delta(os.path.join(GOLD_PATH, "clicks_top_actions")),
        "events_per_device": _read_delta(os.path.join(GOLD_PATH, "clicks_events_per_device")),
    }


@router.get("/bronze/count")
async def bronze_count() -> dict[str, int]:
    return {
        "orders": _count_delta(os.path.join(BRONZE_PATH, "orders")),
        "clicks": _count_delta(os.path.join(BRONZE_PATH, "clicks")),
    }


@router.get("/silver/count")
async def silver_count() -> dict[str, int]:
    return {
        "orders": _count_delta(os.path.join(SILVER_PATH, "orders")),
        "clicks": _count_delta(os.path.join(SILVER_PATH, "clicks")),
    }


@router.get("/gold/tables")
async def gold_tables() -> dict[str, bool]:
    if not os.path.isdir(GOLD_PATH):
        return {}
    out: dict[str, bool] = {}
    for entry in sorted(os.listdir(GOLD_PATH)):
        full = os.path.join(GOLD_PATH, entry)
        if os.path.isdir(full):
            out[entry] = os.path.isdir(os.path.join(full, "_delta_log"))
    return out


@router.get("/layers/summary")
async def layers_summary() -> dict[str, Any]:
    bronze = {
        "orders": _count_delta(os.path.join(BRONZE_PATH, "orders")),
        "clicks": _count_delta(os.path.join(BRONZE_PATH, "clicks")),
    }
    silver = {
        "orders": _count_delta(os.path.join(SILVER_PATH, "orders")),
        "clicks": _count_delta(os.path.join(SILVER_PATH, "clicks")),
    }
    return {
        "bronze": bronze,
        "silver": silver,
        "bronze_total": bronze["orders"] + bronze["clicks"],
        "silver_total": silver["orders"] + silver["clicks"],
    }
