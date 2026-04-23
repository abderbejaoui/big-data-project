"""WebSocket hub: receives internal batch summaries and fans them out to clients."""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from loguru import logger
from pydantic import BaseModel, Field

router = APIRouter()


class BatchEvent(BaseModel):
    type: str = Field(default="batch")
    batch_id: int
    topic: str
    records: int
    timestamp: str
    sample: list[dict[str, Any]] = Field(default_factory=list)


class ConnectionManager:
    """In-process pub/sub for WebSocket clients plus a small replay buffer."""

    def __init__(self, buffer_size: int = 100) -> None:
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()
        self._recent: deque[dict[str, Any]] = deque(maxlen=buffer_size)
        self.total_events: int = 0
        self.last_batch_at: str | None = None

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._clients.add(ws)
        logger.info(f"ws client connected total={len(self._clients)}")

        for msg in list(self._recent):
            try:
                await ws.send_json(msg)
            except Exception:  # noqa: BLE001
                break

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(ws)
        logger.info(f"ws client disconnected total={len(self._clients)}")

    async def broadcast(self, payload: dict[str, Any]) -> None:
        self._recent.append(payload)
        self.total_events += int(payload.get("records", 0))
        self.last_batch_at = payload.get("timestamp") or datetime.now(timezone.utc).isoformat()

        async with self._lock:
            targets = list(self._clients)

        if not targets:
            return

        dead: list[WebSocket] = []
        for ws in targets:
            try:
                await ws.send_json(payload)
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"ws send failed err={exc}")
                dead.append(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    self._clients.discard(ws)

    def snapshot(self) -> dict[str, Any]:
        return {
            "connected_clients": len(self._clients),
            "total_events": self.total_events,
            "last_batch_at": self.last_batch_at,
        }


manager = ConnectionManager()


@router.websocket("/ws/stream")
async def ws_stream(ws: WebSocket) -> None:
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(ws)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"ws loop closed err={exc}")
        await manager.disconnect(ws)


@router.post("/internal/batch-event")
async def ingest_batch_event(request: Request) -> dict[str, Any]:
    """Called by the Spark streaming job after every micro-batch."""
    body = await request.json()
    try:
        evt = BatchEvent(**body)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"bad batch event payload err={exc} body={body}")
        return {"ok": False, "error": str(exc)}

    await manager.broadcast(evt.model_dump())
    logger.debug(f"broadcast topic={evt.topic} batch_id={evt.batch_id} records={evt.records}")
    return {"ok": True, "clients": len(manager._clients)}  # noqa: SLF001


@router.get("/stream/stats")
async def stream_stats() -> dict[str, Any]:
    return manager.snapshot()
