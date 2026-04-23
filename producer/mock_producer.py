"""Mock event producer.

Generates OrderEvents and ClickEvents with Faker and publishes them as JSON
to two Kafka topics at a configurable rate.
"""

from __future__ import annotations

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from confluent_kafka import Producer
from faker import Faker
from loguru import logger
from pydantic import BaseModel, Field

# ---------- configuration ----------

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_ORDERS: str = os.getenv("KAFKA_TOPIC_ORDERS", "raw.orders")
KAFKA_TOPIC_CLICKS: str = os.getenv("KAFKA_TOPIC_CLICKS", "raw.clicks")
EVENTS_PER_SECOND: int = int(os.getenv("EVENTS_PER_SECOND", "5"))
LOG_LEVEL: str = os.getenv("PRODUCER_LOG_LEVEL", "INFO")

PRODUCTS: list[str] = [
    "Macbook Pro", "Wireless Mouse", "Noise-Cancelling Headphones",
    "Mechanical Keyboard", "4K Monitor", "USB-C Hub", "Office Chair",
    "Standing Desk", "Smartphone", "Tablet", "Webcam", "Ring Light",
]
ORDER_STATUSES: list[str] = ["created", "paid", "shipped", "delivered", "cancelled"]
PAGES: list[str] = ["/", "/products", "/product/view", "/cart", "/checkout", "/account", "/search"]
ACTIONS: list[str] = ["view", "click", "scroll", "hover", "add_to_cart", "remove_from_cart"]
DEVICES: list[str] = ["desktop", "mobile", "tablet"]

fake = Faker()


# ---------- models ----------


class OrderEvent(BaseModel):
    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    product: str
    amount: float
    currency: str = "USD"
    status: str
    ip: str
    timestamp: str


class ClickEvent(BaseModel):
    session_id: str
    user_id: str
    page: str
    action: str
    device: str
    ip: str
    timestamp: str


# ---------- generators ----------


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_order_event() -> OrderEvent:
    return OrderEvent(
        user_id=fake.user_name(),
        product=random.choice(PRODUCTS),
        amount=round(random.uniform(9.99, 2499.99), 2),
        status=random.choice(ORDER_STATUSES),
        ip=fake.ipv4_public(),
        timestamp=_now_iso(),
    )


def make_click_event() -> ClickEvent:
    return ClickEvent(
        session_id=str(uuid.uuid4()),
        user_id=fake.user_name(),
        page=random.choice(PAGES),
        action=random.choice(ACTIONS),
        device=random.choice(DEVICES),
        ip=fake.ipv4_public(),
        timestamp=_now_iso(),
    )


# ---------- kafka ----------


def _delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        logger.error(f"delivery failed topic={msg.topic()} err={err}")


def build_producer() -> Producer:
    config: dict[str, Any] = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "mock-producer",
        "linger.ms": 20,
        "acks": "1",
    }
    return Producer(config)


def wait_for_kafka(producer: Producer, max_attempts: int = 30) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            producer.list_topics(timeout=3)
            logger.info(f"connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Kafka not ready (attempt {attempt}/{max_attempts}): {exc}")
            time.sleep(2)
    raise RuntimeError("Kafka not reachable")


def publish(producer: Producer, topic: str, key: str, payload: dict[str, Any]) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
        on_delivery=_delivery_report,
    )


# ---------- main loop ----------


def configure_logging() -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        level=LOG_LEVEL,
        format="<green>{time:HH:mm:ss.SSS}</green> | "
        "<level>{level: <7}</level> | "
        "<cyan>producer</cyan> - <level>{message}</level>",
    )


def install_signal_handlers(stop: Callable[[], None]) -> None:
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop())


def run() -> None:
    configure_logging()
    logger.info(
        f"starting producer rate={EVENTS_PER_SECOND}/s "
        f"orders_topic={KAFKA_TOPIC_ORDERS} clicks_topic={KAFKA_TOPIC_CLICKS}"
    )

    producer = build_producer()
    wait_for_kafka(producer)

    keep_running = {"v": True}

    def stop() -> None:
        logger.info("stop signal received")
        keep_running["v"] = False

    install_signal_handlers(stop)

    interval = 1.0 / max(EVENTS_PER_SECOND, 1)
    sent = 0
    t0 = time.time()

    while keep_running["v"]:
        if random.random() < 0.55:
            evt = make_order_event()
            publish(producer, KAFKA_TOPIC_ORDERS, evt.order_id, evt.model_dump())
            logger.info(
                f"order id={evt.order_id[:8]} user={evt.user_id} "
                f"product='{evt.product}' amount={evt.amount} status={evt.status}"
            )
        else:
            evt = make_click_event()
            publish(producer, KAFKA_TOPIC_CLICKS, evt.session_id, evt.model_dump())
            logger.info(
                f"click user={evt.user_id} page={evt.page} "
                f"action={evt.action} device={evt.device}"
            )

        sent += 1
        if sent % 50 == 0:
            producer.flush(2)
            elapsed = time.time() - t0
            logger.info(f"stats sent={sent} elapsed={elapsed:.1f}s rate={sent/elapsed:.2f}/s")

        producer.poll(0)
        time.sleep(interval)

    logger.info("flushing producer before exit")
    producer.flush(5)
    logger.info(f"producer stopped total_sent={sent}")


if __name__ == "__main__":
    run()
