"""Spark Structured Streaming: Kafka → Bronze Delta + live WebSocket push.

Runs two Kafka streams (orders, clicks) concurrently. Each micro-batch:
  1. writes raw events to a Delta Bronze table
  2. POSTs a small JSON summary to the FastAPI backend, which fans it out to
     every connected dashboard WebSocket client.

Meant to be launched with ``spark-submit`` via the ``run_streaming.sh`` wrapper
which wires in the Kafka + Delta packages.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import requests
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared.schemas import CLICK_SCHEMA, ORDER_SCHEMA  # noqa: E402
from shared.transformations import parse_kafka_json  # noqa: E402

KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_ORDERS: str = os.getenv("KAFKA_TOPIC_ORDERS", "raw.orders")
KAFKA_TOPIC_CLICKS: str = os.getenv("KAFKA_TOPIC_CLICKS", "raw.clicks")

BRONZE_PATH: str = os.getenv("BRONZE_PATH", "/data/bronze")
CHECKPOINT_PATH: str = os.getenv("CHECKPOINT_PATH", "/data/checkpoints")
TRIGGER_SECONDS: int = int(os.getenv("STREAM_TRIGGER_SECONDS", "5"))

API_INTERNAL_URL: str = os.getenv("API_INTERNAL_URL", "http://fastapi:8000")
WEBHOOK_URL: str = f"{API_INTERNAL_URL}/internal/batch-event"


def configure_logging() -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <7}</level> | "
        "<cyan>streaming</cyan> - <level>{message}</level>",
    )


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("streaming-demo-bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def push_summary(topic: str, batch_id: int, rows: list[dict[str, Any]]) -> None:
    payload = {
        "type": "batch",
        "batch_id": batch_id,
        "topic": topic,
        "records": len(rows),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sample": rows[:3],
    }
    try:
        resp = requests.post(WEBHOOK_URL, json=payload, timeout=3)
        if resp.status_code >= 400:
            logger.warning(f"webhook non-2xx status={resp.status_code} body={resp.text[:200]}")
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"webhook failed topic={topic} batch={batch_id} err={exc}")


def make_foreach_batch(topic: str, bronze_path: str):
    def _handler(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        logger.info(f"micro-batch topic={topic} batch_id={batch_id} records={count}")

        if count == 0:
            push_summary(topic, batch_id, [])
            return

        (
            batch_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(bronze_path)
        )

        sample_rows = (
            batch_df.limit(3)
            .withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast("string"))
            .withColumn("event_timestamp", F.col("event_timestamp").cast("string"))
            .toJSON()
            .collect()
        )
        rows = [json.loads(r) for r in sample_rows]
        push_summary(topic, batch_id, rows)

    return _handler


def start_stream(
    spark: SparkSession,
    topic: str,
    schema,
    bronze_subpath: str,
    checkpoint_subpath: str,
):
    raw = read_kafka_stream(spark, topic)
    parsed = parse_kafka_json(raw, schema, topic)

    bronze_path = os.path.join(BRONZE_PATH, bronze_subpath)
    checkpoint_path = os.path.join(CHECKPOINT_PATH, checkpoint_subpath)
    os.makedirs(bronze_path, exist_ok=True)
    os.makedirs(checkpoint_path, exist_ok=True)

    query = (
        parsed.writeStream.foreachBatch(make_foreach_batch(topic, bronze_path))
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .queryName(f"bronze-{bronze_subpath}")
        .start()
    )
    logger.info(
        f"started stream topic={topic} bronze={bronze_path} "
        f"checkpoint={checkpoint_path} trigger={TRIGGER_SECONDS}s"
    )
    return query


def main() -> None:
    configure_logging()
    logger.info(
        f"streaming job starting kafka={KAFKA_BOOTSTRAP} "
        f"bronze={BRONZE_PATH} webhook={WEBHOOK_URL}"
    )

    spark = build_spark()

    orders_query = start_stream(spark, KAFKA_TOPIC_ORDERS, ORDER_SCHEMA, "orders", "orders")
    clicks_query = start_stream(spark, KAFKA_TOPIC_CLICKS, CLICK_SCHEMA, "clicks", "clicks")

    logger.info("streams running. awaiting termination.")
    spark.streams.awaitAnyTermination()

    orders_query.stop()
    clicks_query.stop()


if __name__ == "__main__":
    main()
