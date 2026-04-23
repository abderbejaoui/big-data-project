"""Reusable PySpark transformations for bronze/silver/gold layers."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def parse_kafka_json(raw_df: DataFrame, schema: StructType, topic: str) -> DataFrame:
    """Parse Kafka `value` column as JSON under ``schema`` and enrich with metadata."""
    return (
        raw_df.selectExpr("CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS value")
        .withColumn("payload", F.from_json(F.col("value"), schema))
        .select("payload.*", "kafka_key")
        .withColumn("source_topic", F.lit(topic))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("timestamp")),
        )
    )


def clean_orders(bronze_df: DataFrame) -> DataFrame:
    """Produce a deduplicated + typed orders dataframe for the Silver layer."""
    deduped = (
        bronze_df.dropna(subset=["order_id", "user_id", "amount"])
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn(
            "status",
            F.when(F.col("status").isNull(), F.lit("unknown")).otherwise(F.col("status")),
        )
        .dropDuplicates(["order_id"])
        .withColumn("processed_at", F.current_timestamp())
    )
    return deduped


def clean_clicks(bronze_df: DataFrame) -> DataFrame:
    """Produce a deduplicated + typed clicks dataframe for the Silver layer."""
    return (
        bronze_df.dropna(subset=["session_id", "user_id", "page"])
        .dropDuplicates(["session_id", "event_timestamp", "action"])
        .withColumn("processed_at", F.current_timestamp())
    )


def revenue_per_product(orders_silver: DataFrame) -> DataFrame:
    return (
        orders_silver.groupBy("product")
        .agg(
            F.round(F.sum("amount"), 2).alias("total_revenue"),
            F.count("*").alias("order_count"),
            F.round(F.avg("amount"), 2).alias("avg_order_value"),
        )
        .orderBy(F.col("total_revenue").desc())
    )


def orders_by_status(orders_silver: DataFrame) -> DataFrame:
    return (
        orders_silver.groupBy("status")
        .agg(F.count("*").alias("order_count"))
        .orderBy(F.col("order_count").desc())
    )


def revenue_per_minute(orders_silver: DataFrame) -> DataFrame:
    return (
        orders_silver.groupBy(F.date_trunc("minute", "event_timestamp").alias("minute"))
        .agg(F.round(F.sum("amount"), 2).alias("revenue"))
        .orderBy("minute")
    )


def top_pages(clicks_silver: DataFrame, limit: int = 10) -> DataFrame:
    return (
        clicks_silver.groupBy("page")
        .agg(F.count("*").alias("views"))
        .orderBy(F.col("views").desc())
        .limit(limit)
    )


def top_actions(clicks_silver: DataFrame) -> DataFrame:
    return (
        clicks_silver.groupBy("action")
        .agg(F.count("*").alias("events"))
        .orderBy(F.col("events").desc())
    )


def events_per_device(clicks_silver: DataFrame) -> DataFrame:
    return (
        clicks_silver.groupBy("device")
        .agg(F.count("*").alias("events"))
        .orderBy(F.col("events").desc())
    )
