"""Batch job: Bronze → Silver (clean + deduplicate + upsert via MERGE)."""

from __future__ import annotations

import os
import sys

from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame, SparkSession

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared.transformations import clean_clicks, clean_orders  # noqa: E402

BRONZE_PATH: str = os.getenv("BRONZE_PATH", "/data/bronze")
SILVER_PATH: str = os.getenv("SILVER_PATH", "/data/silver")


def configure_logging() -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <7}</level> | "
        "<cyan>batch_silver</cyan> - <level>{message}</level>",
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("streaming-demo-silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def upsert(spark: SparkSession, silver_path: str, updates: DataFrame, merge_key: str) -> None:
    os.makedirs(silver_path, exist_ok=True)
    if DeltaTable.isDeltaTable(spark, silver_path):
        target = DeltaTable.forPath(spark, silver_path)
        (
            target.alias("t")
            .merge(updates.alias("s"), f"t.{merge_key} = s.{merge_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"merged into existing silver table path={silver_path}")
    else:
        updates.write.format("delta").mode("overwrite").save(silver_path)
        logger.info(f"created new silver table path={silver_path}")


def process_orders(spark: SparkSession) -> None:
    bronze_path = os.path.join(BRONZE_PATH, "orders")
    silver_path = os.path.join(SILVER_PATH, "orders")

    if not DeltaTable.isDeltaTable(spark, bronze_path):
        logger.warning(f"orders bronze not yet populated at {bronze_path}; skipping")
        return

    bronze = spark.read.format("delta").load(bronze_path)
    in_count = bronze.count()
    cleaned = clean_orders(bronze)
    out_count = cleaned.count()
    logger.info(f"orders bronze={in_count} cleaned={out_count}")

    upsert(spark, silver_path, cleaned, merge_key="order_id")


def process_clicks(spark: SparkSession) -> None:
    bronze_path = os.path.join(BRONZE_PATH, "clicks")
    silver_path = os.path.join(SILVER_PATH, "clicks")

    if not DeltaTable.isDeltaTable(spark, bronze_path):
        logger.warning(f"clicks bronze not yet populated at {bronze_path}; skipping")
        return

    bronze = spark.read.format("delta").load(bronze_path)
    in_count = bronze.count()
    cleaned = clean_clicks(bronze)
    out_count = cleaned.count()
    logger.info(f"clicks bronze={in_count} cleaned={out_count}")

    upsert(spark, silver_path, cleaned, merge_key="session_id")


def main() -> None:
    configure_logging()
    logger.info(f"batch_silver starting bronze={BRONZE_PATH} silver={SILVER_PATH}")
    spark = build_spark()
    try:
        process_orders(spark)
        process_clicks(spark)
    finally:
        spark.stop()
    logger.info("batch_silver done")


if __name__ == "__main__":
    main()
