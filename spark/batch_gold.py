"""Batch job: Silver → Gold (business-facing aggregates, overwritten each run)."""

from __future__ import annotations

import os
import sys

from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame, SparkSession

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared.transformations import (  # noqa: E402
    events_per_device,
    orders_by_status,
    revenue_per_minute,
    revenue_per_product,
    top_actions,
    top_pages,
)

SILVER_PATH: str = os.getenv("SILVER_PATH", "/data/silver")
GOLD_PATH: str = os.getenv("GOLD_PATH", "/data/gold")


def configure_logging() -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <7}</level> | "
        "<cyan>batch_gold</cyan> - <level>{message}</level>",
    )


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("streaming-demo-gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def overwrite(df: DataFrame, path: str) -> None:
    os.makedirs(path, exist_ok=True)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    logger.info(f"wrote gold path={path} rows={df.count()}")


def process_orders(spark: SparkSession) -> None:
    silver_path = os.path.join(SILVER_PATH, "orders")
    if not DeltaTable.isDeltaTable(spark, silver_path):
        logger.warning(f"orders silver missing at {silver_path}; skipping")
        return

    silver = spark.read.format("delta").load(silver_path)
    logger.info(f"orders silver rows={silver.count()}")

    overwrite(revenue_per_product(silver), os.path.join(GOLD_PATH, "orders_revenue_per_product"))
    overwrite(orders_by_status(silver), os.path.join(GOLD_PATH, "orders_by_status"))
    overwrite(revenue_per_minute(silver), os.path.join(GOLD_PATH, "orders_revenue_per_minute"))


def process_clicks(spark: SparkSession) -> None:
    silver_path = os.path.join(SILVER_PATH, "clicks")
    if not DeltaTable.isDeltaTable(spark, silver_path):
        logger.warning(f"clicks silver missing at {silver_path}; skipping")
        return

    silver = spark.read.format("delta").load(silver_path)
    logger.info(f"clicks silver rows={silver.count()}")

    overwrite(top_pages(silver), os.path.join(GOLD_PATH, "clicks_top_pages"))
    overwrite(top_actions(silver), os.path.join(GOLD_PATH, "clicks_top_actions"))
    overwrite(events_per_device(silver), os.path.join(GOLD_PATH, "clicks_events_per_device"))


def main() -> None:
    configure_logging()
    logger.info(f"batch_gold starting silver={SILVER_PATH} gold={GOLD_PATH}")
    spark = build_spark()
    try:
        process_orders(spark)
        process_clicks(spark)
    finally:
        spark.stop()
    logger.info("batch_gold done")


if __name__ == "__main__":
    main()
