"""Long-lived batch scheduler.

Keeps one Spark session alive and re-runs the Silver + Gold batch steps on
a fixed cadence so the dashboard BatchStats panel updates continuously
without paying JVM startup cost every cycle.
"""

from __future__ import annotations

import os
import sys
import time

from loguru import logger
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from batch_gold import process_clicks as gold_clicks, process_orders as gold_orders  # noqa: E402
from batch_silver import process_clicks as silver_clicks, process_orders as silver_orders  # noqa: E402

BATCH_INTERVAL_SECONDS: int = int(os.getenv("BATCH_INTERVAL_SECONDS", "30"))
INITIAL_DELAY_SECONDS: int = int(os.getenv("BATCH_INITIAL_DELAY_SECONDS", "15"))


def configure_logging() -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <7}</level> | "
        "<cyan>scheduler</cyan> - <level>{message}</level>",
    )


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("streaming-demo-scheduler")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_once(spark: SparkSession, cycle: int) -> None:
    t0 = time.time()
    logger.info(f"cycle={cycle} starting silver+gold")
    try:
        silver_orders(spark)
        silver_clicks(spark)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"silver failed cycle={cycle} err={exc}")
    try:
        gold_orders(spark)
        gold_clicks(spark)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"gold failed cycle={cycle} err={exc}")
    logger.info(f"cycle={cycle} finished in {time.time() - t0:.1f}s")


def main() -> None:
    configure_logging()
    logger.info(
        f"scheduler starting initial_delay={INITIAL_DELAY_SECONDS}s "
        f"interval={BATCH_INTERVAL_SECONDS}s"
    )

    if INITIAL_DELAY_SECONDS > 0:
        time.sleep(INITIAL_DELAY_SECONDS)

    spark = build_spark()

    cycle = 0
    try:
        while True:
            cycle += 1
            run_once(spark, cycle)
            time.sleep(BATCH_INTERVAL_SECONDS)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
