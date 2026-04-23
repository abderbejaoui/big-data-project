"""Pydantic + Spark schemas shared across streaming and batch jobs."""

from __future__ import annotations

from pydantic import BaseModel
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


class OrderEvent(BaseModel):
    order_id: str
    user_id: str
    product: str
    amount: float
    currency: str
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


ORDER_SCHEMA: StructType = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("product", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)


CLICK_SCHEMA: StructType = StructType(
    [
        StructField("session_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("page", StringType(), True),
        StructField("action", StringType(), True),
        StructField("device", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)
