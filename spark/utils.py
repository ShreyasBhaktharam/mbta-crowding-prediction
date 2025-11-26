from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Optional

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_ROOT = PROJECT_ROOT / "data"
SCHEMA_DIR = PROJECT_ROOT / "schemas"


def build_spark_session(
    app_name: str = "citystream", extra_conf: Optional[Dict[str, str]] = None
) -> SparkSession:
    """Create a Spark session configured for Delta Lake and Kafka."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"))
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            os.getenv(
                "SPARK_PACKAGES",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0",
            ),
        )
    )
    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)
    return configure_spark_with_delta_pip(builder).getOrCreate()


def resolve_path(*parts: str) -> Path:
    base = DEFAULT_DATA_ROOT
    return base.joinpath(*parts)


def load_json(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)
