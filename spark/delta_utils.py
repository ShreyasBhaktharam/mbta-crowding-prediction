from __future__ import annotations

import argparse
from typing import Iterable, Optional

from pyspark.sql import SparkSession

from .utils import build_spark_session


def optimize_table(
    spark: SparkSession, path: str, zorder_cols: Optional[Iterable[str]] = None
) -> None:
    stmt = f"OPTIMIZE delta.`{path}`"
    if zorder_cols:
        cols = ",".join(zorder_cols)
        stmt = f"{stmt} ZORDER BY ({cols})"
    spark.sql(stmt)


def vacuum_table(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")


def main():
    parser = argparse.ArgumentParser(description="Utility for compacting Delta tables")
    parser.add_argument("action", choices=["optimize", "vacuum"])
    parser.add_argument("path")
    parser.add_argument("--zorder", nargs="*", default=None)
    parser.add_argument("--retention", type=int, default=168)
    args = parser.parse_args()

    spark = build_spark_session(app_name="citystream-delta-utils")
    if args.action == "optimize":
        optimize_table(spark, args.path, args.zorder)
    else:
        vacuum_table(spark, args.path, retention_hours=args.retention)


if __name__ == "__main__":
    main()
