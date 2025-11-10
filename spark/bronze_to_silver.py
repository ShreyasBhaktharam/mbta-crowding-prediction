import argparse
import json
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType


def spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("citystream-bronze-silver")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def bronze_stream_kafka_to_parquet(spark: SparkSession, topic: str, out_root: str) -> None:
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BROKER", "localhost:9092"))
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )
    parsed = df.select(
        F.col("timestamp").alias("kafka_ts"),
        F.col("value").cast("string").alias("json_str")
    )

    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    out_dir = os.path.join(out_root, "bronze", topic, f"date={date_str}")
    (
        parsed.writeStream
        .format("parquet")
        .option("path", out_dir)
        .option("checkpointLocation", os.path.join(out_root, "chk", topic))
        .outputMode("append")
        .start()
        .awaitTermination()
    )


def silver_features_batch(spark: SparkSession, bronze_root: str, silver_root: str, h3_res: int = 8) -> None:
    try:
        import h3
    except Exception as e:
        raise RuntimeError("h3 package required for silver features") from e

    # Read latest bronze for minimal demo (wildcard for date if desired)
    vp_path = os.path.join(bronze_root, "bronze", "gtfs.vehicle_positions", "*")
    tu_path = os.path.join(bronze_root, "bronze", "gtfs.trip_updates", "*")

    vp = spark.read.parquet(vp_path)
    tu = spark.read.parquet(tu_path)

    # Define expected schemas so selects work even if inputs are empty
    vp_schema = StructType([
        StructField("vehicle_id", StringType(), True),
        StructField("trip_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("speed_mps", DoubleType(), True),
        StructField("timestamp", LongType(), True),
    ])
    tu_schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("stop_id", StringType(), True),
        StructField("arrival_delay_s", IntegerType(), True),
        StructField("departure_delay_s", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ])

    # Extract JSON fields using schemas
    vp_json = spark.read.schema(vp_schema).json(vp.select("json_str").rdd.map(lambda r: r[0]))
    tu_json = spark.read.schema(tu_schema).json(tu.select("json_str").rdd.map(lambda r: r[0]))

    # Basic H3 index (UDF)
    @F.udf(returnType=StringType())
    def h3_index(lat: F.Column, lon: F.Column) -> str:  # type: ignore[override]
        try:
            return h3.geo_to_h3(lat, lon, h3_res)
        except Exception:
            return None

    vp_enriched = (
        vp_json
        .withColumn("ts", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))
        .withColumn("h3_res", F.lit(h3_res))
        .withColumn("h3", h3_index(F.col("lat"), F.col("lon")))
    )

    tu_sel = tu_json.select("trip_id", "stop_id", "arrival_delay_s", "departure_delay_s", F.col("timestamp").alias("tu_ts"))

    # Simple join on trip_id with 5-minute window proxy (approx with nearest ts after aggregating)
    vp_keyed = vp_enriched.groupBy("trip_id").agg(
        F.max("ts").alias("ts_latest"),
        F.last("h3", ignorenulls=True).alias("h3")
    )

    joined = (
        vp_keyed.join(tu_sel, on="trip_id", how="left")
        .withColumn("minute", F.date_trunc("minute", F.col("ts_latest")))
    )

    # Very simple features by h3 minute
    feats = (
        joined.groupBy("h3", "minute")
        .agg(
            F.countDistinct("trip_id").alias("active_trips"),
            F.avg("arrival_delay_s").alias("avg_arrival_delay_s"),
            F.avg("departure_delay_s").alias("avg_departure_delay_s"),
        )
        .withColumnRenamed("minute", "ts_floor_1m")
    )

    out_dir = os.path.join(silver_root, "silver", f"date={datetime.utcnow().strftime('%Y-%m-%d')}" )
    feats.write.mode("overwrite").parquet(out_dir)
    print(f"Wrote silver features to {out_dir}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["bronze", "silver"], required=True)
    parser.add_argument("--topic", default="gtfs.vehicle_positions")
    parser.add_argument("--out_root", default=os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data")))
    parser.add_argument("--h3_res", type=int, default=8)
    args = parser.parse_args()

    sp = spark()
    if args.mode == "bronze":
        bronze_stream_kafka_to_parquet(sp, args.topic, args.out_root)
    else:
        silver_features_batch(sp, args.out_root, args.out_root, args.h3_res)


if __name__ == "__main__":
    main()

