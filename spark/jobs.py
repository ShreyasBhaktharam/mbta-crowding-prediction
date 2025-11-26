from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

import h3
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from .schemas import load_schema
from .utils import DEFAULT_DATA_ROOT


@dataclass
class BronzeIngestJob:
    spark: SparkSession
    topic: str
    data_root: str = field(default_factory=lambda: str(DEFAULT_DATA_ROOT))
    kafka_bootstrap: str = field(
        default_factory=lambda: os.getenv("KAFKA_BROKER", "localhost:9092")
    )
    checkpoint_root: Optional[str] = None
    dlq_topic: Optional[str] = None

    def start(self) -> None:
        schema = load_schema(self.topic)
        query = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap)
            .option("subscribe", self.topic)
            .option("startingOffsets", os.getenv("KAFKA_OFFSETS", "latest"))
            .option("failOnDataLoss", "false")
            .load()
        )
        parsed = query.select(
            F.col("timestamp").alias("kafka_ts"),
            F.from_json(F.col("value").cast("string"), schema).alias("payload"),
            F.col("value").cast("string").alias("raw_value"),
        )

        good = (
            parsed.filter(F.col("payload").isNotNull())
            .select("payload.*", "kafka_ts")
            .withColumn(
                "event_ts",
                F.when(
                    F.col("timestamp").isNotNull(), F.to_timestamp(F.col("timestamp"))
                ).otherwise(F.col("kafka_ts")),
            )
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("date", F.to_date(F.col("event_ts")))
            .withColumn("hour", F.date_format(F.col("event_ts"), "HH"))
        )

        bad = parsed.filter(F.col("payload").isNull()).select(
            F.col("raw_value"),
            F.col("kafka_ts"),
            F.lit(self.topic).alias("topic"),
            F.current_timestamp().alias("ingest_ts"),
        )

        bronze_path = os.path.join(self.data_root, "bronze", self.topic)
        checkpoint = self.checkpoint_root or os.path.join(self.data_root, "checkpoints", self.topic)

        stream = (
            good.writeStream.format("delta")
            .partitionBy("date", "hour")
            .option("checkpointLocation", checkpoint)
            .outputMode("append")
            .start(bronze_path)
        )

        dlq_query = None
        if int(os.getenv("DLQ_ENABLE", "1")):
            dlq_path = os.path.join(self.data_root, "dlq", self.topic)
            dlq_query = (
                bad.writeStream.format("delta")
                .option("checkpointLocation", os.path.join(checkpoint, "dlq"))
                .outputMode("append")
                .start(dlq_path)
            )
        stream.awaitTermination()
        if dlq_query:
            dlq_query.awaitTermination()


@dataclass
class SilverTransformJob:
    spark: SparkSession
    data_root: str = field(default_factory=lambda: str(DEFAULT_DATA_ROOT))
    h3_resolution: int = 8
    gtfs_root: Optional[str] = None
    process_date: Optional[str] = None

    def _bronze_table(self, topic: str) -> DataFrame:
        path = os.path.join(self.data_root, "bronze", topic)
        return self.spark.read.format("delta").load(path)

    def _load_gtfs(self, table: str) -> DataFrame:
        root = self.gtfs_root or os.path.join(self.data_root, "gtfs")
        path = os.path.join(root, f"{table}.parquet")
        return self.spark.read.parquet(path)

    def _h3_udf(self):
        # Avoid capturing `self` in the UDF closure (which would drag SparkContext into workers)
        resolution = int(self.h3_resolution)

        def to_h3(lat, lon):
            if lat is None or lon is None:
                return None
            return h3.geo_to_h3(lat, lon, resolution)

        return F.udf(to_h3, T.StringType())

    def run(self) -> str:
        vp = self._bronze_table("gtfs.vehicle_positions")
        tu = self._bronze_table("gtfs.trip_updates")
        weather = self._bronze_table("weather.hourly")

        if self.process_date:
            vp = vp.filter(F.col("date") == F.lit(self.process_date))
            tu = tu.filter(F.col("date") == F.lit(self.process_date))
            weather = weather.filter(F.col("date") == F.lit(self.process_date))

        stop_times = self._load_gtfs("stop_times")
        stops = self._load_gtfs("stops")

        vp_curated = (
            vp.withColumn("minute", F.date_trunc("minute", F.col("event_ts")))
            .withColumn("h3", self._h3_udf()(F.col("lat"), F.col("lon")))
            .withColumn("speed_mps", F.col("speed_mps").cast("double"))
        )

        tu_curated = (
            tu.withColumn("tu_minute", F.date_trunc("minute", F.col("event_ts")))
            .withColumn("arrival_delay_s", F.col("arrival_delay_s").cast("double"))
            .withColumn("departure_delay_s", F.col("departure_delay_s").cast("double"))
        )

        stop_pairs = stop_times.withColumn(
            "dest_stop",
            F.lead("stop_id").over(Window.partitionBy("trip_id").orderBy("stop_sequence")),
        ).dropna(subset=["dest_stop"])

        trips_with_pairs = tu_curated.join(stop_pairs, ["trip_id", "stop_id"], how="left")

        enriched = (
            vp_curated.join(trips_with_pairs, on="trip_id", how="left")
            .withColumn("origin_stop", F.col("stop_id"))
            .withColumn("weather_hour", F.date_trunc("hour", F.col("minute")))
        )

        weather_hourly = weather.withColumn(
            "weather_hour", F.date_trunc("hour", F.col("event_ts"))
        ).select(
            "weather_hour",
            "temp_c",
            "wind_mps",
            "precip_mm",
        )
        enriched = enriched.join(weather_hourly, on="weather_hour", how="left")

        stops_sel = stops.select(
            F.col("stop_id").alias("origin_stop_key"),
            F.col("stop_name").alias("origin_name"),
            F.col("parent_station"),
        )
        enriched = (
            enriched.join(stops_sel, enriched.origin_stop == F.col("origin_stop_key"), "left")
            .drop("origin_stop_key")
            .fillna({"dest_stop": "unknown", "origin_stop": "unknown"})
        )

        aggregates = (
            enriched.groupBy("origin_stop", "dest_stop", "h3", "minute")
            .agg(
                F.countDistinct("trip_id").alias("active_trips"),
                F.avg("arrival_delay_s").alias("avg_arrival_delay_s"),
                F.avg("departure_delay_s").alias("avg_departure_delay_s"),
                F.avg("temp_c").alias("avg_temp_c"),
                F.avg("wind_mps").alias("avg_wind_speed"),
                F.avg("precip_mm").alias("avg_precip_mm"),
            )
            .withColumn(
                "crowding_score",
                F.coalesce(F.col("avg_arrival_delay_s"), F.lit(0.0)) + F.col("active_trips") * 0.1,
            )
            .withColumn("date", F.to_date("minute"))
        )

        dataset = SparkDFDataset(aggregates)
        suite = ExpectationSuite(expectation_suite_name="silver_aggregates_suite")
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "h3"},
            )
        )
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={"column": "active_trips", "min_value": 0},
            )
        )
        result = dataset.validate(expectation_suite=suite)
        if not result.get("success"):
            raise ValueError(f"Silver data quality checks failed: {result}")

        silver_path = os.path.join(self.data_root, "silver")
        (aggregates.write.format("delta").mode("overwrite").partitionBy("date").save(silver_path))
        return silver_path


@dataclass
class GoldAggregationJob:
    spark: SparkSession
    data_root: str = field(default_factory=lambda: str(DEFAULT_DATA_ROOT))
    horizons: Iterable[int] = (10, 20, 30)

    def run(self) -> str:
        silver_path = os.path.join(self.data_root, "silver")
        silver = self.spark.read.format("delta").load(silver_path)
        order_col = F.col("minute").cast("long")
        window_lead = Window.partitionBy("origin_stop", "dest_stop", "h3").orderBy(order_col)
        window_stats = window_lead.rangeBetween(-7 * 24 * 60 * 60, 0)

        hist_quantiles = silver.groupBy("origin_stop", "dest_stop", "h3").agg(
            F.percentile_approx("crowding_score", 0.5).alias("hist_p50"),
            F.percentile_approx("crowding_score", 0.9).alias("hist_p90"),
        )

        union_frames: List[DataFrame] = []
        for horizon in self.horizons:
            df = (
                silver.withColumn("horizon_min", F.lit(int(horizon)))
                .withColumn("label_p50", F.lead("crowding_score", horizon).over(window_lead))
                .withColumn("rolling_mean_7d", F.avg("crowding_score").over(window_stats))
                .withColumn("rolling_std_7d", F.stddev_pop("crowding_score").over(window_stats))
                .withColumn("date", F.to_date("minute"))
                .join(hist_quantiles, on=["origin_stop", "dest_stop", "h3"], how="left")
            )
            union_frames.append(df)

        gold = union_frames[0]
        for extra in union_frames[1:]:
            gold = gold.unionByName(extra)

        gold_path = os.path.join(self.data_root, "gold")
        (
            gold.write.format("delta")
            .mode("overwrite")
            .partitionBy("date", "horizon_min")
            .save(gold_path)
        )
        return gold_path


def validate_tables(spark: SparkSession, tables: Dict[str, str]) -> Dict[str, Dict]:
    results = {}
    for name, path in tables.items():
        df = spark.read.format("delta").load(path)
        dataset = SparkDFDataset(df)
        validation = dataset.validate(
            expectation_suite={
                "expectations": [
                    {
                        "expectation_type": "expect_table_row_count_to_be_greater_than",
                        "kwargs": {"value": 0},
                    },
                ]
            }
        )
        results[name] = validation
        if not validation.get("success"):
            raise ValueError(f"Validation failed for {name}: {validation}")
    return results
