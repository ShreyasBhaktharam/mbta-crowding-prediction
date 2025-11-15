from datetime import datetime

import pandas as pd
from chispa import assert_df_equality

from spark.jobs import GoldAggregationJob, SilverTransformJob


def _write_delta(df, path):
    df.write.format("delta").mode("overwrite").save(path)


def test_silver_and_gold_jobs(tmp_path, spark_session):
    data_root = tmp_path / "data"
    gtfs_root = data_root / "gtfs"
    (data_root / "bronze" / "gtfs.vehicle_positions").mkdir(parents=True, exist_ok=True)
    (data_root / "bronze" / "gtfs.trip_updates").mkdir(parents=True, exist_ok=True)
    (data_root / "bronze" / "weather.hourly").mkdir(parents=True, exist_ok=True)
    gtfs_root.mkdir(parents=True, exist_ok=True)

    vp_rows = [
        ("trip-1", 42.352, -71.055, 12.0, datetime(2024, 1, 1, 12, 0, 0), datetime(2024, 1, 1, 12, 0, 0)),
    ]
    vp_df = spark_session.createDataFrame(
        vp_rows,
        schema="trip_id string, lat double, lon double, speed_mps double, event_ts timestamp, timestamp timestamp",
    )
    _write_delta(vp_df, str(data_root / "bronze" / "gtfs.vehicle_positions"))

    tu_rows = [("trip-1", "stop-a", 30.0, 15.0, datetime(2024, 1, 1, 12, 0, 0))]
    tu_df = spark_session.createDataFrame(
        tu_rows,
        schema="trip_id string, stop_id string, arrival_delay_s double, departure_delay_s double, event_ts timestamp",
    )
    _write_delta(tu_df, str(data_root / "bronze" / "gtfs.trip_updates"))

    wx_rows = [(datetime(2024, 1, 1, 12, 0, 0), 10.0, 2.0, 0.1)]
    wx_df = spark_session.createDataFrame(
        wx_rows,
        schema="event_ts timestamp, temp_c double, wind_mps double, precip_mm double",
    )
    _write_delta(wx_df, str(data_root / "bronze" / "weather.hourly"))

    stop_times = pd.DataFrame(
        {
            "trip_id": ["trip-1"],
            "arrival_time": ["12:00:00"],
            "departure_time": ["12:00:00"],
            "stop_id": ["stop-a"],
            "stop_sequence": [1],
        }
    )
    stops = pd.DataFrame(
        {
            "stop_id": ["stop-a"],
            "stop_name": ["South Station"],
            "parent_station": [None],
        }
    )
    stop_times.to_parquet(gtfs_root / "stop_times.parquet", index=False)
    stops.to_parquet(gtfs_root / "stops.parquet", index=False)

    silver_job = SilverTransformJob(spark=spark_session, data_root=str(data_root), gtfs_root=str(gtfs_root))
    silver_path = silver_job.run()
    silver_df = spark_session.read.format("delta").load(silver_path)
    assert silver_df.count() == 1
    expected = spark_session.createDataFrame([("stop-a", 1)], schema="origin_stop string, active_trips long")
    assert_df_equality(
        silver_df.select("origin_stop", "active_trips"),
        expected,
        ignore_column_order=True,
        ignore_row_order=True,
        ignore_nullable=True,
    )

    gold_job = GoldAggregationJob(spark=spark_session, data_root=str(data_root), horizons=[10])
    gold_path = gold_job.run()
    gold_df = spark_session.read.format("delta").load(gold_path)
    assert "label_p50" in gold_df.columns
    assert gold_df.filter(gold_df.horizon_min == 10).count() >= 1
