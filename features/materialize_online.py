import argparse
import os

from features.store import get_feature_store
from spark.utils import DEFAULT_DATA_ROOT, build_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Materialize Gold aggregates to the online feature store"
    )
    parser.add_argument("--data-root", default=str(DEFAULT_DATA_ROOT))
    args = parser.parse_args()

    spark = build_spark_session(app_name="citystream-online-materializer")
    store = get_feature_store()
    gold_path = os.path.join(args.data_root, "gold")
    checkpoint_dir = os.path.join(args.data_root, "checkpoints", "online_features")

    def publish(batch_df, batch_id):  # noqa: ANN001
        rows = batch_df.select(
            "origin_stop",
            "dest_stop",
            "h3",
            "horizon_min",
            "rolling_mean_7d",
            "hist_p50",
            "hist_p90",
        ).collect()
        for row in rows:
            payload = {
                "rolling_mean_7d": float(row.rolling_mean_7d or 0.0),
                "hist_p50": float(row.hist_p50 or 0.0),
                "hist_p90": float(row.hist_p90 or 0.0),
                "h3": row.h3 or "unknown",
            }
            store.set_features(row.origin_stop, row.dest_stop, int(row.horizon_min), payload)

    query = (
        spark.readStream.format("delta")
        .option("skipChangeCommits", "true")
        .load(gold_path)
        .writeStream.foreachBatch(publish)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
