import argparse
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from features.store import get_feature_store
from spark.utils import DEFAULT_DATA_ROOT, build_spark_session


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("materialize_online")
    parser = argparse.ArgumentParser(
        description="Materialize Gold aggregates to the online feature store"
    )
    parser.add_argument("--data-root", default=str(DEFAULT_DATA_ROOT))
    args = parser.parse_args()

    spark = build_spark_session(app_name="citystream-online-materializer")
    store = get_feature_store()
    gold_path = os.path.join(args.data_root, "gold")

    # Check if Gold table exists and has data
    logger.info(f"Checking Gold table at {gold_path}")
    try:
        test_df = spark.read.format("delta").load(gold_path)
        row_count = test_df.count()
        logger.info(f"Gold table exists with {row_count} rows")
        logger.info(f"Columns: {test_df.columns}")
        if row_count == 0:
            logger.error("Gold table is empty! Cannot materialize.")
            return
    except Exception as e:
        logger.error(f"Failed to read Gold table: {e}")
        return

    def publish(batch_df, batch_id):  # noqa: ANN001
        # Log batch arrival
        logger.info(f"batch_id={batch_id} triggered, checking columns...")
        logger.info(f"Available columns: {batch_df.columns}")

        # Check if minute column exists
        if "minute" not in batch_df.columns:
            logger.error("minute column not found in batch_df!")
            return

        rows = batch_df.select(
            "origin_stop",
            "dest_stop",
            "h3",
            "horizon_min",
            "minute",
            "rolling_mean_7d",
            "hist_p50",
            "hist_p90",
        ).collect()
        total = len(rows)
        logger.info("batch_id=%s received rows=%s", batch_id, total)
        if total == 0:
            return
        success = 0
        errors = 0
        for row in rows:
            try:
                payload = {
                    "rolling_mean_7d": float(row.rolling_mean_7d or 0.0),
                    "hist_p50": float(row.hist_p50 or 0.0),
                    "hist_p90": float(row.hist_p90 or 0.0),
                    "h3": row.h3 or "unknown",
                }
                # Write both timestamped key (for TFT sequences) and current key (for LightGBM)
                timestamp = int(row.minute.timestamp()) if row.minute else 0
                store.set_features_timestamped(row.origin_stop, row.dest_stop, int(row.horizon_min), timestamp, payload)
                store.set_features(row.origin_stop, row.dest_stop, int(row.horizon_min), payload)
                success += 1
            except Exception as exc:  # pragma: no cover - defensive logging
                errors += 1
                logger.warning(
                    "batch_id=%s failed write for key=(%s,%s,%s): %s",
                    batch_id,
                    row.origin_stop,
                    row.dest_stop,
                    int(row.horizon_min),
                    exc,
                )
        logger.info("batch_id=%s wrote=%s errors=%s", batch_id, success, errors)

    checkpoint_dir = os.path.join(args.data_root, "checkpoints", "online_features")
    query = (
        spark.readStream.format("delta")
        .option("skipChangeCommits", "true")  # Handle overwrites from Gold job
        .option("startingVersion", "0")  # Start from beginning of Delta log
        .option("maxFilesPerTrigger", "1")  # Process incrementally
        .load(gold_path)
        .writeStream.foreachBatch(publish)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)  # Process all available data immediately
        .start()
    )
    logger.info(
        "materializer started gold_path=%s checkpoint=%s backend=%s",
        gold_path,
        checkpoint_dir,
        type(store.backend).__name__,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
