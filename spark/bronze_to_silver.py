import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from spark.jobs import BronzeIngestJob, GoldAggregationJob, SilverTransformJob, validate_tables
from spark.utils import DEFAULT_DATA_ROOT, build_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description="CityStream Spark Orchestrator")
    parser.add_argument("--mode", choices=["bronze", "silver", "gold", "validate"], required=True)
    parser.add_argument("--topic", default="gtfs.vehicle_positions")
    parser.add_argument("--data-root", default=str(DEFAULT_DATA_ROOT))
    parser.add_argument("--gtfs-root", default=None)
    parser.add_argument("--h3-res", type=int, default=8)
    parser.add_argument("--horizons", nargs="*", type=int, default=[10, 20, 30])
    parser.add_argument(
        "--date", default=None, help="Process only this YYYY-MM-DD partition from bronze"
    )
    args = parser.parse_args()

    spark = build_spark_session(app_name=f"citystream-{args.mode}")

    if args.mode == "bronze":
        job = BronzeIngestJob(
            spark=spark,
            topic=args.topic,
            data_root=args.data_root,
            checkpoint_root=os.path.join(args.data_root, "checkpoints", args.topic),
        )
        job.start()
    elif args.mode == "silver":
        job = SilverTransformJob(
            spark=spark,
            data_root=args.data_root,
            gtfs_root=args.gtfs_root,
            h3_resolution=args.h3_res,
            process_date=args.date,
        )
        job.run()
    elif args.mode == "gold":
        job = GoldAggregationJob(
            spark=spark,
            data_root=args.data_root,
            horizons=args.horizons,
        )
        job.run()
    else:
        validate_tables(
            spark,
            {
                "silver": os.path.join(args.data_root, "silver"),
                "gold": os.path.join(args.data_root, "gold"),
            },
        )


if __name__ == "__main__":
    main()
