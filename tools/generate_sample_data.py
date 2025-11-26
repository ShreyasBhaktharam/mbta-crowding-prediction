from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate small synthetic Bronze/Silver/Gold samples"
    )
    parser.add_argument("--out", default="data")
    args = parser.parse_args()

    root = Path(args.out)
    (root / "bronze" / "gtfs.vehicle_positions").mkdir(parents=True, exist_ok=True)
    (root / "bronze" / "gtfs.trip_updates").mkdir(parents=True, exist_ok=True)
    (root / "bronze" / "weather.hourly").mkdir(parents=True, exist_ok=True)
    (root / "gtfs").mkdir(parents=True, exist_ok=True)

    base_ts = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    vp = pd.DataFrame(
        {
            "trip_id": ["t1", "t2"],
            "lat": [42.35, 42.36],
            "lon": [-71.05, -71.06],
            "speed_mps": [10, 12],
            "event_ts": [base_ts, base_ts + timedelta(minutes=1)],
            "timestamp": [base_ts, base_ts + timedelta(minutes=1)],
        }
    )
    vp.to_parquet(root / "bronze" / "gtfs.vehicle_positions" / "sample.parquet", index=False)

    tu = pd.DataFrame(
        {
            "trip_id": ["t1", "t2"],
            "stop_id": ["s1", "s2"],
            "arrival_delay_s": [30, 20],
            "departure_delay_s": [10, 5],
            "event_ts": [base_ts, base_ts + timedelta(minutes=1)],
        }
    )
    tu.to_parquet(root / "bronze" / "gtfs.trip_updates" / "sample.parquet", index=False)

    wx = pd.DataFrame(
        {
            "event_ts": [base_ts, base_ts + timedelta(hours=1)],
            "temp_c": [10.0, 11.0],
            "wind_mps": [1.5, 1.7],
            "precip_mm": [0.1, 0.0],
        }
    )
    wx.to_parquet(root / "bronze" / "weather.hourly" / "sample.parquet", index=False)

    stops = pd.DataFrame(
        {"stop_id": ["s1", "s2"], "stop_name": ["Alpha", "Beta"], "parent_station": [None, None]}
    )
    stops.to_parquet(root / "gtfs" / "stops.parquet", index=False)
    stop_times = pd.DataFrame(
        {"trip_id": ["t1", "t2"], "stop_id": ["s1", "s2"], "stop_sequence": [1, 1]}
    )
    stop_times.to_parquet(root / "gtfs" / "stop_times.parquet", index=False)

    print(f"Synthetic data written to {root}")


if __name__ == "__main__":
    main()
