import os
import zipfile
import io
import requests
import pandas as pd


MBTA_GTFS_URL = "https://cdn.mbta.com/MBTA_GTFS.zip"


def download_gtfs(url: str = MBTA_GTFS_URL) -> bytes:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.content


def extract_tables(content: bytes, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        for name in ["stops.txt", "shapes.txt", "trips.txt", "routes.txt", "stop_times.txt"]:
            if name in zf.namelist():
                with zf.open(name) as f:
                    # Read all columns as strings to avoid mixed-type issues when writing Parquet
                    df = pd.read_csv(f, dtype=str, low_memory=False)
                    df.to_parquet(os.path.join(out_dir, name.replace(".txt", ".parquet")), index=False)


def main():
    out = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "gtfs"))
    buf = download_gtfs()
    extract_tables(buf, out)
    print(f"Saved GTFS tables to {out}")


if __name__ == "__main__":
    main()

