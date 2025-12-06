#!/usr/bin/env python3
"""Run `make gold` every 25 minutes."""

import subprocess
import time
from datetime import datetime, timezone

INTERVAL_SECONDS = 25 * 60  # 25 minutes


def run_gold() -> None:
    cmd = ["make", "gold"]
    print(f"[{datetime.now(timezone.utc).isoformat()}] Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd="/Users/shreyas/Documents/citystream", capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"[ERROR] {result.stderr}")


def main() -> None:
    print(f"Starting gold job loop (every {INTERVAL_SECONDS // 60} min). Ctrl+C to stop.")
    while True:
        try:
            run_gold()
        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

