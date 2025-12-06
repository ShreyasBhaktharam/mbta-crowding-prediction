#!/usr/bin/env python3
"""Run `make silver DATE=<today>` every 10 minutes."""

import subprocess
import time
from datetime import datetime, timezone

INTERVAL_SECONDS = 10 * 60  # 10 minutes


def run_silver() -> None:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cmd = ["make", "silver", f"DATE={date_str}"]
    print(f"[{datetime.now(timezone.utc).isoformat()}] Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd="/Users/shreyas/Documents/citystream", capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(f"[ERROR] {result.stderr}")


def main() -> None:
    print(f"Starting silver job loop (every {INTERVAL_SECONDS // 60} min). Ctrl+C to stop.")
    while True:
        try:
            run_silver()
        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

