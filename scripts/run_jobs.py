#!/usr/bin/env python3
"""
Run Silver every 10 min and Gold every 25 min, but never overlap.
Uses a simple lock to ensure only one job runs at a time.
"""

import subprocess
import threading
import time
from datetime import datetime, timezone

SILVER_INTERVAL = 10 * 60  # 10 minutes
GOLD_INTERVAL = 25 * 60    # 25 minutes
PROJECT_DIR = "/Users/shreyas/Documents/citystream"

lock = threading.Lock()


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def run_cmd(cmd: list[str], name: str) -> None:
    with lock:
        log(f"START {name}: {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=PROJECT_DIR, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
        if result.returncode != 0:
            log(f"ERROR {name}: {result.stderr}")
        else:
            log(f"DONE {name}")


def silver_loop() -> None:
    while True:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        run_cmd(["make", "silver", f"DATE={date_str}"], "silver")
        time.sleep(SILVER_INTERVAL)


def gold_loop() -> None:
    # Offset Gold start by 6 minutes to reduce collision likelihood
    time.sleep(6 * 60)
    while True:
        run_cmd(["make", "gold"], "gold")
        time.sleep(GOLD_INTERVAL)


def main() -> None:
    log("Starting job scheduler (Silver every 10m, Gold every 25m, no overlap). Ctrl+C to stop.")
    t_silver = threading.Thread(target=silver_loop, daemon=True)
    t_gold = threading.Thread(target=gold_loop, daemon=True)
    t_silver.start()
    t_gold.start()
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log("Shutting down.")


if __name__ == "__main__":
    main()

