import argparse
import json
import os
from glob import glob

import numpy as np
import pandas as pd
import lightgbm as lgb


def load_silver(silver_root: str) -> pd.DataFrame:
    paths = sorted(glob(os.path.join(silver_root, "silver", "date=*", "*.parquet")))
    if not paths:
        raise FileNotFoundError("No silver parquet found; run silver job first")
    df = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
    # Minimal synthetic target: use arrival_delay as proxy for horizon=10 min travel-time delta
    df = df.rename(columns={"avg_arrival_delay_s": "target_s"})
    df = df.dropna(subset=["h3", "ts_floor_1m", "target_s"])  # keep simple
    # Features
    df["minute_of_day"] = pd.to_datetime(df["ts_floor_1m"]).dt.hour * 60 + pd.to_datetime(df["ts_floor_1m"]).dt.minute
    df["active_trips"].fillna(0, inplace=True)
    df["avg_departure_delay_s"].fillna(0, inplace=True)
    return df


def train_quantile(df: pd.DataFrame, out_dir: str) -> None:
    features = ["active_trips", "avg_departure_delay_s", "minute_of_day"]
    X = df[features].values.astype(np.float32)
    y = df["target_s"].values.astype(np.float32)

    os.makedirs(out_dir, exist_ok=True)

    models = {}
    for alpha in [0.5, 0.9]:
        params = {
            "objective": "quantile",
            "alpha": alpha,
            "verbosity": -1,
            "num_leaves": 31,
            "learning_rate": 0.1,
            "feature_pre_filter": False,
        }
        dtrain = lgb.Dataset(X, label=y)
        m = lgb.train(params, dtrain, num_boost_round=200)
        out_path = os.path.join(out_dir, f"lgb_quantile_p{int(alpha*100)}.txt")
        m.save_model(out_path)
        models[f"p{int(alpha*100)}"] = out_path

    meta = {"features": features, "models": models}
    with open(os.path.join(out_dir, "meta.json"), "w") as f:
        json.dump(meta, f, indent=2)
    print(f"Saved models to {out_dir}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    df = load_silver(args.data)
    train_quantile(df, args.out)


if __name__ == "__main__":
    main()

