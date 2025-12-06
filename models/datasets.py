from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from spark.utils import DEFAULT_DATA_ROOT, build_spark_session


@dataclass
class DatasetConfig:
    horizon_min: int
    start: Optional[str] = None
    end: Optional[str] = None
    gold_path: str = field(default_factory=lambda: str(DEFAULT_DATA_ROOT / "gold"))
    feature_columns: List[str] = field(
        default_factory=lambda: [
            "active_trips",
            "avg_departure_delay_s",
            "rolling_mean_7d",
            "hist_p50",
            "hist_p90",
        ]
    )
    label_column: str = "label_p50"


def load_dataset(config: DatasetConfig) -> pd.DataFrame:
    path = Path(config.gold_path)
    if path.is_file() and path.suffix == ".parquet":
        pdf = pd.read_parquet(path)
    else:
        spark = build_spark_session(app_name="citystream-datasets")
        df = spark.read.format("delta").load(config.gold_path)
        df = df.filter(F.col("horizon_min") == config.horizon_min)
        if config.start:
            df = df.filter(F.col("minute") >= F.lit(config.start))
        if config.end:
            df = df.filter(F.col("minute") <= F.lit(config.end))
        pdf = (
            df.select(*(config.feature_columns + [config.label_column, "origin_stop", "dest_stop", "minute"]))
            .dropna(subset=[config.label_column])  # avoid training on empty labels
            .toPandas()
        )
        spark.stop()
    return pdf


def assemble_features(df: pd.DataFrame, config: DatasetConfig) -> Tuple[np.ndarray, np.ndarray]:
    missing = [col for col in config.feature_columns + [config.label_column] if col not in df.columns]
    if missing:
        raise ValueError(f"Columns missing from dataset: {missing}")
    features = df[config.feature_columns].fillna(0.0).values.astype(np.float32)
    labels = df[config.label_column].fillna(0.0).values.astype(np.float32)
    return features, labels
