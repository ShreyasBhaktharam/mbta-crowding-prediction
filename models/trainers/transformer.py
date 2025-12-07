from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict

import pandas as pd

try:  # pragma: no cover - optional GPU dependency
    from nixtla import ChronosPipeline  # type: ignore
except Exception:  # pragma: no cover
    ChronosPipeline = None

try:  # pragma: no cover - GPU optional
    import torch
except Exception:  # pragma: no cover
    torch = None


@dataclass
class TransformerConfig:
    """Configuration for Chronos fine-tuning.

    Chronos (Nixtla) gives us a Temporal Fusion Transformer-style backbone that
    natively supports multi-horizon probabilistic forecasts and GPU acceleration,
    which aligns with the MBTA latency and quantile coverage requirements.
    """

    prediction_length: int = 6
    max_epochs: int = 5
    learning_rate: float = 1e-3
    batch_size: int = 64
    use_gpu: bool = False
    output_dir: str = field(default_factory=lambda: os.path.join("models", "transformer"))
    checkpoint: str = "chronos-tiny"


class ChronosTrainer:
    def __init__(self, config: TransformerConfig):
        self.config = config
        Path(self.config.output_dir).mkdir(parents=True, exist_ok=True)
        if self.config.use_gpu and (torch is None or not torch.cuda.is_available()):
            raise RuntimeError("GPU requested but CUDA is not available")
        if ChronosPipeline is None:
            raise RuntimeError("Install nixtla to use ChronosTrainer")

    def train(self, df: pd.DataFrame) -> Dict[str, Dict]:
        required_cols = {"minute", "label_p50", "origin_stop", "dest_stop"}
        if not required_cols.issubset(df.columns):
            raise ValueError(f"ChronosTrainer requires columns: {required_cols}")
        dataset = ChronosPipeline.from_dataframe(
            df,
            time_col="minute",
            target_col="label_p50",
            id_cols=["origin_stop", "dest_stop"],
            freq="1min",
        )
        pipeline = ChronosPipeline.from_pretrained(self.config.checkpoint, prediction_length=self.config.prediction_length)
        pipeline.fit(
            dataset,
            learning_rate=self.config.learning_rate,
            max_epochs=self.config.max_epochs,
            batch_size=self.config.batch_size,
            gpus=1 if self.config.use_gpu else 0,
        )
        metrics = pipeline.evaluate(dataset)
        out_path = Path(self.config.output_dir) / "chronos.pt"
        pipeline.save(str(out_path))
        with (Path(self.config.output_dir) / "chronos_metrics.json").open("w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
        return {"artifacts": {"chronos": str(out_path)}, "metrics": metrics}
