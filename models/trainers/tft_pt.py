from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import lightning.pytorch as pl
import torch
from pytorch_forecasting import TimeSeriesDataSet
from pytorch_forecasting.metrics import QuantileLoss
from pytorch_forecasting.models import TemporalFusionTransformer
from torch.utils.data import DataLoader


@dataclass
class TFTTrainerConfig:
    quantiles: List[float] = field(default_factory=lambda: [0.5, 0.9])
    max_epochs: int = 5
    learning_rate: float = 1e-3
    batch_size: int = 24
    hidden_size: int = 64
    attention_head_size: int = 2
    dropout: float = 0.1
    loss_monotone_constaints: Optional[dict] = None
    output_dir: str = field(default_factory=lambda: os.path.join("models", "tft"))
    use_gpu: bool = False
    max_encoder_length: int = 16
    max_prediction_length: int = 1  # horizon already baked into label
    num_workers: int = 0


class TFTTrainer:
    def __init__(self, config: TFTTrainerConfig):
        self.config = config
        Path(self.config.output_dir).mkdir(parents=True, exist_ok=True)

    def _get_known_reals(self, df: pd.DataFrame) -> List[str]:
        # Prefer curated feature list; fall back to any numeric columns if empty
        preferred = [
            "active_trips",
            "avg_departure_delay_s",
            "rolling_mean_7d",
            "hist_p50",
            "hist_p90",
            "avg_arrival_delay_s",
            "avg_temp_c",
            "avg_wind_speed",
            "avg_precip_mm",
            "rolling_std_7d",
            "crowding_score",
        ]
        available = [c for c in preferred if c in df.columns]
        exclude = {"label_p50", "minute", "origin_stop", "dest_stop", "h3", "time_idx", "series_id"}
        if not available:
            available = [c for c in df.columns if c not in exclude]
        return available

    def _build_dataset(self, df: pd.DataFrame, known_reals: List[str]) -> TimeSeriesDataSet:
        target = "label_p50"
        min_enc = int(min(self.config.max_encoder_length, max(4, int(df["time_idx"].max()) + 1)))
        dataset = TimeSeriesDataSet(
            df,
            time_idx="time_idx",
            target=target,
            group_ids=["series_id"],
            min_encoder_length=min_enc,
            max_encoder_length=int(self.config.max_encoder_length),
            min_prediction_length=int(self.config.max_prediction_length),
            max_prediction_length=int(self.config.max_prediction_length),
            static_categoricals=[],
            time_varying_known_categoricals=[],
            time_varying_unknown_categoricals=[],
            static_reals=[],
            time_varying_known_reals=[],
            time_varying_unknown_reals=known_reals,
            target_normalizer=None,
            allow_missing_timesteps=True,
        )
        return dataset

    def train(self, df: pd.DataFrame) -> Dict[str, Dict]:
        device = "cuda" if self.config.use_gpu and torch.cuda.is_available() else "cpu"
        df = df.copy()
        df["time_idx"] = pd.factorize(df["minute"])[0]
        df["series_id"] = df["origin_stop"].astype(str) + "_" + df["dest_stop"].astype(str)
        known_reals = self._get_known_reals(df)
        if not known_reals:
            raise ValueError("No features available for TFT training.")
        df[known_reals] = df[known_reals].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        dataset = self._build_dataset(df, known_reals)
        # Use full dataset for train/val; to_dataloader pads variable lengths correctly
        train_ds = dataset
        val_ds = dataset
        train_loader = train_ds.to_dataloader(train=True, batch_size=self.config.batch_size, num_workers=self.config.num_workers)
        val_loader = val_ds.to_dataloader(train=False, batch_size=self.config.batch_size, num_workers=self.config.num_workers)

        loss = QuantileLoss(quantiles=self.config.quantiles)
        model = TemporalFusionTransformer.from_dataset(
            train_ds,
            learning_rate=self.config.learning_rate,
            hidden_size=self.config.hidden_size,
            attention_head_size=self.config.attention_head_size,
            dropout=self.config.dropout,
            loss=loss,
        )

        trainer = pl.Trainer(
            max_epochs=self.config.max_epochs,
            accelerator="gpu" if device == "cuda" else "cpu",
            devices=1,
            enable_progress_bar=True,
            enable_checkpointing=False,
            logger=False,
            # Cap batches to keep local runs fast; increase for full training
            limit_train_batches=200,
            limit_val_batches=20,
        )
        trainer.fit(model, train_loader, val_loader)
        val_metrics = trainer.validate(model, dataloaders=val_loader, verbose=False)[0]

        out_path = Path(self.config.output_dir) / "tft.ckpt"
        trainer.save_checkpoint(str(out_path))
        artifacts = {"checkpoint": str(out_path)}
        metrics = {k: float(v) for k, v in val_metrics.items()}
        return {"artifacts": artifacts, "metrics": metrics}
