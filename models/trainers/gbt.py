from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import lightgbm as lgb
import numpy as np
import optuna

from models.metrics import mae, pinball_loss, rmse


@dataclass
class GBTTrainerConfig:
    quantiles: Iterable[float] = (0.5, 0.9)
    num_boost_round: int = 300
    n_trials: int = 20
    early_stopping_rounds: int = 30
    output_dir: str = field(default_factory=lambda: os.path.join("models", "artifacts"))


class LightGBMTrainer:
    def __init__(self, config: GBTTrainerConfig):
        self.config = config
        self.quantiles = list(config.quantiles) or [0.5]
        Path(self.config.output_dir).mkdir(parents=True, exist_ok=True)

    def _split(self, X: np.ndarray, y: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        split = max(int(len(X) * 0.8), 1)
        return X[:split], X[split:], y[:split], y[split:]

    def _tune(self, X_train: np.ndarray, y_train: np.ndarray, X_val: np.ndarray, y_val: np.ndarray) -> Dict:
        def objective(trial: optuna.Trial) -> float:
            params = {
                "objective": "quantile",
                "alpha": self.quantiles[0],
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "num_leaves": trial.suggest_int("num_leaves", 16, 128),
                "feature_fraction": trial.suggest_float("feature_fraction", 0.5, 1.0),
                "bagging_fraction": trial.suggest_float("bagging_fraction", 0.5, 1.0),
                "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 10, 200),
                "verbosity": -1,
            }
            train_ds = lgb.Dataset(X_train, label=y_train)
            val_ds = lgb.Dataset(X_val, label=y_val)
            booster = lgb.train(
                params,
                train_ds,
                valid_sets=[val_ds],
                num_boost_round=self.config.num_boost_round,
                callbacks=[
                    lgb.early_stopping(self.config.early_stopping_rounds, verbose=False),
                    lgb.log_evaluation(period=0),
                ],
            )
            preds = booster.predict(X_val)
            return pinball_loss(y_val, preds, float(self.quantiles[0]))

        study = optuna.create_study(direction="minimize")
        study.optimize(objective, n_trials=self.config.n_trials, show_progress_bar=False)
        best = study.best_params
        best.pop("alpha", None)
        return best

    def train(self, X: np.ndarray, y: np.ndarray) -> Dict[str, Dict]:
        X_train, X_val, y_train, y_val = self._split(X, y)
        tuned_params = self._tune(X_train, y_train, X_val, y_val)
        artifacts: Dict[str, str] = {}
        metrics: Dict[str, float] = {}
        feature_imp: Dict[str, float] = {}

        for quantile in self.quantiles:
            params = {
                **tuned_params,
                "objective": "quantile",
                "alpha": quantile,
                "verbosity": -1,
            }
            train_ds = lgb.Dataset(X_train, label=y_train)
            booster = lgb.train(
                params,
                train_ds,
                num_boost_round=self.config.num_boost_round,
                valid_sets=[lgb.Dataset(X_val, label=y_val)],
                callbacks=[
                    lgb.early_stopping(self.config.early_stopping_rounds, verbose=False),
                    lgb.log_evaluation(period=0),
                ],
            )
            preds = booster.predict(X_val)
            metrics[f"val_pinball_p{int(quantile * 100)}"] = pinball_loss(y_val, preds, float(quantile))
            metrics[f"val_mae_p{int(quantile * 100)}"] = mae(y_val, preds)
            metrics[f"val_rmse_p{int(quantile * 100)}"] = rmse(y_val, preds)
            out_path = Path(self.config.output_dir) / f"lgb_quantile_p{int(quantile * 100)}.txt"
            booster.save_model(str(out_path))
            artifacts[f"p{int(quantile * 100)}"] = str(out_path)
            feature_imp[f"p{int(quantile * 100)}"] = float(np.mean(booster.feature_importance()))

        metadata_path = Path(self.config.output_dir) / "metadata.json"
        with metadata_path.open("w", encoding="utf-8") as f:
            json.dump({"params": tuned_params, "quantiles": list(self.config.quantiles)}, f, indent=2)
        artifacts["metadata"] = str(metadata_path)

        return {"artifacts": artifacts, "metrics": metrics, "feature_importance": feature_imp}
