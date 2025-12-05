import numpy as np
import pandas as pd
from fastapi.testclient import TestClient

import serve.app as app_module
from features.store import FeatureStore
from models.datasets import DatasetConfig, assemble_features, load_dataset
from models.trainers.gbt import GBTTrainerConfig, LightGBMTrainer
from serve.app import app, feature_store_dep


def test_end_to_end_training_and_prediction(tmp_path, monkeypatch):
    df = pd.DataFrame(
        {
            "active_trips": [1, 2, 3, 4],
            "avg_departure_delay_s": [0.5, 0.2, 0.1, 0.0],
            "rolling_mean_7d": [2.0, 2.5, 3.0, 3.5],
            "hist_p50": [2.0, 2.6, 3.2, 3.6],
            "hist_p90": [2.5, 3.0, 3.5, 4.0],
            "label_p50": [3.0, 3.4, 3.8, 4.2],
            "origin_stop": ["o1", "o2", "o3", "o4"],
            "dest_stop": ["d1", "d2", "d3", "d4"],
            "minute": ["2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z", "2024-01-01T00:02:00Z", "2024-01-01T00:03:00Z"],
            "horizon_min": [10, 10, 10, 10],
        }
    )
    data_path = tmp_path / "gold.parquet"
    df.to_parquet(data_path, index=False)

    cfg = DatasetConfig(horizon_min=10, gold_path=str(data_path))
    loaded = load_dataset(cfg)
    X, y = assemble_features(loaded, cfg)
    trainer = LightGBMTrainer(
        GBTTrainerConfig(num_boost_round=10, n_trials=1, output_dir=str(tmp_path / "models"))
    )
    result = trainer.train(X, y)

    class InlineStore:
        def __init__(self):
            self._cache = {}
            self._cache_expiry = {}

        def _key(self, origin_stop, dest_stop, horizon_min):
            return f"{origin_stop}:{dest_stop}:{horizon_min}"

        def get_features(self, origin_stop, dest_stop, horizon_min):
            row = loaded.iloc[0].to_dict()
            row["h3"] = "stub"
            return row

        def snapshot(self):
            return {"o1:d1:10": loaded.iloc[0].to_dict()}

    import lightgbm as lgb

    class InlineModelService:
        _entry = object()

        def __init__(self, artifacts, feature_columns):
            self.p50 = lgb.Booster(model_file=artifacts["p50"])
            self.p90 = lgb.Booster(model_file=artifacts.get("p90", artifacts["p50"]))
            self.feature_columns = feature_columns

        def refresh(self):
            return None

        def predict(self, feature_rows):
            matrix = np.array([[row.get(col, 0.0) for col in self.feature_columns] for row in feature_rows], dtype=np.float32)
            return [
                {"p50": float(self.p50.predict(matrix)[0]), "p90": float(self.p90.predict(matrix)[0])}
                for _ in feature_rows
            ]

    app.dependency_overrides[feature_store_dep] = lambda: InlineStore()
    monkeypatch.setattr(app_module, "model_service", InlineModelService(result["artifacts"], cfg.feature_columns))

    client = TestClient(app)
    resp = client.post(
        "/predict",
        json={"requests": [{"origin_stop": "o1", "dest_stop": "d1", "horizon_min": 10}]},
    )
    assert resp.status_code == 200
    assert resp.json()[0]["p50"] > 0
    app.dependency_overrides.clear()
