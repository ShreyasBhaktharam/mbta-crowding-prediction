import pandas as pd

from models.datasets import DatasetConfig, assemble_features, load_dataset


def test_dataset_loader_from_parquet(tmp_path):
    df = pd.DataFrame(
        {
            "active_trips": [1, 2],
            "avg_departure_delay_s": [5.0, 3.0],
            "rolling_mean_7d": [4.0, 4.5],
            "hist_p50": [4.0, 4.2],
            "hist_p90": [5.0, 5.5],
            "label_p50": [6.0, 7.0],
            "origin_stop": ["o1", "o2"],
            "dest_stop": ["d1", "d2"],
            "minute": ["2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z"],
            "horizon_min": [10, 10],
        }
    )
    path = tmp_path / "gold.parquet"
    df.to_parquet(path, index=False)

    cfg = DatasetConfig(horizon_min=10, gold_path=str(path))
    loaded = load_dataset(cfg)
    assert len(loaded) == 2
    X, y = assemble_features(loaded, cfg)
    assert X.shape[1] == len(cfg.feature_columns)
    assert y.shape[0] == 2
