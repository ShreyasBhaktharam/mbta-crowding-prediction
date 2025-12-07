from features.store import DuckDBBackend, FeatureStore


def test_feature_store_caching(tmp_path):
    backend = DuckDBBackend(path=str(tmp_path / "fs.duckdb"))
    store = FeatureStore(backend=backend, ttl_seconds=60)
    store.set_features("origin", "dest", 10, {"rolling_mean_7d": 5.0})
    fetched = store.get_features("origin", "dest", 10)
    assert fetched["rolling_mean_7d"] == 5.0

    backend.write(store._key("origin", "dest", 10), {"rolling_mean_7d": 3.0}, ttl=60)  # type: ignore[attr-defined]
    cached = store.get_features("origin", "dest", 10)
    assert cached["rolling_mean_7d"] == 5.0  # still cached

    snapshot = store.snapshot()
    assert store._key("origin", "dest", 10) in snapshot  # type: ignore[attr-defined]
