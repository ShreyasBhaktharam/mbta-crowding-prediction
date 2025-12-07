from fastapi.testclient import TestClient

import serve.app as app_module
from serve.app import app, feature_store_dep


class StubFeatureStore:
    def __init__(self):
        self._cache = {}
        self._cache_expiry = {}

    def _key(self, origin_stop, dest_stop, horizon_min):  # noqa: D401
        return f"{origin_stop}:{dest_stop}:{horizon_min}"

    def get_features(self, origin_stop, dest_stop, horizon_min):
        return {"rolling_mean_7d": 5.0, "h3": "884"}

    def snapshot(self):
        return {"origin:dest:10": {"rolling_mean_7d": 5.0, "h3": "884"}}


class StubModelService:
    _entry = object()

    def refresh(self, *_, **__):
        return None

    def predict(self, features):
        return [{"p50": 4.0, "p90": 6.0} for _ in features]


def test_predict_and_metrics(monkeypatch):
    stub_store = StubFeatureStore()
    app.dependency_overrides[feature_store_dep] = lambda: stub_store
    monkeypatch.setattr(app_module, "model_service", StubModelService())
    client = TestClient(app)

    resp = client.post(
        "/predict",
        json={"requests": [{"origin_stop": "origin", "dest_stop": "dest", "horizon_min": 10}]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body[0]["p50"] == 4.0

    stream_resp = client.get("/crowding_map")
    assert stream_resp.status_code == 200

    metrics_resp = client.get("/metrics")
    assert metrics_resp.status_code == 200

    app.dependency_overrides.clear()
