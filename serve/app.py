from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, StreamingResponse
from loguru import logger
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, Field
from starlette.status import HTTP_503_SERVICE_UNAVAILABLE
from starlette.staticfiles import StaticFiles

from features.store import FeatureStore, get_feature_store, snapshot
from models.registry import ModelRegistry

try:  # pragma: no cover - optional for transformer
    import lightgbm as lgb
except Exception:  # pragma: no cover
    lgb = None

try:  # pragma: no cover
    from nixtla import ChronosPipeline
except Exception:  # pragma: no cover
    ChronosPipeline = None

DEFAULT_FEATURES = [
    "active_trips",
    "avg_departure_delay_s",
    "rolling_mean_7d",
    "hist_p50",
    "hist_p90",
]

REQUESTS = Counter("citystream_api_requests", "Total API requests", ["endpoint"])
LATENCY = Histogram("citystream_request_latency_seconds", "Request latency", ["endpoint"])
CACHE_HITS = Counter("citystream_feature_cache_hits", "Feature cache hits")
CACHE_MISSES = Counter("citystream_feature_cache_misses", "Feature cache misses")
MODEL_VERSION = Gauge("citystream_model_version_info", "Active model version", ["model", "version"])


class PredictionQuery(BaseModel):
    origin_stop: str
    dest_stop: str
    horizon_min: int = Field(..., ge=5, le=120)


class PredictionRequest(BaseModel):
    requests: List[PredictionQuery]


class PredictionResponse(BaseModel):
    origin_stop: str
    dest_stop: str
    horizon_min: int
    p50: float
    p90: float
    p10: Optional[float] = None
    p95: Optional[float] = None


class ModelService:
    def __init__(self) -> None:
        registry_path = Path(os.getenv("MODEL_REGISTRY_PATH", str(REGISTRY_DEFAULT)))
        self.registry = ModelRegistry(path=registry_path)
        self.registry_file = registry_path
        self._lock = threading.Lock()
        self._mtime = 0.0
        self._entry = None
        self._feature_columns = list(DEFAULT_FEATURES)
        self._models: Dict[str, any] = {}
        self._chronos = None
        self.refresh(force=True)

    def refresh(self, force: bool = False) -> None:
        if not self.registry_file.exists():
            return
        mtime = self.registry_file.stat().st_mtime
        if force or mtime > self._mtime:
            entry = self.registry.latest()
            if entry:
                logger.info("Loading model %s version %s", entry.model_name, entry.version)
                self._load_entry(entry)
                self._mtime = mtime
                MODEL_VERSION.labels(model=entry.model_name, version=entry.version).set(1)

    def _load_entry(self, entry) -> None:
        with self._lock:
            self._entry = entry
            dataset_cfg = entry.params.get("dataset", {}) if isinstance(entry.params, dict) else {}
            self._feature_columns = dataset_cfg.get("feature_columns", DEFAULT_FEATURES)
            if entry.model_name == "gbt" and lgb is not None:
                models = {}
                for name, artifact in entry.artifacts.items():
                    if name.startswith("p") and os.path.exists(artifact):
                        models[name] = lgb.Booster(model_file=artifact)
                self._models = models
            elif entry.model_name == "transformer" and ChronosPipeline is not None:
                artifact = entry.artifacts.get("chronos")
                if artifact and os.path.exists(artifact):
                    self._chronos = ChronosPipeline.load(artifact)
            else:
                self._models = {}

    def predict(self, feature_rows: List[Dict[str, float]]) -> List[Dict[str, float]]:
        self.refresh()
        if self._entry is None:
            return [self._fallback(row) for row in feature_rows]
        if self._entry.model_name == "gbt" and self._models:
            matrix = np.array([[row.get(col, 0.0) for col in self._feature_columns] for row in feature_rows], dtype=np.float32)
            preds: Dict[str, np.ndarray] = {}
            for name, model in self._models.items():
                preds[name] = model.predict(matrix)
            responses = []
            for idx, row in enumerate(feature_rows):
                p50 = float(preds.get("p50", np.zeros(len(matrix)))[idx])
                p90 = float(preds.get("p90", np.zeros(len(matrix)))[idx])
                responses.append({"p50": p50, "p90": p90})
            return responses
        if self._entry.model_name == "transformer" and self._chronos is not None:
            responses = []
            for row in feature_rows:
                series = [[row.get("rolling_mean_7d", 0.0)]]
                quantiles = self._chronos.predict(series)
                responses.append(
                    {
                        "p10": float(quantiles.get("p10", 0.0)),
                        "p50": float(quantiles.get("p50", 0.0)),
                        "p90": float(quantiles.get("p90", 0.0)),
                        "p95": float(quantiles.get("p95", 0.0)),
                    }
                )
            return responses
        return [self._fallback(row) for row in feature_rows]

    @staticmethod
    def _fallback(row: Dict[str, float]) -> Dict[str, float]:
        baseline = float(row.get("rolling_mean_7d", 0.0))
        return {"p50": baseline, "p90": baseline * 1.2}


ROOT = Path(__file__).resolve().parents[1]
REGISTRY_DEFAULT = ROOT / "models" / "registry.json"

model_service = ModelService()
app = FastAPI(title="CityStream MBTA API", version="2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def feature_store_dep() -> FeatureStore:
    return get_feature_store()


@app.get("/healthz")
def healthz():
    model_service.refresh()
    return {"model_loaded": model_service._entry is not None}


@app.post("/predict", response_model=List[PredictionResponse])
async def predict(payload: PredictionRequest, store: FeatureStore = Depends(feature_store_dep)):
    REQUESTS.labels(endpoint="predict").inc()
    with LATENCY.labels(endpoint="predict").time():
        if not payload.requests:
            raise HTTPException(status_code=400, detail="No requests supplied")
        features = []
        for req in payload.requests:
            cache_key = store._key(req.origin_stop, req.dest_stop, req.horizon_min)  # type: ignore[attr-defined]
            now = time.time()
            cached = cache_key in store._cache and store._cache_expiry.get(cache_key, 0) > now  # type: ignore[attr-defined]
            feature_payload = store.get_features(req.origin_stop, req.dest_stop, req.horizon_min)
            if cached:
                CACHE_HITS.inc()
            else:
                CACHE_MISSES.inc()
            features.append(feature_payload)
        predictions = model_service.predict(features)
        responses = []
        for req, preds in zip(payload.requests, predictions):
            if not preds:
                raise HTTPException(status_code=HTTP_503_SERVICE_UNAVAILABLE, detail="Model unavailable")
            responses.append(
                PredictionResponse(
                    origin_stop=req.origin_stop,
                    dest_stop=req.dest_stop,
                    horizon_min=req.horizon_min,
                    p50=float(preds.get("p50", preds.get("mean", 0.0))),
                    p90=float(preds.get("p90", preds.get("p50", 0.0))),
                    p10=preds.get("p10"),
                    p95=preds.get("p95"),
                )
            )
        return responses


@app.get("/crowding_map")
async def crowding_map():
    REQUESTS.labels(endpoint="crowding_map").inc()

    async def event_stream():
        while True:
            snapshot_data = snapshot()
            payload = []
            for key, values in snapshot_data.items():
                origin, dest, horizon = key.split(":")
                payload.append(
                    {
                        "origin_stop": origin,
                        "dest_stop": dest,
                        "horizon_min": int(horizon),
                        "h3": values.get("h3", "unknown"),
                        "rolling_mean": values.get("rolling_mean_7d", 0.0),
                        "p50": values.get("hist_p50", values.get("rolling_mean_7d", 0.0)),
                        "p90": values.get("hist_p90", values.get("rolling_mean_7d", 0.0) * 1.2),
                    }
                )
            yield f"data: {json.dumps(payload)}\n\n"
            await asyncio.sleep(2)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/metrics")
def metrics():
    data = generate_latest()
    return PlainTextResponse(data.decode("utf-8"), media_type=CONTENT_TYPE_LATEST)


ui_dist = Path(__file__).resolve().parents[1] / "ui" / "dist"
if ui_dist.exists():
    app.mount("/", StaticFiles(directory=str(ui_dist), html=True), name="ui")
