from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from datetime import datetime
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

import pandas as pd

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

try:  # pragma: no cover - optional for TFT
    import torch
    from pytorch_forecasting.models import TemporalFusionTransformer
    TFT_AVAILABLE = True
    logger.info("TFT dependencies loaded successfully")
except Exception as e:  # pragma: no cover
    TFT_AVAILABLE = False
    torch = None
    TemporalFusionTransformer = None
    logger.warning(f"TFT dependencies not available: {e}")

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
    origin_name: Optional[str] = None
    dest_name: Optional[str] = None
    horizon_min: int
    p50: float
    p90: float
    p10: Optional[float] = None
    p95: Optional[float] = None


class ForecastQuery(BaseModel):
    origin_stop: str
    dest_stop: str
    horizon_min: int = Field(default=10, ge=5, le=120)


class ForecastRequest(BaseModel):
    requests: List[ForecastQuery]


class ForecastHorizon(BaseModel):
    horizon_min: int
    p50: float
    p90: float


class ForecastResponse(BaseModel):
    origin_stop: str
    dest_stop: str
    origin_name: Optional[str] = None
    dest_name: Optional[str] = None
    forecasts: List[ForecastHorizon]


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
        self._tft = None
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

            # Additionally load TFT model for /forecast endpoint if not already loaded
            if self._tft is None:
                tft_entry = self.registry.latest(model_name="tft")
                if tft_entry:
                    logger.info("Loading TFT model %s version %s", tft_entry.model_name, tft_entry.version)
                    self._load_tft_model(tft_entry)
                    MODEL_VERSION.labels(model=tft_entry.model_name, version=tft_entry.version).set(1)

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
            elif entry.model_name == "tft" and TFT_AVAILABLE:
                # TFT loading happens in _load_tft_model, but keep this branch for consistency
                pass
            else:
                self._models = {}

    def _load_tft_model(self, entry) -> None:
        """Load TFT model separately from GBT."""
        with self._lock:
            if entry.model_name == "tft" and TFT_AVAILABLE:
                artifact = entry.artifacts.get("checkpoint")
                if artifact and os.path.exists(artifact):
                    logger.info(f"Loading TFT checkpoint from {artifact}")
                    # PyTorch 2.6+ requires weights_only=False for models with custom classes
                    self._tft = TemporalFusionTransformer.load_from_checkpoint(artifact, weights_only=False)
                    self._tft.eval()  # Set to inference mode
                    if torch.cuda.is_available():
                        self._tft = self._tft.cuda()
                    logger.info("TFT model loaded successfully")

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

    def forecast_tft(self, historical_sequences: List[Optional[list]], horizons: List[int]) -> List[List[Dict[str, float]]]:
        """
        Generate TFT forecasts for multiple horizons.
        Returns list of forecasts, each containing predictions for requested horizons.
        """
        logger.info(f"forecast_tft called: TFT_AVAILABLE={TFT_AVAILABLE}, self._tft={self._tft is not None}")
        if not TFT_AVAILABLE or self._tft is None:
            # Return fallback forecasts
            logger.warning(f"TFT not available! TFT_AVAILABLE={TFT_AVAILABLE}, model_loaded={self._tft is not None}")
            return [[{"horizon_min": h, "p50": 0.0, "p90": 0.0} for h in horizons] for _ in historical_sequences]

        all_forecasts = []
        for hist_seq in historical_sequences:
            if hist_seq is None or len(hist_seq) == 0:
                # No historical data - return fallback
                all_forecasts.append([{"horizon_min": h, "p50": 0.0, "p90": 0.0} for h in horizons])
                continue

            try:
                # Build TFT input from historical sequence
                # Extract feature values from each timestep
                feature_names = self._feature_columns

                # Build sequence tensor: shape (seq_len, n_features)
                seq_features = []
                for timestamp, features in hist_seq:
                    feat_vec = [features.get(col, 0.0) for col in feature_names]
                    seq_features.append(feat_vec)

                # Pad or truncate to encoder length (16 timesteps)
                encoder_len = 16
                if len(seq_features) < encoder_len:
                    # Pad with zeros at the beginning
                    padding = [[0.0] * len(feature_names)] * (encoder_len - len(seq_features))
                    seq_features = padding + seq_features
                elif len(seq_features) > encoder_len:
                    # Take last encoder_len timesteps
                    seq_features = seq_features[-encoder_len:]

                # Run actual TFT inference
                # Build input batch dict that TFT expects
                # TFT requires: x_cat, x_cont, encoder_length, decoder_length, encoder_target, etc.

                # Convert features to tensor: (batch_size=1, seq_len=16, n_features)
                encoder_cont = torch.tensor([seq_features], dtype=torch.float32)

                # Build minimal batch dict for TFT
                # Based on pytorch-forecasting's batch structure
                batch = {
                    "encoder_cont": encoder_cont,  # (1, 16, n_features)
                    "encoder_cat": torch.zeros((1, 16, 0), dtype=torch.long),  # No categorical features
                    "decoder_cont": torch.zeros((1, 1, len(feature_names)), dtype=torch.float32),  # Empty decoder for 1-step ahead
                    "decoder_cat": torch.zeros((1, 1, 0), dtype=torch.long),
                    "encoder_lengths": torch.tensor([16], dtype=torch.long),  # Note: plural
                    "decoder_lengths": torch.tensor([1], dtype=torch.long),  # Note: plural
                    "encoder_target": encoder_cont[:, :, 0:1],  # Use first feature as target proxy
                    "decoder_target": torch.zeros((1, 1, 1), dtype=torch.float32),
                    "target_scale": torch.tensor([[0.0, 1.0]], dtype=torch.float32),  # (batch_size, 2) for (center, scale)
                }

                # Move to GPU if available
                if torch.cuda.is_available() and next(self._tft.parameters()).is_cuda:
                    batch = {k: v.cuda() if isinstance(v, torch.Tensor) else v for k, v in batch.items()}

                # TFT forward pass
                with torch.no_grad():
                    output = self._tft(batch)
                    # output['prediction'] shape: (batch_size, prediction_length, n_quantiles)
                    # For max_prediction_length=1: (1, 1, 2) where quantiles are [0.5, 0.9]
                    predictions = output['prediction'].cpu().numpy()

                # predictions[0, 0, :] gives [p50, p90] for the single prediction step
                p50_base = float(predictions[0, 0, 0])
                p90_base = float(predictions[0, 0, 1])

                # For multiple horizons, we'll use the single-step prediction as baseline
                # and scale by horizon distance (simple heuristic for multi-horizon)
                horizon_forecasts = []
                for horizon in horizons:
                    # Scale predictions slightly by horizon (further horizons have more uncertainty)
                    horizon_factor = 1.0 + (horizon / 100.0)  # e.g., 30min -> 1.3x factor
                    horizon_forecasts.append({
                        "horizon_min": horizon,
                        "p50": float(p50_base * horizon_factor),
                        "p90": float(p90_base * horizon_factor)
                    })

                all_forecasts.append(horizon_forecasts)

            except Exception as e:
                logger.warning(f"TFT forecast error: {e}")
                all_forecasts.append([{"horizon_min": h, "p50": 0.0, "p90": 0.0} for h in horizons])

        return all_forecasts

    @staticmethod
    def _fallback(row: Dict[str, float]) -> Dict[str, float]:
        baseline = float(row.get("rolling_mean_7d", 0.0))
        return {"p50": baseline, "p90": baseline * 1.2}


ROOT = Path(__file__).resolve().parents[1]
REGISTRY_DEFAULT = ROOT / "models" / "registry.json"
GTFS_STOPS_PATH = ROOT / "data" / "gtfs" / "stops.parquet"


class StopsService:
    """Loads and caches GTFS stops for ID ↔ name lookups."""

    def __init__(self, path: Path = GTFS_STOPS_PATH) -> None:
        self.path = path
        self._stops: Dict[str, str] = {}  # stop_id -> stop_name
        self._names: Dict[str, str] = {}  # stop_name (lower) -> stop_id
        self._loaded = False

    def _load(self) -> None:
        if self._loaded or not self.path.exists():
            return
        try:
            df = pd.read_parquet(self.path)
            # Filter to parent stations (place-*) for cleaner dropdown
            stations = df[df["stop_id"].str.startswith("place-", na=False)]
            for _, row in stations.iterrows():
                sid = str(row["stop_id"])
                name = str(row["stop_name"])
                self._stops[sid] = name
                self._names[name.lower()] = sid
            self._loaded = True
            logger.info(f"Loaded {len(self._stops)} stops from {self.path}")
        except Exception as e:
            logger.warning(f"Failed to load stops: {e}")

    def get_all(self) -> Dict[str, str]:
        """Return dict of stop_id -> stop_name."""
        self._load()
        return dict(self._stops)

    def id_to_name(self, stop_id: str) -> str:
        """Convert stop_id to human-readable name."""
        self._load()
        return self._stops.get(stop_id, stop_id)

    def name_to_id(self, name: str) -> Optional[str]:
        """Convert stop name to stop_id (case-insensitive)."""
        self._load()
        return self._names.get(name.lower())

    def resolve(self, identifier: str) -> str:
        """Accept either stop_id or stop_name, return stop_id."""
        self._load()
        if identifier in self._stops:
            return identifier
        resolved = self._names.get(identifier.lower())
        if resolved:
            return resolved
        return identifier  # fallback to original


stops_service = StopsService()
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


class StopInfo(BaseModel):
    stop_id: str
    stop_name: str


@app.get("/stops", response_model=List[StopInfo])
def get_stops():
    """Return list of available stops (parent stations) with ID and name."""
    REQUESTS.labels(endpoint="stops").inc()
    stops = stops_service.get_all()
    return [StopInfo(stop_id=sid, stop_name=name) for sid, name in sorted(stops.items(), key=lambda x: x[1])]


@app.get("/healthz")
def healthz():
    model_service.refresh()
    return {"model_loaded": model_service._entry is not None}


@app.post("/predict", response_model=List[PredictionResponse])
async def predict(payload: PredictionRequest, store: FeatureStore = Depends(feature_store_dep)):
    """
    Predict crowding for given origin/dest stops.
    Accepts either stop_id (e.g., 'place-dwnxg') or stop_name (e.g., 'Downtown Crossing').
    """
    REQUESTS.labels(endpoint="predict").inc()
    with LATENCY.labels(endpoint="predict").time():
        if not payload.requests:
            raise HTTPException(status_code=400, detail="No requests supplied")
        features = []
        resolved_requests = []
        for req in payload.requests:
            # Resolve stop names to IDs (accepts either)
            origin_id = stops_service.resolve(req.origin_stop)
            dest_id = stops_service.resolve(req.dest_stop)
            resolved_requests.append((origin_id, dest_id, req.horizon_min))

            cache_key = store._key(origin_id, dest_id, req.horizon_min)  # type: ignore[attr-defined]
            now = time.time()
            cached = cache_key in store._cache and store._cache_expiry.get(cache_key, 0) > now  # type: ignore[attr-defined]
            feature_payload = store.get_features(origin_id, dest_id, req.horizon_min)
            if cached:
                CACHE_HITS.inc()
            else:
                CACHE_MISSES.inc()
            # Add time-based features for model
            current_time = datetime.now()
            feature_payload["minute_of_day"] = current_time.hour * 60 + current_time.minute
            features.append(feature_payload)
        predictions = model_service.predict(features)
        responses = []
        for (origin_id, dest_id, horizon), preds in zip(resolved_requests, predictions):
            if not preds:
                raise HTTPException(status_code=HTTP_503_SERVICE_UNAVAILABLE, detail="Model unavailable")
            responses.append(
                PredictionResponse(
                    origin_stop=origin_id,
                    dest_stop=dest_id,
                    origin_name=stops_service.id_to_name(origin_id),
                    dest_name=stops_service.id_to_name(dest_id),
                    horizon_min=horizon,
                    p50=float(preds.get("p50", preds.get("mean", 0.0))),
                    p90=float(preds.get("p90", preds.get("p50", 0.0))),
                    p10=preds.get("p10"),
                    p95=preds.get("p95"),
                )
            )
        return responses


@app.post("/forecast", response_model=List[ForecastResponse])
async def forecast(payload: ForecastRequest, store: FeatureStore = Depends(feature_store_dep)):
    """
    Multi-step time series forecast using TFT.
    Returns predictions for horizons: 5, 10, 15, 20, 30 minutes.
    """
    REQUESTS.labels(endpoint="forecast").inc()
    with LATENCY.labels(endpoint="forecast").time():
        if not payload.requests:
            raise HTTPException(status_code=400, detail="No requests supplied")

        responses = []
        horizons = [5, 10, 15, 20, 30]

        for req in payload.requests:
            # Resolve stop names to IDs
            origin_id = stops_service.resolve(req.origin_stop)
            dest_id = stops_service.resolve(req.dest_stop)

            # Query historical sequences for this route
            historical_seq = store.get_historical_features(origin_id, dest_id, req.horizon_min, lookback=16)

            # Debug logging
            if historical_seq is None:
                logger.warning(f"No historical data found for {origin_id}->{dest_id} horizon={req.horizon_min}")
            else:
                logger.info(f"Found {len(historical_seq)} historical datapoints for {origin_id}->{dest_id} horizon={req.horizon_min}")

            # Generate forecasts for multiple horizons
            forecasts = model_service.forecast_tft([historical_seq], horizons)[0]

            responses.append(
                ForecastResponse(
                    origin_stop=origin_id,
                    dest_stop=dest_id,
                    origin_name=stops_service.id_to_name(origin_id),
                    dest_name=stops_service.id_to_name(dest_id),
                    forecasts=[
                        ForecastHorizon(
                            horizon_min=f["horizon_min"],
                            p50=f["p50"],
                            p90=f["p90"]
                        )
                        for f in forecasts
                    ]
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
                parts = key.split(":")
                if len(parts) != 3:
                    continue
                origin, dest, horizon = parts
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
