import json
import os
from typing import Optional

import numpy as np
import lightgbm as lgb
from fastapi import FastAPI, Query
from pydantic import BaseModel


class PredictResponse(BaseModel):
    p50: float
    p90: float
    unit: str = "seconds"


def load_models(models_dir: str):
    meta_path = os.path.join(models_dir, "meta.json")
    if not os.path.exists(meta_path):
        return None, None, None
    with open(meta_path, "r") as f:
        meta = json.load(f)
    features = meta["features"]
    m50 = lgb.Booster(model_file=meta["models"]["p50"]) if meta["models"].get("p50") else None
    m90 = lgb.Booster(model_file=meta["models"]["p90"]) if meta["models"].get("p90") else None
    return features, m50, m90


MODELS_DIR = os.environ.get("MODELS_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "models")))
FEATURES, M50, M90 = load_models(MODELS_DIR)

app = FastAPI(title="CityStream MBTA API")


@app.get("/healthz")
def healthz():
    ok = bool(M50 and M90 and FEATURES)
    return {"ok": ok}


@app.get("/predict", response_model=PredictResponse)
def predict(
    origin_stop: str = Query(..., min_length=1),
    dest_stop: str = Query(..., min_length=1),
    horizon_min: int = Query(10, ge=5, le=30),
    active_trips: int = Query(3, ge=0),
    avg_departure_delay_s: float = Query(0.0),
    minute_of_day: Optional[int] = Query(None),
):
    # Minimal feature construction; in production fetch from feature store
    if minute_of_day is None:
        minute_of_day = 8 * 60  # assume 8am
    x = np.array([[float(active_trips), float(avg_departure_delay_s), float(minute_of_day)]], dtype=np.float32)
    if not (M50 and M90):
        # Return simple baseline if models are missing
        base = float(avg_departure_delay_s)
        return PredictResponse(p50=base, p90=base * 1.5)
    p50 = float(M50.predict(x)[0])
    p90 = float(M90.predict(x)[0])
    # Adjust by horizon as a naive scaling (placeholder)
    scale = max(1.0, horizon_min / 10.0)
    return PredictResponse(p50=p50 * scale, p90=p90 * scale)

