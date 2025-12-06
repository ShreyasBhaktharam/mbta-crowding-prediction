from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

import duckdb
import pandas as pd

try:
    import redis
except ImportError:  # pragma: no cover - redis optional in CI
    redis = None

from pyspark.sql import functions as F

from spark.utils import DEFAULT_DATA_ROOT, build_spark_session


class Backend:
    def read(self, key: str) -> Optional[Dict[str, float]]:
        raise NotImplementedError

    def write(self, key: str, payload: Dict[str, float], ttl: int) -> None:
        raise NotImplementedError

    def scan(self) -> Dict[str, Dict[str, float]]:
        raise NotImplementedError


class RedisBackend(Backend):
    def __init__(self) -> None:
        if redis is None:
            raise RuntimeError("redis package not installed")
        self.client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            password=os.getenv("REDIS_PASSWORD"),
            db=int(os.getenv("REDIS_DB", "0")),
            decode_responses=True,
        )

    def read(self, key: str) -> Optional[Dict[str, float]]:
        data = self.client.hgetall(key)
        if not data:
            return None
        return {k: float(v) for k, v in data.items()}

    def write(self, key: str, payload: Dict[str, float], ttl: int) -> None:
        self.client.hset(key, mapping={k: str(v) for k, v in payload.items()})
        if ttl:
            self.client.expire(key, ttl)

    def scan(self) -> Dict[str, Dict[str, float]]:
        keys = []
        cursor = 0
        while True:
            cursor, batch = self.client.scan(cursor=cursor, match="*")
            keys.extend(batch)
            if cursor == 0:
                break
        result = {}
        for key in keys:
            payload = self.client.hgetall(key)
            if payload:
                result[key] = {k: float(v) for k, v in payload.items()}
        return result


class DuckDBBackend(Backend):
    def __init__(self, path: Optional[str] = None) -> None:
        self.path = path or os.path.join(str(DEFAULT_DATA_ROOT), "duckdb_features.duckdb")
        self.conn = duckdb.connect(self.path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS online_features (
                cache_key TEXT PRIMARY KEY,
                data JSON,
                updated_at TIMESTAMP
            )
            """
        )

    def read(self, key: str) -> Optional[Dict[str, float]]:
        result = self.conn.execute(
            "SELECT data FROM online_features WHERE cache_key = ?", [key]
        ).fetchone()
        if not result:
            return None
        payload = json.loads(result[0])
        return {k: float(v) for k, v in payload.items()}

    def write(self, key: str, payload: Dict[str, float], ttl: int) -> None:  # ttl kept for parity
        self.conn.execute("DELETE FROM online_features WHERE cache_key = ?", [key])
        self.conn.execute(
            "INSERT INTO online_features VALUES (?, ?, CURRENT_TIMESTAMP)",
            [key, json.dumps(payload)],
        )

    def scan(self) -> Dict[str, Dict[str, float]]:
        rows = self.conn.execute("SELECT cache_key, data FROM online_features").fetchall()
        result = {}
        for cache_key, payload in rows:
            result[cache_key] = {k: float(v) for k, v in json.loads(payload).items()}
        return result


@dataclass
class FeatureStore:
    backend: Backend
    offline_path: str = os.path.join(str(DEFAULT_DATA_ROOT), "gold")
    ttl_seconds: int = int(os.getenv("FEATURE_CACHE_TTL", "300"))

    def __post_init__(self) -> None:
        self._cache: Dict[str, Dict[str, float]] = {}
        self._cache_expiry: Dict[str, float] = {}

    def _key(self, origin: str, dest: str, horizon: int) -> str:
        return f"{origin}:{dest}:{horizon}"

    def get_features(self, origin_stop: str, dest_stop: str, horizon_min: int) -> Dict[str, float]:
        key = self._key(origin_stop, dest_stop, horizon_min)
        now = time.time()
        if key in self._cache and self._cache_expiry.get(key, 0) > now:
            return self._cache[key]
        payload = self.backend.read(key)
        if payload is None:
            payload = {
                "active_trips": 0.0,
                "avg_departure_delay_s": 0.0,
                "rolling_mean_7d": 0.0,
                "h3": "unknown",
            }
        else:
            payload.setdefault("active_trips", 0.0)
            payload.setdefault("avg_departure_delay_s", 0.0)
            payload.setdefault("rolling_mean_7d", 0.0)
            payload.setdefault("h3", "unknown")
        self._cache[key] = payload
        self._cache_expiry[key] = now + self.ttl_seconds
        return payload

    def set_features(self, origin_stop: str, dest_stop: str, horizon_min: int, payload: Dict[str, float]) -> None:
        key = self._key(origin_stop, dest_stop, horizon_min)
        self.backend.write(key, payload, self.ttl_seconds)
        self._cache[key] = payload
        self._cache_expiry[key] = time.time() + self.ttl_seconds

    def load_training_features(self, horizon_min: int, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        spark = build_spark_session(app_name="feature-store-loader")
        df = spark.read.format("delta").load(self.offline_path).filter(F.col("horizon_min") == horizon_min)
        if start:
            df = df.filter(F.col("minute") >= F.lit(start))
        if end:
            df = df.filter(F.col("minute") <= F.lit(end))
        pdf = df.toPandas()
        spark.stop()
        return pdf

    def snapshot(self) -> Dict[str, Dict[str, float]]:
        return self.backend.scan()


_STORE: Optional[FeatureStore] = None


def _build_backend() -> Backend:
    backend = os.getenv("FEATURE_STORE_BACKEND", "redis").lower()
    if backend == "redis":
        try:
            return RedisBackend()
        except Exception:
            return DuckDBBackend()
    return DuckDBBackend()


def get_feature_store() -> FeatureStore:
    global _STORE
    if _STORE is None:
        _STORE = FeatureStore(backend=_build_backend())
    return _STORE


def get_features(origin_stop: str, dest_stop: str, horizon_min: int) -> Dict[str, float]:
    store = get_feature_store()
    return store.get_features(origin_stop, dest_stop, horizon_min)


def load_training_features(horizon_min: int, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    store = get_feature_store()
    return store.load_training_features(horizon_min, start, end)


def snapshot() -> Dict[str, Dict[str, float]]:
    return get_feature_store().snapshot()
