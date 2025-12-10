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


def _safe_float(value: str) -> any:
    """Convert to float if possible, otherwise return original string."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return value


class Backend:
    def read(self, key: str) -> Optional[Dict[str, any]]:
        raise NotImplementedError

    def write(self, key: str, payload: Dict[str, any], ttl: int) -> None:
        raise NotImplementedError

    def scan(self) -> Dict[str, Dict[str, any]]:
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

    def read(self, key: str) -> Optional[Dict[str, any]]:
        data = self.client.hgetall(key)
        if not data:
            return None
        return {k: _safe_float(v) for k, v in data.items()}

    def write(self, key: str, payload: Dict[str, float], ttl: int) -> None:
        self.client.hset(key, mapping={k: str(v) for k, v in payload.items()})
        if ttl:
            self.client.expire(key, ttl)

    def scan(self) -> Dict[str, Dict[str, any]]:
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
                result[key] = {k: _safe_float(v) for k, v in payload.items()}
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

    def read(self, key: str) -> Optional[Dict[str, any]]:
        result = self.conn.execute(
            "SELECT data FROM online_features WHERE cache_key = ?", [key]
        ).fetchone()
        if not result:
            return None
        payload = json.loads(result[0])
        return {k: _safe_float(v) for k, v in payload.items()}

    def write(self, key: str, payload: Dict[str, float], ttl: int) -> None:  # ttl kept for parity
        self.conn.execute("DELETE FROM online_features WHERE cache_key = ?", [key])
        self.conn.execute(
            "INSERT INTO online_features VALUES (?, ?, CURRENT_TIMESTAMP)",
            [key, json.dumps(payload)],
        )

    def scan(self) -> Dict[str, Dict[str, any]]:
        rows = self.conn.execute("SELECT cache_key, data FROM online_features").fetchall()
        result = {}
        for cache_key, payload in rows:
            result[cache_key] = {k: _safe_float(v) for k, v in json.loads(payload).items()}
        return result


@dataclass
class FeatureStore:
    backend: Backend
    offline_path: str = os.path.join(str(DEFAULT_DATA_ROOT), "gold")
    ttl_seconds: int = int(os.getenv("FEATURE_CACHE_TTL", "300"))

    def __post_init__(self) -> None:
        self._cache: Dict[str, Dict[str, float]] = {}
        self._cache_expiry: Dict[str, float] = {}
        self._stop_hierarchy: Dict[str, list] = {}  # parent_station -> [child_stop_ids]
        self._hierarchy_loaded = False

    def _load_stop_hierarchy(self) -> None:
        """Load parent station -> child stops mapping from GTFS."""
        if self._hierarchy_loaded:
            return
        stops_path = os.path.join(str(DEFAULT_DATA_ROOT), "gtfs", "stops.parquet")
        if not os.path.exists(stops_path):
            self._hierarchy_loaded = True
            return
        try:
            df = pd.read_parquet(stops_path)
            # Build parent -> children mapping
            for _, row in df.iterrows():
                parent = row.get("parent_station")
                stop_id = str(row.get("stop_id", ""))
                if parent and pd.notna(parent) and stop_id:
                    parent = str(parent)
                    if parent not in self._stop_hierarchy:
                        self._stop_hierarchy[parent] = []
                    self._stop_hierarchy[parent].append(stop_id)
            self._hierarchy_loaded = True
        except Exception:
            self._hierarchy_loaded = True

    def _get_child_stops(self, stop_id: str) -> list:
        """Get child stops for a parent station, or [stop_id] if not a parent."""
        self._load_stop_hierarchy()
        if stop_id in self._stop_hierarchy:
            return self._stop_hierarchy[stop_id]
        return [stop_id]

    def _key(self, origin: str, dest: str, horizon: int) -> str:
        return f"{origin}:{dest}:{horizon}"

    def _timestamped_key(self, origin: str, dest: str, horizon: int, timestamp: int) -> str:
        return f"{origin}:{dest}:{horizon}:{timestamp}"

    def _default_payload(self) -> Dict[str, any]:
        return {
            "active_trips": 0.0,
            "avg_departure_delay_s": 0.0,
            "rolling_mean_7d": 0.0,
            "hist_p50": 0.0,
            "hist_p90": 0.0,
            "h3": "unknown",
        }

    def get_features(self, origin_stop: str, dest_stop: str, horizon_min: int) -> Dict[str, any]:
        # First try direct lookup
        key = self._key(origin_stop, dest_stop, horizon_min)
        now = time.time()
        if key in self._cache and self._cache_expiry.get(key, 0) > now:
            return self._cache[key]

        payload = self.backend.read(key)

        # If not found and this looks like a parent station, try child stops
        if payload is None and (origin_stop.startswith("place-") or dest_stop.startswith("place-")):
            payload = self._get_aggregated_features(origin_stop, dest_stop, horizon_min)

        if payload is None:
            payload = self._default_payload()
        else:
            for k, v in self._default_payload().items():
                payload.setdefault(k, v)

        self._cache[key] = payload
        self._cache_expiry[key] = now + self.ttl_seconds
        return payload

    def _get_aggregated_features(self, origin_stop: str, dest_stop: str, horizon_min: int) -> Optional[Dict[str, any]]:
        """Query all child stop combinations and aggregate."""
        origin_children = self._get_child_stops(origin_stop)
        dest_children = self._get_child_stops(dest_stop)

        # Prioritize platform stops (70xxx, numeric) over doors/nodes
        def priority_sort(stops):
            platforms = [s for s in stops if s.isdigit() or (s.startswith('7') and s[:5].isdigit())]
            others = [s for s in stops if s not in platforms]
            return platforms + others

        origin_children = priority_sort(origin_children)
        dest_children = priority_sort(dest_children)

        all_features = []
        # Limit combinations to avoid too many lookups
        max_combinations = 500
        count = 0
        for o in origin_children:
            if count >= max_combinations:
                break
            for d in dest_children:
                if count >= max_combinations:
                    break
                key = self._key(o, d, horizon_min)
                feat = self.backend.read(key)
                if feat:
                    all_features.append(feat)
                count += 1

        if not all_features:
            return None

        # Aggregate: average numeric fields, take first h3
        aggregated = {}
        numeric_fields = ["active_trips", "avg_departure_delay_s", "rolling_mean_7d", "hist_p50", "hist_p90"]
        for field in numeric_fields:
            values = [f.get(field, 0.0) for f in all_features if f.get(field) is not None]
            if values:
                aggregated[field] = sum(values) / len(values)
            else:
                aggregated[field] = 0.0

        # Take first non-unknown h3
        for f in all_features:
            h3_val = f.get("h3")
            if h3_val and h3_val != "unknown":
                aggregated["h3"] = h3_val
                break
        else:
            aggregated["h3"] = "unknown"

        return aggregated

    def set_features(self, origin_stop: str, dest_stop: str, horizon_min: int, payload: Dict[str, float]) -> None:
        key = self._key(origin_stop, dest_stop, horizon_min)
        self.backend.write(key, payload, self.ttl_seconds)
        self._cache[key] = payload
        self._cache_expiry[key] = time.time() + self.ttl_seconds

    def set_features_timestamped(self, origin_stop: str, dest_stop: str, horizon_min: int, timestamp: int, payload: Dict[str, float]) -> None:
        """Write features with timestamp suffix for TFT historical sequence queries."""
        key = self._timestamped_key(origin_stop, dest_stop, horizon_min, timestamp)
        self.backend.write(key, payload, self.ttl_seconds)

    def get_historical_features(self, origin_stop: str, dest_stop: str, horizon_min: int, lookback: int = 16) -> Optional[list]:
        """
        Query historical feature sequences from Redis timestamped keys.
        Returns list of (timestamp, features) tuples sorted by timestamp (oldest first).
        """
        if not isinstance(self.backend, RedisBackend):
            return None

        pattern = f"{origin_stop}:{dest_stop}:{horizon_min}:*"
        keys = []
        cursor = 0
        # Scan for timestamped keys matching this route/horizon
        while True:
            cursor, batch = self.backend.client.scan(cursor=cursor, match=pattern)
            keys.extend(batch)
            if cursor == 0:
                break

        if not keys:
            return None

        # Extract timestamps and sort
        timestamped_features = []
        for key in keys:
            parts = key.split(":")
            if len(parts) == 4:
                try:
                    timestamp = int(parts[3])
                    payload = self.backend.read(key)
                    if payload:
                        timestamped_features.append((timestamp, payload))
                except (ValueError, IndexError):
                    continue

        if not timestamped_features:
            return None

        # Sort by timestamp and take last N entries
        timestamped_features.sort(key=lambda x: x[0])
        return timestamped_features[-lookback:]

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
