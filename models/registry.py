from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import mlflow

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY_PATH = PROJECT_ROOT / "models" / "registry.json"
DEFAULT_TRACKING = PROJECT_ROOT / "mlruns"


@dataclass
class RegistryEntry:
    model_name: str
    version: str
    artifacts: Dict[str, str]
    params: Dict[str, Any]
    metrics: Dict[str, float]
    created_at: str
    run_id: Optional[str] = None


class ModelRegistry:
    def __init__(self, path: Path = DEFAULT_REGISTRY_PATH, tracking_uri: Optional[str] = None) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.tracking_uri = tracking_uri or f"file:{DEFAULT_TRACKING}"
        mlflow.set_tracking_uri(self.tracking_uri)

    def _load(self) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []
        with self.path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _write(self, entries: List[Dict[str, Any]]) -> None:
        with self.path.open("w", encoding="utf-8") as f:
            json.dump(entries, f, indent=2)

    def register(self, model_name: str, artifacts: Dict[str, str], params: Dict[str, Any], metrics: Dict[str, float]) -> RegistryEntry:
        version = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        entry = RegistryEntry(
            model_name=model_name,
            version=version,
            artifacts=artifacts,
            params=params,
            metrics=metrics,
            created_at=datetime.utcnow().isoformat(),
        )
        with mlflow.start_run(run_name=f"{model_name}-{version}") as run:
            entry.run_id = run.info.run_id
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)
            for label, artifact_path in artifacts.items():
                if os.path.exists(artifact_path):
                    mlflow.log_artifact(artifact_path, artifact_path=label)
        entries = self._load()
        entries.append(asdict(entry))
        self._write(entries)
        return entry

    def latest(self, model_name: Optional[str] = None) -> Optional[RegistryEntry]:
        entries = self._load()
        if model_name:
            entries = [e for e in entries if e["model_name"] == model_name]
        if not entries:
            return None
        latest = sorted(entries, key=lambda e: e["created_at"], reverse=True)[0]
        return RegistryEntry(**latest)

