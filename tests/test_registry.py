from pathlib import Path

from models.registry import ModelRegistry


def test_registry_registers_and_reads(tmp_path):
    registry_path = tmp_path / "registry.json"
    tracking_uri = f"file:{tmp_path / 'mlruns'}"
    registry = ModelRegistry(path=registry_path, tracking_uri=tracking_uri)
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()
    artifact_file = artifact_dir / "model.txt"
    artifact_file.write_text("stub", encoding="utf-8")

    entry = registry.register(
        "gbt",
        artifacts={"p50": str(artifact_file)},
        params={"dataset": {"horizon_min": 10}},
        metrics={"mae": 1.0},
    )

    assert entry.model_name == "gbt"
    latest = registry.latest("gbt")
    assert latest.version == entry.version
