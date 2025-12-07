from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import yaml

from models.datasets import DatasetConfig, assemble_features, load_dataset
from models.metrics import coverage
from models.registry import ModelRegistry
from models.trainers.gbt import GBTTrainerConfig, LightGBMTrainer
from models.trainers.transformer import ChronosTrainer, TransformerConfig
from models.trainers.tft_pt import TFTTrainer, TFTTrainerConfig

REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)


def _build_trainer(config: dict):
    trainer_type = config.get("type", "gbt").lower()
    params = config.get("params", {})
    if trainer_type == "gbt":
        trainer_config = GBTTrainerConfig(**params)
        return "gbt", LightGBMTrainer(trainer_config)
    if trainer_type == "transformer":
        trainer_config = TransformerConfig(**params)
        return "transformer", ChronosTrainer(trainer_config)
    if trainer_type == "tft":
        trainer_config = TFTTrainerConfig(**params)
        return "tft", TFTTrainer(trainer_config)
    raise ValueError(f"Unsupported trainer type: {trainer_type}")


def run(config_path: str) -> None:
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    dataset_cfg = DatasetConfig(**config["dataset"])
    df = load_dataset(dataset_cfg)
    trainer_name, trainer = _build_trainer(config["trainer"])

    if trainer_name == "gbt":
        features, labels = assemble_features(df, dataset_cfg)
        result = trainer.train(features, labels)
        metrics = result["metrics"]
        artifacts = result["artifacts"]
        metrics["coverage_baseline"] = coverage(labels, labels - 1, labels + 1)
    else:
        result = trainer.train(df)
        artifacts = result["artifacts"]
        metrics = result["metrics"]

    registry = ModelRegistry()
    entry = registry.register(
        model_name=trainer_name,
        artifacts=artifacts,
        params=config,
        metrics=metrics,
    )

    report_path = REPORTS_DIR / f"{trainer_name}-{entry.version}.md"
    with report_path.open("w", encoding="utf-8") as report:
        report.write(f"# Training report ({trainer_name})\n\n")
        report.write(f"- Version: {entry.version}\n")
        report.write(f"- Created: {entry.created_at}\n")
        report.write("## Metrics\n")
        report.write(json.dumps(metrics, indent=2))
        report.write("\n## Artifacts\n")
        report.write(json.dumps(artifacts, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Train CityStream models via config")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
