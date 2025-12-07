# CityStream MBTA Crowding Prediction

CityStream ingests MBTA GTFS-RT + NOAA weather streams, curates Bronze → Silver → Gold Delta tables, trains quantile models, and serves multi-horizon crowding predictions + a deck.gl dashboard.

[Architecture overview](docs/architecture.md)

## Architecture Summary

- **Data lake**: Redpanda ➜ Spark Structured Streaming ➜ Delta Lake tiers under `data/` with DLQ tables and Delta compaction utilities.
- **Feature store**: Gold Delta for offline training, Redis/DuckDB for online serving with TTL caching and streaming materialization.
- **Models**: LightGBM quantile trainer with Optuna tuning + optional Chronos/TFT transformer path, registered via MLflow + `models/registry.json`.
- **Serving**: FastAPI w/ `/predict`, `/crowding_map` SSE, `/metrics`, plus Prometheus + Loguru logging.
- **UI**: Vite/React/deck.gl consuming the SSE stream and REST predictions.

See `docs/architecture.md` for diagrams, scaling tips, and runbooks.

## Environment & Bootstrap

```bash
cp .env.example .env             # fill MBTA_KEY, MAPBOX_TOKEN, etc.
make bootstrap                   # create venv + pip install
make topics                      # create Kafka topics in Redpanda
python gtfs/load_static.py       # fetch GTFS parquet for Silver joins
```

## Make Targets

| Target | Description |
| --- | --- |
| `make bootstrap` | create virtualenv + install Python deps |
| `make topics` | create Kafka topics in running Redpanda cluster |
| `make pollers` | run MBTA + NOAA pollers locally |
| `make bronze` | run Spark bronze streaming writer (Structured Streaming) |
| `make silver` | run Silver batch enrichment job |
| `make gold` | build Gold fact tables (rolling stats + labels) |
| `make materialize_online` | stream Gold updates into Redis/DuckDB feature store |
| `make validate_data` | run Great Expectations validations on Silver/Gold |
| `make train` | run `python -m models.train --config configs/gbt.yaml` |
| `make serve` | start FastAPI app (uvicorn) |
| `make ui-build` | `npm install && npm run build` inside `ui/` |
| `make test` | pytest (Spark + API + feature store + e2e) |
| `make lint` | ruff/black (configured in `pyproject.toml`) |
| `make mypy` | static type checks |

## Pipeline Walkthrough

1. **Ingestion (Bronze)**
   - `python spark/bronze_to_silver.py --mode bronze --topic gtfs.vehicle_positions`
   - Writes Delta tables in `data/bronze/<topic>/date=.../hour=...`. Invalid JSON is captured in `data/dlq/<topic>`.
2. **Silver**
   - `python spark/bronze_to_silver.py --mode silver` joins GTFS static parquet (`data/gtfs/`), NOAA weather, computes H3 res 8, and validates with Great Expectations.
3. **Gold**
   - `python spark/bronze_to_silver.py --mode gold --horizons 10 20 30` builds fact tables keyed by `(origin_stop, dest_stop, h3, minute, horizon_min)`.
4. **Online Materialization**
   - `python -m features.materialize_online` runs Structured Streaming on Gold Delta and pushes aggregates into Redis/DuckDB.
5. **Training**
   - `python -m models.train --config configs/gbt.yaml` (or `configs/transformer.yaml`). Generates reports in `reports/` and logs to MLflow.
6. **Serving & Dashboard**
   - `make serve` ➜ FastAPI on :8000, SSE at `/crowding_map`.
   - `make ui-build` ➜ Vite output in `ui/dist`. FastAPI auto-serves static files when `ui/dist` exists.

## Feature Store Usage

```python
from features import load_training_features, get_features

batch_df = load_training_features(horizon_min=10, start="2024-01-01", end="2024-01-07")
row = get_features("place-dwnxg", "place-pktrm", 10)
```

Configure backend via `FEATURE_STORE_BACKEND=redis|duckdb`. Redis credentials controlled through `.env` values.

## Model Training Configs

- `configs/gbt.yaml`: LightGBM quantile trainer (Optuna tuned, multi-quantile). Runs entirely on CPU.
- `configs/transformer.yaml`: Chronos/TFT fine-tuning (optional GPU). Docstrings outline GPU sizing + batch size tuning.

Switch configs via `python -m models.train --config configs/<name>.yaml`. Registry metadata + metrics stored in `models/registry.json` and MLflow (`mlruns/`).

## Serving API

- `/predict` (POST) – batch prediction for `{origin_stop, dest_stop, horizon_min}` requests, returns p50/p90 (and p10/p95 for transformer models).
- `/crowding_map` (SSE) – stream aggregated predictions for deck.gl.
- `/metrics` – Prometheus metrics (requests, latency, cache hits, model version).
- `/healthz` – readiness check (model available?).

Dependency injection allows `tests/test_api.py` to stub the feature store + model service.

## Dashboard

Inside `ui/`:

```bash
npm install
npm run dev   # Vite dev server on 5173
npm run build # outputs ui/dist for FastAPI static hosting
```

Set `VITE_API_BASE` + `VITE_MAPBOX_TOKEN` (Mapbox or MapLibre) in `.env` or `ui/.env.local`.

## CI/CD & Containers

- `docker-compose.yml` spins up Redpanda + Console, Spark master/worker, Redis, API, and UI container for full-stack local runs.
- `.github/workflows/ci.yml` runs lint → mypy → pytest (Spark included) → `npm run build`.

## Runbooks / Troubleshooting

- **Data quality failure**: `make validate_data`, inspect `data/dlq/<topic>` for schema rejects, replay via `tools/generate_sample_data.py` or Kafka console.
- **Missing features**: ensure Redis reachable, or set `FEATURE_STORE_BACKEND=duckdb` and rerun `make materialize_online`.
- **Model rollback**: copy desired entry to top of `models/registry.json`, ensure artifacts exist, restart `make serve` (reload happens automatically on file change).
- **GPU training**: set `trainer.params.use_gpu=true` in `configs/transformer.yaml`, provision CUDA-capable host, adjust `batch_size` by VRAM (16GB ≈ 128 sequences, 24GB ≈ 256 sequences).

For more details see `docs/architecture.md`.
