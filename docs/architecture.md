# Architecture

## Data Lake & Streaming

```
MBTA/NOAA Pollers -> Kafka/Redpanda -> Bronze Delta (raw) -> Silver Delta (validated features) -> Gold Delta (fact tables)
                                            \-> DLQ Delta (schema rejects)
```

- **Bronze**: Structured Streaming, schema-enforced JSON ingestion, DLQ + checkpoints.
- **Silver**: Batch Spark joins GTFS static + NOAA weather, computes H3 (res 8) aggregates, stop pair stats, and validates with Great Expectations.
- **Gold**: Fact tables keyed by `(origin_stop, dest_stop, h3, minute, horizon_min)` with rolling 7d stats + quantiles.

## Feature Store

- Offline batch loads Gold via Spark → Arrow → pandas (`features.store.load_training_features`).
- Online store uses Redis by default with DuckDB fallback. Keys include origin/dest/horizon, TTL caching, and snapshot support for the SSE dashboard.
- `features/materialize_online.py` streams Gold updates into the online store.

## Models

- GBT quantile trainer + Optuna tuning (fast CPU path).
- Chronos/Temporal Transformer harness for GPU fine-tuning.
- Registry writes to `models/registry.json` + MLflow, API watches file changes to reload.

## Serving & Dashboard

- FastAPI with `/predict`, `/crowding_map` SSE, `/metrics`. Dependency injection allows mocking the feature store.
- deck.gl + React UI streams SSE and calls `/predict`. Vite build artifacts served from `ui/dist`.

## Ops & Runbooks

- `make bronze|silver|gold|materialize_online` orchestrate Spark flow.
- Delta compaction/vacuum via `spark/delta_utils.py`.
- CI runs lint/mypy/pytest + UI build.
- Runbooks: `docs/architecture.md` covers DLQ replay, feature cache fixation, model rollback.
