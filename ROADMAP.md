I'll add a concise, actionable checklist of remaining work by week to ensure we hit the Dec 11 deadline, mapped to your current repo (Delta-backed Bronze/Silver/Gold, Makefile targets, UI/API). Then I’ll share the steps per week with commands.

[1 tool called]

Here’s the shortest path to done by Dec 11, mapped to your repo.

Week 2 (now) — Silver v1 complete
- Finish Week 1 closeout:
  - Run GTFS static: python gtfs/load_static.py; confirm data/gtfs/*.parquet.
  - Verify 24h Bronze for vehicles/trips/weather; add a 1-page DQ snapshot in README.
- Generate Silver (batch over Bronze Delta):
  - Run Silver job: use SilverTransformJob with h3_resolution=8; verify data/silver exists.
  - Ensure features per minute by (origin_stop, dest_stop, h3); validate Great Expectations passes.
  - Backfill 24h for 2–3 routes; record hit-rate (non-null h3) and join coverage.
- Optional polish:
  - If available, include route_id in outputs; keep partition by date.

Week 3 — Gold labels, baseline model, API stub
- Create labels/features:
  - Run GoldAggregationJob with horizons (10,20,30) to write data/gold (Delta).
- Train + backtest:
  - make train (or script) on Gold; compute MAE (P50), pinball (P90), coverage; save artifacts + meta.
- API stub:
  - make serve; implement /predict to fetch latest Silver/Gold features and return quantiles; smoke test with sample corridor.
- Metrics:
  - Add basic panels for consumer lag, watermark delay, API P95 (Grafana/Console or logs).

Week 4 — Online refresh, drift, serving hardening, Gold predictions
- Online refresh:
  - Mini-batch retrain every 10–30 min using most recent window; store latest model artifact/version.
- Drift + fallback:
  - Add KS/ADWIN on residuals; on high drift -> EWMA fallback.
- Residuals (stub):
  - ANN stub over recent windows (Faiss optional); blend residual correction.
- Serving:
  - Run uvicorn/gunicorn with >1 workers; add request IDs, timing logs, and simple in-memory cache for hot corridors.
- Gold predictions:
  - Daily backfill of predictions + targets; partition by date,horizon_min.

Week 5 — UI v1, SLOs, hardening
- UI:
  - deck.gl map (speed heatmap per h3; corridor band with P50/P90); wire to API.
- SLOs:
  - Dashboards for API P95 latency and dashboard freshness; alert thresholds.
- Data hygiene:
  - Compact/OPTIMIZE Delta tables, fix small-file issues; validate schema evolution.
- CI/CD + docs:
  - Add CI (lint/tests/build); write ops runbook + README quickstart.

Week 6 (Dec 9–11) — Perf, freeze, demo
- Load test:
  - Target QPS for API; verify P95 < 150 ms; adjust workers/cache.
- Freeze:
  - Snapshot Bronze/Silver/Gold, lock model artifacts, export config.
- Drill + demo:
  - Failure drill (pause pollers, show resilience); finalize slides; record short demo video.

Concrete commands you can run now
- Bronze (per topic):
  - TOPIC=gtfs.vehicle_positions make bronze
  - TOPIC=gtfs.trip_updates make bronze
  - TOPIC=weather.hourly make bronze
- Silver (Python):
  - from spark.utils import build_spark_session; from spark.jobs import SilverTransformJob; sp=build_spark_session("silver"); SilverTransformJob(sp, h3_resolution=8).run()
- Gold:
  - from spark.utils import build_spark_session; from spark.jobs import GoldAggregationJob; sp=build_spark_session("gold"); GoldAggregationJob(sp, horizons=(10,20,30)).run()
- Train/Serve/UI:
  - make train
  - make serve
  - make ui-build

Scope guardrails
- Focus on 2–3 routes end-to-end for Silver/Gold/Serve/UI; expand after Week 5 if time.
- ANN residuals can be a stub; prioritize latency and freshness SLOs.
- Keep replay over Bronze for iteration; avoid long live runs when not needed.

Status: I added a consolidated checklist for Weeks 2–6 and created actionable todos for Gold/model/API, online refresh/drift, UI/SLOs, and final demo to keep us on track by Dec 11.