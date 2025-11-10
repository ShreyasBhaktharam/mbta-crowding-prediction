# CityStream (MBTA)

Goal: Real-time pipeline for MBTA corridor forecasts with Kafka ingest, Spark streaming (Bronze→Silver), baseline quantile model, FastAPI serving, and a minimal deck.gl UI.

## Quickstart

1) Prereqs
- Docker Desktop
- Python 3.10+
- Java 11+ (for Spark)

2) Environment
- Set `MBTA_KEY` and `KAFKA_BROKER`.

3) Start Kafka (Redpanda) locally
```bash
make up
```
- Console UI: http://localhost:8080
- Kafka broker: localhost:9092

4) Create topics
```bash
make topics
```

5) Install Python deps
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

6) Run pollers (MBTA + NOAA)
```bash
# terminal A
python pollers/mbta_gtfsrt_kafka.py
# terminal B
python pollers/noaa_hourly_kafka.py
```

7) Run Spark Bronze writer (Kafka → Parquet)
```bash
# writes to data/bronze/<topic>/
python spark/bronze_to_silver.py --mode bronze
# Or using spark-submit (Python):
PYSPARK_PYTHON=$(which python) spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark/bronze_to_silver.py --mode bronze --topic gtfs.vehicle_positions
```

8) Generate Silver features (H3 + basic joins)
```bash
python spark/bronze_to_silver.py --mode silver
# Or:
PYSPARK_PYTHON=$(which python) spark-submit spark/bronze_to_silver.py --mode silver
```

9) Train baseline model
```bash
python models/train_baseline.py --data data/silver --out models/
```

10) Serve API
```bash
uvicorn serve.app:app --reload --port 8000
```
- Try: `curl 'http://localhost:8000/predict?origin_stop=place-dwnxg&dest_stop=place-pktrm&horizon_min=10'`

11) UI (static)
- Open `ui/index.html` (set `MAPBOX_TOKEN` if using Mapbox basemap).

## Topics (suggested)
- gtfs.vehicle_positions
- gtfs.trip_updates
- weather.hourly
- events.city (optional)

## Paths
- data/bronze/<topic>/date=YYYY-MM-DD/*.parquet
- data/silver/date=YYYY-MM-DD/*.parquet
- models/*.txt (LightGBM) and models/meta.json

## Notes
- This scaffold favors fast iteration locally. Switch to Delta Lake and a registry later.
- H3 res defaults to 8; adjust with `--h3_res`.
- The Spark job is pure Python (PySpark). It auto-downloads the Kafka connector via `spark.jars.packages` and works with `spark-submit`.

