SHELL := /bin/bash
PYTHON ?= python3
VENV ?= .venv
SPARK_SUBMIT ?= PYSPARK_PYTHON=$$(command -v python3 || command -v python) spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

.PHONY: bootstrap topics pollers bronze silver gold materialize_online train serve ui-build validate_data test lint mypy delta-optimize delta-vacuum

bootstrap:
	$(PYTHON) -m venv $(VENV)
	source $(VENV)/bin/activate && pip install -U pip && pip install -r requirements.txt

topics:
	docker compose exec redpanda rpk topic create \
		gtfs.vehicle_positions \
		gtfs.trip_updates \
		weather.hourly || true

pollers:
	$(PYTHON) pollers/mbta_gtfsrt_kafka.py & \
	$(PYTHON) pollers/noaa_hourly_kafka.py & \
	wait

bronze:
	$(SPARK_SUBMIT) spark/bronze_to_silver.py --mode bronze --topic $${TOPIC:-gtfs.vehicle_positions}

silver:
	$(SPARK_SUBMIT) spark/bronze_to_silver.py --mode silver

gold:
	$(SPARK_SUBMIT) spark/bronze_to_silver.py --mode gold --horizons 10 20 30

materialize_online:
	$(PYTHON) -m features.materialize_online

train:
	$(PYTHON) -m models.train --config $${CONFIG:-configs/gbt.yaml}

serve:
	uvicorn serve.app:app --host 0.0.0.0 --port 8000 --reload

ui-build:
	cd ui && npm install --legacy-peer-deps && npm run build && cp -r dist/* ../serve/static/

validate_data:
	$(PYTHON) spark/bronze_to_silver.py --mode validate

test:
	pytest

lint:
	ruff check .
	black --check .

mypy:
	mypy features models serve spark

