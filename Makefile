SHELL := /bin/bash

.PHONY: up down topics bronze-vp bronze-tu silver

up:
	docker compose up -d

down:
	docker compose down -v

topics:
	@echo "Creating topics..."
	docker exec -it $$(docker ps -qf name=citystream-redpanda-1) rpk topic create \
		gtfs.vehicle_positions gtfs.trip_updates weather.hourly events.city || true

bronze-vp:
	PYSPARK_PYTHON=$$(which python) spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		spark/bronze_to_silver.py --mode bronze --topic gtfs.vehicle_positions

bronze-tu:
	PYSPARK_PYTHON=$$(which python) spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		spark/bronze_to_silver.py --mode bronze --topic gtfs.trip_updates

silver:
	PYSPARK_PYTHON=$$(which python) spark-submit \
		spark/bronze_to_silver.py --mode silver

