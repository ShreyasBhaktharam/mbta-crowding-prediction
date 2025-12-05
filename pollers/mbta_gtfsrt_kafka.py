import os
import time
import json
import threading
from typing import Optional

import requests
from confluent_kafka import Producer
from google.transit import gtfs_realtime_pb2

def on_delivery(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def create_producer() -> Producer:
    broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    return Producer({"bootstrap.servers": broker, "on_delivery": on_delivery})


def poll_vehicle_positions(p: Producer, api_key: str, interval_s: int = 5) -> None:
    url = f"https://cdn.mbta.com/realtime/VehiclePositions.pb?api_key={api_key}"
    topic = "gtfs.vehicle_positions"
    while True:
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(r.content)
            count = 0
            for e in feed.entity:
                vp = getattr(e, "vehicle", None)
                if not vp or not getattr(vp, "position", None):
                    continue
                msg = {
                    "vehicle_id": getattr(vp.vehicle, "id", None) or "",
                    "trip_id": getattr(vp.trip, "trip_id", None) or "",
                    "route_id": getattr(vp.trip, "route_id", None) or "",
                    "lat": vp.position.latitude,
                    "lon": vp.position.longitude,
                    "speed_mps": getattr(vp.position, "speed", None),
                    "timestamp": int(getattr(vp, "timestamp", 0) or time.time()),
                }
                p.produce(topic, json.dumps(msg).encode("utf-8"), callback=on_delivery)
                p.poll(0)
                count += 1
            p.flush(5)
            print(f"[vehicle_positions] produced {count} records")
        except Exception as exc:
            print(f"[vehicle_positions] error: {exc}")
        time.sleep(interval_s)


def poll_trip_updates(p: Producer, api_key: str, interval_s: int = 5) -> None:
    url = f"https://cdn.mbta.com/realtime/TripUpdates.pb?api_key={api_key}"
    topic = "gtfs.trip_updates"
    while True:
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(r.content)
            count = 0
            for e in feed.entity:
                tu = getattr(e, "trip_update", None)
                if not tu:
                    continue
                ts = int(getattr(tu, "timestamp", 0) or time.time())
                for stu in getattr(tu, "stop_time_update", []):
                    msg = {
                        "trip_id": getattr(tu.trip, "trip_id", None) or "",
                        "stop_id": getattr(stu, "stop_id", None) or "",
                        "arrival_delay_s": getattr(getattr(stu, "arrival", None), "delay", 0) or 0,
                        "departure_delay_s": getattr(getattr(stu, "departure", None), "delay", 0) or 0,
                        "timestamp": ts,
                    }
                    p.produce(topic, json.dumps(msg).encode("utf-8"), callback=on_delivery)
                    p.poll(0)
                    count += 1
            p.flush(5)
            print(f"[trip_updates] produced {count} records")
        except Exception as exc:
            print(f"[trip_updates] error: {exc}")
        time.sleep(interval_s)


def main() -> None:
    api_key = os.environ.get("MBTA_KEY")
    if not api_key:
        raise RuntimeError("MBTA_KEY not set in environment")
    producer = create_producer()

    t1 = threading.Thread(target=poll_vehicle_positions, args=(producer, api_key), daemon=True)
    t2 = threading.Thread(target=poll_trip_updates, args=(producer, api_key), daemon=True)
    t1.start()
    t2.start()
    print("MBTA GTFS-RT pollers running. Ctrl+C to exit.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Exiting...")


if __name__ == "__main__":
    main()

