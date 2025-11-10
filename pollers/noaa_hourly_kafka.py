import os
import time
import json
from typing import Optional

import requests
from confluent_kafka import Producer


def create_producer() -> Producer:
    broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
    return Producer({"bootstrap.servers": broker})


def discover_hourly_endpoint(lat: float, lon: float) -> str:
    headers = {"User-Agent": os.environ.get("NOAA_USER_AGENT", "you@example.com")}
    url = f"https://api.weather.gov/points/{lat},{lon}"
    r = requests.get(url, headers=headers, timeout=10)
    r.raise_for_status()
    props = r.json().get("properties", {})
    hourly = props.get("forecastHourly")
    if not hourly:
        raise RuntimeError("No hourly endpoint discovered from NWS")
    return hourly


def fetch_hourly(hourly_url: str, etag: Optional[str] = None, last_modified: Optional[str] = None):
    headers = {"User-Agent": os.environ.get("NOAA_USER_AGENT", "you@example.com")}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    r = requests.get(hourly_url, headers=headers, timeout=10)
    if r.status_code == 304:
        return None, etag, last_modified
    r.raise_for_status()
    new_etag = r.headers.get("ETag")
    new_lm = r.headers.get("Last-Modified")
    return r.json(), new_etag, new_lm


def run_poll(lat: float, lon: float, interval_s: int = 600) -> None:
    topic = "weather.hourly"
    p = create_producer()
    hourly_url = discover_hourly_endpoint(lat, lon)
    etag = None
    last_modified = None
    print(f"Polling NWS hourly: {hourly_url}")
    while True:
        try:
            data, etag, last_modified = fetch_hourly(hourly_url, etag, last_modified)
            if data:
                periods = data.get("properties", {}).get("periods", [])
                count = 0
                for pd in periods[:12]:  # send near horizon
                    msg = {
                        "station_id": data.get("properties", {}).get("units", "NWS"),
                        "temp_c": None,
                        "wind_mps": None,
                        "precip_mm": None,
                        "condition": pd.get("shortForecast"),
                        "timestamp": int(time.time()),
                        "lat": lat,
                        "lon": lon,
                    }
                    # Best-effort conversions if present
                    if pd.get("temperature") is not None:
                        if (pd.get("temperatureUnit") or "C").upper() == "F":
                            msg["temp_c"] = (float(pd["temperature"]) - 32.0) * 5.0 / 9.0
                        else:
                            msg["temp_c"] = float(pd["temperature"])  # assume C
                    if pd.get("windSpeed"):
                        try:
                            # "5 mph" or "5 to 10 mph"
                            part = pd["windSpeed"].split(" ")[0]
                            mph = float(part)
                            msg["wind_mps"] = mph * 0.44704
                        except Exception:
                            pass
                    p.produce(topic, json.dumps(msg).encode("utf-8"))
                    count += 1
                p.flush(5)
                print(f"[weather.hourly] produced {count} records")
        except Exception as exc:
            print(f"[weather.hourly] error: {exc}")
        time.sleep(interval_s)


if __name__ == "__main__":
    lat = float(os.environ.get("NOAA_POINT_LAT", "42.3601"))
    lon = float(os.environ.get("NOAA_POINT_LON", "-71.0589"))
    run_poll(lat, lon)

