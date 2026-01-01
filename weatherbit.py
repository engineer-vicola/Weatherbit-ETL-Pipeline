import logging
import os
from datetime import datetime, timezone
from urllib.parse import quote_plus

import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

load_dotenv()

API_KEY  = os.getenv("API_KEY")
BASE_URL = os.getenv("url")
DB_USER  = os.getenv("username")
DB_PASS  = os.getenv("password")
DB_NAME  = os.getenv("database")
API_URL = f"{BASE_URL}&key={API_KEY}"

MONGO_URI = (
    f"mongodb://{DB_USER}:{DB_PASS}"
    f"@localhost:27017/{DB_NAME}"
    f"?authSource={DB_NAME}"
)


def convert_utc_iso(timespec):
    utc_now = datetime.now(timezone.utc)
    iso_str = utc_now.isoformat(timespec=timespec)
    return iso_str.replace('+00:00', 'Z')


def timestamp_to_iso_utc(ts):
    ts = int(ts)
    dt = datetime.fromtimestamp(ts, timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def fetch_hourly_data(url):
    logging.info("fetching hourly weather data...")
    response = requests.get(url, timeout=30)
    response.raise_for_status()    
    payload = response.json()

    metadata = {
        "city_name": payload.get("city_name"),
        "country_code": payload.get("country_code"),
        "state_code": payload.get("state_code"),
        "lat": payload.get("lat"),
        "lon": payload.get("lon"),
    }
    
    hourly = payload.get("data", [])
    return [{**item, **metadata} for item in hourly]


def normalize(record):
    dt_iso: str
    dt_str = record.get("timestamp_utc") or record.get("datetime")

    if dt_str:
        dt_iso = dt_str.replace(" ", "T")
        if not dt_iso.endswith("Z"):
            dt_iso += "Z"
    elif record.get("ts") is not None:
        dt_iso = timestamp_to_iso_utc(record["ts"])
    else:
        dt_iso = convert_utc_iso()

    city = record.get("city_name") or "NOT_KNOWN"
    wx   = record.get("weather") or {}

    return {
        "_id": f"{city}|{dt_iso}",
        "city": city,
        "country": record.get("country_code", ""),
        "state_code": record.get("state_code", ""),
        "lat": record.get("lat"),
        "lon": record.get("lon"),
        "dt": dt_iso,

        "temp_c": record.get("temp"),
        "feels_like_c": record.get("app_temp"),
        "rh": record.get("rh"),
        "dewpt_c": record.get("dewpt"),
        "wind_ms": record.get("wind_spd"),
        "wind_gust_ms": record.get("wind_gust_spd"),
        "wind_dir_deg": record.get("wind_dir"),
        "wind_cdir": record.get("wind_cdir"),
        "wind_cdir_full": record.get("wind_cdir_full"),
        "pop_pct": record.get("pop"),
        "precip_mm": record.get("precip"),
        "snow_mm": record.get("snow"),
        "snow_depth_mm": record.get("snow_depth"),
        "clouds_low_pct": record.get("clouds_low"),
        "clouds_mid_pct": record.get("clouds_mid"),
        "clouds_hi_pct": record.get("clouds_hi"),
        "clouds_pct": record.get("clouds"),
        "slp_mb": record.get("slp"),
        "pres_mb": record.get("pres"),
        "vis_km": record.get("vis"),
        "uv_index": record.get("uv"),
        "dhi_wm2": record.get("dhi"),
        "dni_wm2": record.get("dni"),
        "ghi_wm2": record.get("ghi"),
        "solar_rad_wm2": record.get("solar_rad"),
        "ozone_dobson": record.get("ozone"),

        "conditions": wx.get("description") if isinstance(wx, dict) else None,
        "weather_code": wx.get("code") if isinstance(wx, dict) else None,
        "weather_icon": wx.get("icon") if isinstance(wx, dict) else None,
        "pod": record.get("pod"),

        "ingestedAt": convert_utc_iso(),
        "updatedAt": convert_utc_iso(),
    }


def indexes(collection):
    try:
        collection.create_index([("updatedAt", 1)])
        collection.create_index([("city", 1), ("dt", 1)], unique=True)
        logging.info("indexes created successfully")
    except Exception as e:
        logging.warning(f"failed to create indexes: {e}")


def upsert(collection, docs):
    if not docs:
        logging.warning("no documents to upsert")
        return 0

    operations = [
        UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True)
        for d in docs
    ]

    results = collection.bulk_write(operations, ordered=False)
    return (results.upserted_count or 0) + (results.modified_count or 0)


def main():
    try:
        with MongoClient(MONGO_URI) as client:
            db = client.get_default_database()
            collection = db["weather"]

            indexes(collection)

            records = fetch_hourly_data(API_URL)
            docs = [normalize(r) for r in records]
            count = upsert(collection, docs)

            logging.info(f"records_fetched={len(records)}, upserted_or_modified={count}")

    except Exception as e:
        logging.error(f"pipeline failed: {e}", exc_info=True)

    finally:
        logging.info("pipeline run completed")


if __name__ == "__main__":
    main()
