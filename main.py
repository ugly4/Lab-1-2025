import json
import datetime
from typing import Dict, List
import httpx
from prefect import flow, task
from minio import Minio
from clickhouse_connect import get_client
from config import CITIES, MINIO_CONFIG, CLICKHOUSE_CONFIG, TELEGRAM_CONFIG
import io
from datetime import datetime as dt


def get_clickhouse_client():
    return get_client(
        host=CLICKHOUSE_CONFIG["host"],
        port=CLICKHOUSE_CONFIG["port"],
        username=CLICKHOUSE_CONFIG["user"],
        password=CLICKHOUSE_CONFIG["password"],
    )

@task(retries=2, retry_delay_seconds=5)
def fetch_weather(city: str, lat: float, lon: float) -> dict:
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&hourly=temperature_2m,precipitation,windspeed_10m,winddirection_10m"
        f"&timezone=Europe/Moscow&start_date={tomorrow}&end_date={tomorrow}"
    )
    resp = httpx.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

@task
def save_raw_to_minio(city: str, data: dict):
    minio_client = Minio(
        MINIO_CONFIG["endpoint"],
        access_key=MINIO_CONFIG["access_key"],
        secret_key=MINIO_CONFIG["secret_key"],
        secure=False,
    )

    bucket = MINIO_CONFIG["bucket"]

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    json_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
    data_stream = io.BytesIO(json_bytes)

    filename = f"weather_{city}_{datetime.date.today()}.json"
    minio_client.put_object(
        bucket,
        filename,
        data=data_stream,
        length=len(json_bytes),
        content_type="application/json",
    )

    return filename

@task
def transform_hourly(data: dict, city: str) -> List[dict]:
    hourly = data["hourly"]

    return [
        {
            "city": city,
            "timestamp": dt.fromisoformat(hourly["time"][i]),
            "temperature": hourly["temperature_2m"][i],
            "precipitation": hourly["precipitation"][i],
            "windspeed": hourly["windspeed_10m"][i],
            "winddirection": hourly["winddirection_10m"][i],
        }
        for i in range(len(hourly["time"]))
    ]

@task
def transform_daily(data: dict, city: str) -> dict:
    hourly = data["hourly"]
    temps = [t for t in hourly["temperature_2m"] if t is not None]
    precip = sum(p for p in hourly["precipitation"] if p is not None)
    windspeeds = [w for w in hourly["windspeed_10m"] if w is not None]

    return {
        "city": city,
        "date": dt.fromisoformat(hourly["time"][0].split("T")[0]),
        "temp_min": min(temps),
        "temp_max": max(temps),
        "temp_avg": sum(temps) / len(temps),
        "total_precipitation": precip,
        "max_windspeed": max(windspeeds),
    }

@task
def create_clickhouse_tables():
    client = get_clickhouse_client()
    with open("create-db.sql", "r", encoding="utf-8") as f:
        sql = f.read()

    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt and not stmt.startswith("--"):
            client.command(stmt)

@task
def load_hourly_to_clickhouse(records: List[dict]):
    client = get_clickhouse_client()

    columns = list(records[0].keys())

    data_as_lists = [
        [r[col] for col in columns]
        for r in records
    ]

    client.insert(f"{CLICKHOUSE_CONFIG["database"]}.weather_hourly", data_as_lists, column_names=columns)

@task
def load_daily_to_clickhouse(record: dict):
    client = get_clickhouse_client()

    columns = list(record.keys())

    client.insert(f"{CLICKHOUSE_CONFIG["database"]}.weather_daily", [[record[col] for col in columns]], column_names=columns)

@task
async def send_telegram(daily: dict):
    bot_token = TELEGRAM_CONFIG["bot_token"]
    chat_id = TELEGRAM_CONFIG["chat_id"]

    message = (
        f"🌤 Прогноз на {daily['date']} в {daily['city']}:\n"
        f"🌡️ {daily['temp_min']:.1f}°C ... {daily['temp_max']:.1f}°C (среднее {daily['temp_avg']:.1f}°C)\n"
        f"🌧️ Осадки: {daily['total_precipitation']:.1f} мм\n"
    )
    if daily["total_precipitation"] > 10:
        message += "⚠️ Ожидается сильный дождь!\n"
    if daily["max_windspeed"] > 15:
        message += "💨 Ожидается сильный ветер!\n"

    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": message}
        )

@flow(name="Погода: ETL на завтра")
async def weather_etl():
    # Автоматически создаём таблицы при первом запуске
    create_clickhouse_tables()

    for city, (lat, lon) in CITIES.items():
        # Extract
        raw = fetch_weather(city, lat, lon)
        save_raw_to_minio(city, raw)

        # Transform
        hourly = transform_hourly(raw, city)
        daily = transform_daily(raw, city)

        print(f"Hourly: {hourly}")
        print(f"Daily: {daily}")

        # Load
        load_hourly_to_clickhouse(hourly)
        load_daily_to_clickhouse(daily)

        # Notify
        await send_telegram(daily)

    return "✅ ETL завершён"

if __name__ == "__main__":
    import asyncio
    asyncio.run(weather_etl())