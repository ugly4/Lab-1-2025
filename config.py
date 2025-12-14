import os
from dotenv import load_dotenv

load_dotenv()

CITIES = {
    "Москва": (55.7522, 37.6156),
    "Самара": (53.1959, 50.1001),
}

MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT"),
    "access_key": os.getenv("MINIO_ACCESS_KEY"),
    "secret_key": os.getenv("MINIO_SECRET_KEY"),
    "bucket": os.getenv("MINIO_BUCKET"),
}

CLICKHOUSE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST"),
    "port": int(os.getenv("CLICKHOUSE_PORT", 8123)),
    "user": os.getenv("CLICKHOUSE_USER", "default"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
    "database": os.getenv("CLICKHOUSE_DATABASE", "weather"),
}

TELEGRAM_CONFIG = {
    "bot_token": os.getenv("TELEGRAM_BOT_TOKEN"),
    "chat_id": os.getenv("TELEGRAM_CHAT_ID"),
}