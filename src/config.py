import os

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

SCRAPE_START_URL = os.getenv("SCRAPE_START_URL", "https://auto.ria.com/uk/car/used/")
PHONE_POPUP_URL = "https://auto.ria.com/bff/final-page/public/auto/popUp/"
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "10"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
RETRIES = int(os.getenv("RETRIES", "3"))
JITTER_MIN = float(os.getenv("JITTER_MIN", "0.1"))
JITTER_MAX = float(os.getenv("JITTER_MAX", "0.3"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF", "0.5"))
ENABLE_PHONE_FETCH = os.getenv("ENABLE_PHONE_FETCH", "false").lower() in {"1", "true", "yes"}
