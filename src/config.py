import os

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

SCRAPE_START_URL = os.getenv("SCRAPE_START_URL", "https://auto.ria.com/uk/car/used/")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "5"))
MAX_PAGES_TO_SCRAPE = 0
