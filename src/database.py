import os
import subprocess
import logging
from datetime import datetime, UTC
from pathlib import Path
from typing import Iterable

import psycopg

from .config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from .models import CarListing


CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS car_listings (
        url TEXT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        price_usd INTEGER,
        odometer INTEGER,
        username TEXT NOT NULL,
        phone_number BIGINT,
        image_url TEXT NOT NULL,
        images_count INTEGER NOT NULL,
        car_number TEXT,
        car_vin TEXT,
        datetime_found TIMESTAMPTZ NOT NULL
        );
        """
LISTING_INSERT = """
          INSERT INTO car_listings
            (url, title, price_usd, odometer, username, phone_number,
             image_url, images_count, car_number, car_vin, datetime_found)
          VALUES
            (%(url)s, %(title)s, %(price_usd)s, %(odometer)s, %(username)s, %(phone_number)s,
             %(image_url)s, %(images_count)s, %(car_number)s, %(car_vin)s, %(datetime_found)s)
          ON CONFLICT (url) DO NOTHING;
          """


class DB:
    DUMPS_DIR = Path("dumps")

    @staticmethod
    def _connect() -> psycopg.Connection:
        return psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )

    def init_db(self) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(CREATE_TABLE)
            conn.commit()

    def is_sampled(self, url: str) -> bool:
        """Check if a listing with the given URL already exists in the database."""
        query = "SELECT 1 FROM car_listings WHERE url = %s LIMIT 1"
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(query, (url,))
                return cur.fetchone() is not None
        except Exception as ex:
            logging.error(f"Database check error: {ex}")
            return False

    def insert_batch(self, listings: Iterable[CarListing]) -> None:
        """ Insert a batch of listings into the database. Accepts both CarListing instances and dicts."""
        params = []
        for listing in listings:
            if hasattr(listing, "model_dump"):
                params.append(listing.model_dump())
            else:
                params.append(dict(listing))
        if not params:
            return
        with self._connect() as conn, conn.cursor() as cur:
            cur.executemany(LISTING_INSERT, params)
            conn.commit()

    def create_dump(self) -> None:
        filename = f"dump_{datetime.now(UTC):%Y%m%d_%H%M%S}.dump"
        output_path = self.DUMPS_DIR / filename
        logging.info(f"📦 Creating backup to DB: {filename}")
        cmd = [
            "pg_dump",
            "--format=custom",
            "--file",
            str(output_path),
            "--host",
            DB_HOST,
            "--port",
            str(DB_PORT),
            "--username",
            DB_USER,
            DB_NAME,
        ]
        env = os.environ.copy()
        if DB_PASSWORD:
            env["PGPASSWORD"] = DB_PASSWORD
        subprocess.run(cmd, check=True, env=env)

