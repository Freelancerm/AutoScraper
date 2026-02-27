import os
import asyncio
import logging
from datetime import datetime, UTC
from pathlib import Path
from typing import Iterable, Mapping, Any
from dataclasses import is_dataclass, asdict

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
        image_url TEXT,
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

    def existing_urls(self, urls: list[str]) -> set[str]:
        """ Check which of the given URLs already exist in the database. """
        if not urls:
            return set()
        query = "SELECT url FROM car_listings WHERE url = ANY(%s)"
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query, (urls,))
            return {row[0] for row in cur.fetchall()}

    def insert_batch(self, listings: Iterable[CarListing]) -> None:
        """ Insert a batch of listings into the database. """

        def serialize(listing: CarListing) -> None | dict[str, Any]:
            """Helper to convert various types to dict."""
            if listing is None: return None
            if isinstance(listing, Mapping): return dict(listing)
            if hasattr(listing, "model_dump"): return listing.model_dump()
            if is_dataclass(listing): return asdict(listing)
            raise TypeError(f"Unsupported listing type: {type(listing)!r}")

        params = [p for p in map(serialize, listings) if p is not None]

        if not params:
            return

        with self._connect() as conn, conn.cursor() as cur:
            cur.executemany(LISTING_INSERT, params)
            conn.commit()

    async def create_dump(self) -> None:
        """Create a backup dump of the database using pg_dump."""
        filename = f"dump_{datetime.now(UTC):%Y%m%d_%H%M%S}.dump"
        output_path = self.DUMPS_DIR / filename
        logging.info(f"Creating backup to DB: {filename}")
        cmd = [
            "pg_dump",
            "--format=custom",
            "--file", str(output_path),
            "--host", DB_HOST,
            "--port", str(DB_PORT),
            "--username", DB_USER,
            DB_NAME,
        ]
        env = os.environ.copy()
        if DB_PASSWORD:
            env["PGPASSWORD"] = DB_PASSWORD

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            raise RuntimeError(
                f"Backup failed with code {proc.returncode}: {stderr.decode(errors='replace')}")
        logging.info(f"Dump created: {output_path}")
