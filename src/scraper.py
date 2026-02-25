import asyncio
import json
import logging
import random
import re
from typing import Any, AsyncIterator
from urllib.parse import urlsplit, urlunsplit, urlencode, parse_qs

import aiohttp
from pydantic import ValidationError

from .config import (
    SCRAPE_START_URL,
    MAX_PAGES,
    CONCURRENCY,
    REQUEST_TIMEOUT,
    BATCH_SIZE,
    RETRIES,
    JITTER_MIN,
    JITTER_MAX,
    RETRY_BACKOFF,
    ENABLE_PHONE_FETCH,
    PHONE_POPUP_URL,
)
from .models import CarListing
from .parser import parse_listing


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


LISTING_URL_RE = re.compile(r"https?://auto\.ria\.com/uk/auto_[^\"'#?]+?\.html")
LISTING_URL_REL_RE = re.compile(r"/uk/auto_[^\"'#?]+?\.html")
USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_0) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36",
]
logger = logging.getLogger(__name__)


class Scraper:
    """Async AutoRia scraper with page->queue->workers->batch insert pipeline."""

    def __init__(
        self,
        start_url: str = SCRAPE_START_URL,
        concurrency: int = CONCURRENCY,
        timeout: int = REQUEST_TIMEOUT,
        max_pages: int = MAX_PAGES,
        batch_size: int = BATCH_SIZE,
    ) -> None:
        """Initialize scraper settings and limits."""
        self.start_url = start_url
        self.concurrency = max(concurrency, 1)
        self.timeout = max(timeout, 5)
        self.max_pages = max_pages
        self.batch_size = max(batch_size, 1)

    @staticmethod
    def _build_page_url(base_url: str, page: int) -> str:
        """Return a paginated URL for search results."""
        if page <= 1:
            return base_url
        parts = urlsplit(base_url)
        query = parse_qs(parts.query)
        query["page"] = [str(page)]
        return urlunsplit(
            (
                parts.scheme or "https",
                parts.netloc or "auto.ria.com",
                parts.path,
                urlencode(query, doseq=True),
                parts.fragment,
            )
        )

    async def _fetch_text(self, session: aiohttp.ClientSession, url: str) -> str:
        """Fetch page HTML with retries, jitter, and basic backoff."""
        retries = max(RETRIES, 1)
        for attempt in range(retries):
            await asyncio.sleep(random.uniform(JITTER_MIN, JITTER_MAX))
            try:
                async with session.get(url, headers=self._random_headers()) as resp:
                    if resp.status == 200:
                        return await resp.text(errors="ignore")
                    if resp.status in {429, 500, 502, 503, 504}:
                        logger.warning("retryable status %s for %s", resp.status, url)
                        await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
                        continue
                    logger.warning("non-200 status %s for %s", resp.status, url)
                    return ""
            except asyncio.TimeoutError:
                logger.warning("timeout on %s", url)
                await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
            except aiohttp.ClientError:
                logger.warning("client error on %s", url)
                await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
        logger.warning("exhausted retries for %s", url)
        return ""

    @staticmethod
    def _extract_listing_urls(html: str) -> list[str]:
        """Extract listing URLs from a search results page."""
        urls = set(LISTING_URL_RE.findall(html))
        urls.update(
            f"https://auto.ria.com{rel}" for rel in LISTING_URL_REL_RE.findall(html)
        )
        return list(urls)

    @staticmethod
    def _random_headers() -> dict[str, str]:
        """Return randomized headers to reduce request fingerprinting."""
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
        }

    async def iter_listing_urls(
        self, session: aiohttp.ClientSession
    ) -> AsyncIterator[list[str]]:
        """Yield batches of listing URLs page by page."""
        collected: set[str] = set()
        page = 1
        while True:
            if self.max_pages and page > self.max_pages:
                break
            page_url = self._build_page_url(self.start_url, page)
            html = await self._fetch_text(session, page_url)
            page_urls = self._extract_listing_urls(html)
            if not page_urls:
                break
            new = [u for u in page_urls if u not in collected]
            collected.update(new)
            if page > 1 and not new:
                break
            logger.info("page %s: found %s urls", page, len(new))
            yield new
            page += 1

    @staticmethod
    def _build_phone_payload(meta: dict[str, Any]) -> dict[str, Any]:
        """Build phone popup payload from extracted metadata."""
        return {
            "blockId": "autoPhone",
            "popUpId": "autoPhone",
            "isLoginRequired": False,
            "isConfirmPhoneEmailRequired": False,
            "autoId": int(meta["auto_id"]),
            "data": [
                ["userId", meta["user_id"]],
                ["phoneId", meta["phone_id"]],
                ["title", meta.get("title", "")],
                ["isCheckedVin", ""],
                ["companyId", ""],
                ["companyEng", ""],
                ["avatar", meta.get("avatar", "")],
                ["userName", meta.get("user_name", "")],
                ["isCardPayer", "1"],
                ["dia", ""],
                ["isOnline", ""],
                ["isCompany", ""],
                ["workTime", ""],
                ["srcAnalytic", "main_side_sellerInfo_sellerInfoPhone_showBottomPopUp"],
            ],
            "params": {
                "userId": meta["user_id"],
                "phoneId": meta["phone_id"],
                "title": meta.get("title", ""),
                "isCheckedVin": "",
                "companyId": "",
                "companyEng": "",
                "avatar": meta.get("avatar", ""),
                "userName": meta.get("user_name", ""),
                "isCardPayer": "1",
                "dia": "",
                "isOnline": "",
                "isCompany": "",
                "workTime": "",
            },
            "target": {},
            "formId": None,
            "langId": 4,
            "device": "desktop-web",
        }

    async def _fetch_phone_number(
        self, session: aiohttp.ClientSession, url: str, meta: dict[str, Any]
    ) -> str | None:
        """Call phone popup endpoint and return normalized digits."""
        payload = self._build_phone_payload(meta)
        try:
            async with session.post(
                PHONE_POPUP_URL, json=payload, headers=self._random_headers()
            ) as resp:
                if resp.status != 200:
                    logger.warning("phone status %s for %s", resp.status, url)
                    return None
                text = await resp.text(errors="ignore")
        except asyncio.TimeoutError:
            logger.warning("phone timeout for %s", url)
            return None
        except aiohttp.ClientError:
            logger.warning("phone client error for %s", url)
            return None
        if not text:
            return None
        phone_value: Any = None
        try:
            payload = json.loads(text)
            if isinstance(payload, dict):
                phone_value = _as_dict(payload.get("additionalParams")).get("phoneStr")
        except json.JSONDecodeError:
            phone_value = text
        digits = re.sub(r"\D", "", str(phone_value)) if phone_value else ""
        return digits or None

    async def scrape_single(
        self, session: aiohttp.ClientSession, url: str
    ) -> CarListing | None:
        """Scrape one listing URL and return a validated CarListing."""
        html = await self._fetch_text(session, url)
        if not html:
            logger.warning("empty response: %s", url)
            return None
        data, phone_meta, missing = parse_listing(html, url)
        if phone_meta and ENABLE_PHONE_FETCH:
            phone_number = await self._fetch_phone_number(session, url, phone_meta)
            if phone_number:
                data["phone_number"] = phone_number
        try:
            if missing:
                optional = {"car_number"}
                critical = [field for field in missing if field not in optional]
                if critical:
                    logger.warning(
                        "missing fields for %s after pinia/ld_json/title/meta: %s",
                        url,
                        ", ".join(missing),
                    )
                else:
                    logger.info(
                        "missing optional fields for %s after pinia/ld_json/title/meta: %s",
                        url,
                        ", ".join(missing),
                    )
            return CarListing(**data)
        except (ValidationError, ValueError, TypeError):
            logger.exception("failed to parse listing: %s", url)
            return None

    async def run(self, db) -> None:
        """Run scraping pipeline and write results in batches."""
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            url_queue: asyncio.Queue[str | None] = asyncio.Queue(
                maxsize=self.concurrency * 2
            )
            result_queue: asyncio.Queue[CarListing | None] = asyncio.Queue(
                maxsize=self.batch_size * 2
            )

            # Producer -> URL queue, workers -> result queue, collector -> DB batches.
            async def producer() -> None:
                async for urls in self.iter_listing_urls(session):
                    for url in urls:
                        await url_queue.put(url)
                for _ in range(self.concurrency):
                    await url_queue.put(None)

            async def worker() -> None:
                while True:
                    url = await url_queue.get()
                    if url is None:
                        break
                    listing = await self.scrape_single(session, url)
                    if listing is not None:
                        await result_queue.put(listing)
                await result_queue.put(None)

            async def collector() -> None:
                batch: list[CarListing] = []
                done_workers = 0
                while True:
                    item = await result_queue.get()
                    if item is None:
                        done_workers += 1
                        if done_workers >= self.concurrency:
                            break
                        continue
                    batch.append(item)
                    if len(batch) >= self.batch_size:
                        db.insert_batch(batch)
                        batch = []
                if batch:
                    db.insert_batch(batch)

            await asyncio.gather(
                producer(), *(worker() for _ in range(self.concurrency)), collector()
            )
