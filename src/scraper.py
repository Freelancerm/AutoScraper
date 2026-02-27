import asyncio
import math
import re
import logging

import aiohttp
from bs4 import BeautifulSoup

from .config import (
    SCRAPE_START_URL,
    MAX_RETRIES,
    MAX_CONCURRENCY,
    MAX_PAGES_TO_SCRAPE
)
from typing import List, Optional
from .models import CarListing

AD_PER_PAGE = 20


class Scraper:
    """Main Scraper class to handle fetching and parsing of auto listings from auto.ria.com."""

    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        self.timeout = aiohttp.ClientTimeout(total=20)
        self.sem = asyncio.Semaphore(MAX_CONCURRENCY)

    # === TOTAL PAGES ===
    async def get_total_pages(self, session: aiohttp.ClientSession) -> int:
        """
        Get total number of pages by parsing the initial search page HTML for results count.
        We look for the JavaScript variable that holds the total results count and calculate pages from it.
        """
        logging.info("Getting count pages from window.ria.server.resultsCount...")

        # Load the first page HTML to find the total results count.
        html = await self._fetch(session, SCRAPE_START_URL + "?page=1")

        if not html:
            logging.error(
                "Can't load the first page to get total results count, defaulting to 1 page."
            )
            return 1

        try:
            # Searching number in string window.ria.server.resultsCount = Number(312990);
            match = re.search(
                r"window\.ria\.server\.resultsCount\s*=\s*Number\((\d+)\)", html
            )

            if match:
                ads_count = int(match.group(1))
                total_pages = math.ceil(ads_count / AD_PER_PAGE)
                logging.info(f"Found listings: {ads_count}. Pages: {total_pages}")
                return total_pages

        except Exception as ex:
            logging.error(f"Error in process resultsCount: {ex}")

        logging.warning("Don't find count pages, parsing only 1 page.")
        return 1

    # === STABLE FETCH (aiohttp): retry + backoff ===

    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> str | None:
        """Fetch a URL with retries and exponential backoff."""
        backoff = 1.0

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with self.sem:
                    async with session.get(
                            url, headers=self.headers, allow_redirects=True
                    ) as resp:
                        status = resp.status
                        raw = await resp.read()
                if status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 20)
                    continue
                if status != 200:
                    logging.info(f"Failed to fetch {url}: HTTP {status}")
                    return None
                return raw.decode("utf-8", errors="replace")
            except (aiohttp.ClientError, asyncio.TimeoutError) as ex:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 20)
                logging.error(f"Error fetching {url} (attempt {attempt}): {ex}")

        return None

    # === SEARCH PAGE: fetch + parse urls ===

    async def fetch_search_page(
            self, session: aiohttp.ClientSession, page_num: int
    ) -> str:
        """Fetch a search results page HTML."""
        url = f"{SCRAPE_START_URL}?page={page_num}"
        return await self._fetch(session, url)

    @staticmethod
    async def parse_search_page(html: str) -> List[str]:
        """
        Extract listing URLs from a search results page.
        """
        soup = BeautifulSoup(html, "html.parser")
        links = soup.find_all("a", class_="m-link-ticket", href=True)
        return list(
            {
                l["href"]
                for l in links
                if l["href"].startswith("https://auto.ria.com/uk/auto")
            }
        )

    # === LISTING PAGE: fetch + parse to CarListing ===
    async def fetch_listing_html(
            self, session: aiohttp.ClientSession, url: str
    ) -> Optional[str]:
        return await self._fetch(session, url)

    @staticmethod
    def _get_title(soup: BeautifulSoup) -> str:
        """Extract title from the listing page."""
        return soup.find("h1").get_text(strip=True) if soup.find("h1") else "Auto"

    @staticmethod
    def _get_price_usd(html: str, soup: BeautifulSoup) -> int | None:
        """Extract price in USD from HTML using regex and BeautifulSoup as fallback."""
        price_usd = 0

        price_match = (
                re.search(r'"usd"\s*:\s*(\d+)', html)
                or re.search(r'"priceUSD"\s*:\s*(\d+)', html)
                or re.search(r'"price"\s*:\s*(\d+)', html)
        )

        if price_match:
            val = int(price_match.group(1))
            # If price is small (like 11), it's likely not price.
            # If it's large (150000), it's already in full amount.
            if val > 100:
                price_usd = val
            else:
                # Try to find price in the text, like "11 тис. $"
                price_tag = soup.select_one(".price_value strong")
                if price_tag:
                    price_usd = int(re.sub(r"\D", "", price_tag.get_text()))
        return price_usd if price_usd > 0 else None

    @staticmethod
    def _get_odometer(html: str) -> int | None:
        """
        Extract odometer (mileage) from HTML using regex.
        Find raceInt of odometer. It can be in different formats, so we will try several regex patterns to find it.
        We will look for "raceInt":12345 or "odometer":12345 in the raw HTML, and if that fails, we will try to find it in the text using BeautifulSoup.
        """
        odometer = 0

        odo_match = re.search(r'"raceInt"\s*:\s*(\d+)', html) or re.search(
            r'"odometer"\s*:\s*(\d+)', html
        )
        if odo_match:
            return int(odo_match.group(1))
        else:
            # Reserve search in text, like "150 тис. км"
            text_odo = re.search(r"(\d+)\s*тис\.\s*км", html)
            if text_odo:
                odometer = int(text_odo.group(1)) * 1000
        return odometer if odometer > 0 else None

    @staticmethod
    def _get_username(html: str) -> str:
        """Extract seller's name from HTML."""
        username_match = re.search(r'"userName"\s*:\s*"([^"]+)"', html)
        return username_match.group(1) if username_match else "Продавець"

    async def _get_phone_number(
            self, session: aiohttp.ClientSession, html: str, url: str
    ) -> int | None:
        """
        Get userId, phoneId, autoId from HTML.
        It can be in PINIA or in the raw HTML as JavaScript variables.
        """
        user_id_match = re.search(r'"userId"\s*:\s*(\d+)', html)
        phone_id_match = re.search(r'"phoneId"\s*:\s*"(\d+)"', html)
        auto_id_match = re.search(r"_(\d+)\.html", url)

        user_id = user_id_match.group(1) if user_id_match else None
        phone_id = phone_id_match.group(1) if phone_id_match else None
        auto_id = auto_id_match.group(1) if auto_id_match else None

        if not all([user_id, phone_id, auto_id]):
            logging.warning(f"Still no IDs for {url}: user={user_id}, phone={phone_id}")
            return None

        # Proceed to API call to get phone number using the extracted IDs.
        payload = {
            "blockId": "autoPhone",
            "popUpId": "autoPhone",
            "autoId": int(auto_id),
            "data": [
                ["userId", str(user_id)],
                ["phoneId", str(phone_id)],
                ["userName", "Продавець"],
                ["srcAnalytic", "main_side_sellerInfo_sellerInfoPhone_showBottomPopUp"],
            ],
            "params": {"userId": str(user_id), "phoneId": str(phone_id)},
            "langId": 4,
            "device": "desktop-web",
        }

        api_url = "https://auto.ria.com/bff/final-page/public/auto/popUp/"
        headers = {
            **self.headers,
            "X-Ria-Source": "vue3-1.47.0",
            "Content-Type": "application/json",
        }

        try:
            async with session.post(api_url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    res = data.get("additionalParams", {}).get("phoneStr", "")
                    raw_phone = re.sub(r"\D", "", res)
                    if raw_phone.startswith("0") and len(raw_phone) == 10:
                        return int("38" + raw_phone)
        except Exception as ex:
            logging.error(f"API Phone Error: {ex}")
        return None

    @staticmethod
    def _get_images(html: str) -> tuple:
        """Extract first image URL and total image count from HTML."""
        image_links = re.findall(
            r'"large":"(https://cdn\d+\.riastatic\.com/photosnew/auto/photo/.*?hd\.webp)"',
            html,
        )
        return (image_links[0], len(image_links)) if image_links else (None, 0)

    @staticmethod
    def _get_car_number(html: str) -> str | None:
        """
        Extract car number (stateNumber) from HTML.
        We will try several methods to find it, starting with the most reliable one (meta description), and then falling back to BeautifulSoup if needed.
        """
        # First try to find in meta description, where it is often in format "(ВН1234ЕК)". We
        pattern = r"\(([А-ЯA-Z]{2}\d{4}[А-ЯA-Z]{2})\)"
        number_match = re.search(pattern, html)

        if number_match:
            return number_match.group(1).strip()
        return None

    @staticmethod
    def _get_car_vin(html: str) -> str | None:
        """Extract VIN from HTML using regex."""
        vin_match = re.search(r'"vin"\s*:\s*"([A-Z0-9]{17})"', html)
        return vin_match.group(1) if vin_match else None

    async def parse_listing_page(
            self, session: aiohttp.ClientSession, html: str, url: str
    ) -> CarListing:
        soup = BeautifulSoup(html, "html.parser")

        title = self._get_title(soup)
        price_usd = self._get_price_usd(html, soup)
        odometer = self._get_odometer(html)
        username = self._get_username(html)
        phone_number = await self._get_phone_number(session, html, url)
        image_url, images_count = self._get_images(html)
        car_number = self._get_car_number(html)
        car_vin = self._get_car_vin(html)

        return CarListing(
            url=url,
            title=title,
            price_usd=price_usd,
            odometer=odometer,
            username=username,
            phone_number=phone_number,
            image_url=image_url,
            images_count=images_count,
            car_number=car_number,
            car_vin=car_vin,
        )

    async def run(self, db) -> None:
        """Streamlined main method to run the scraper with database integration."""
        logging.info("Starting Scraper jobs...")

        async with aiohttp.ClientSession() as session:
            # Get total pages to scan
            total_pages = await self.get_total_pages(session)

            if MAX_PAGES_TO_SCRAPE > 0:
                pages_to_scan = min(total_pages, MAX_PAGES_TO_SCRAPE)
            else:
                pages_to_scan = total_pages

            logging.info(f"Scanning {pages_to_scan} pages out of {total_pages} total available.")

            for page_num in range(1, pages_to_scan + 1):
                logging.info(f"=== Page parsing {page_num}/{pages_to_scan} ===")
                # Get html of the search page
                html = await self.fetch_search_page(session, page_num)
                if not html:
                    continue

                # Get all listing URLs from the search page
                all_urls = await self.parse_search_page(html)
                existing = db.existing_urls(all_urls)
                new_urls = [url for url in all_urls if url not in existing]
                logging.info(f"Found {len(all_urls)} links, {len(existing)} already in DB, {len(new_urls)} new to process.")
                if not all_urls:
                    continue

                tasks = [self._process_single_listing(session, url, db) for url in new_urls]
                await asyncio.gather(*tasks)

                # For stability
                # await asyncio.sleep(1.5)

    async def _process_single_listing(
            self, session: aiohttp.ClientSession, url: str, db
    ) -> None:
        """Helper method to process a single listing URL: fetch, parse, and save to DB."""
        try:
            html = await self.fetch_listing_html(session, url)
            if not html:
                return
            if "Оголошення не знайдено" in html or "Видалено" in html:
                logging.info("Skip %s: listing removed", url)
                return
            car_data = await self.parse_listing_page(session, html, url)
            if not car_data.image_url:
                logging.info("Skip %s: no image_url (deleted/invalid listing)", url)
                return
            if not car_data.phone_number:
                logging.info("Skip %s: no phone number (API error or missing IDs)", url)
                return

            # Saved to DB after parsing each listing to avoid data loss in case of crashes.
            db.insert_batch([car_data])
            logging.info(f"Saved: {car_data.title} | {car_data.phone_number}")


        except Exception as ex:
            logging.exception(f"Error in process %s, {ex}", url)
