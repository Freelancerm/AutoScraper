import asyncio
import logging

from .database import DB
from .scraper import Scraper

from .scheduler import AppScheduler, load_config_from_env

cfg = load_config_from_env()


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    db = DB()
    db.init_db()
    scraper = Scraper()

    async def run_scrape():
        await scraper.run(db)

    async def run_dump() -> None:
        await db.create_dump()

    scheduler = AppScheduler(cfg, run_scrape=run_scrape, run_dump=run_dump)
    scheduler.start()
    await scheduler.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
