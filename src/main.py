import asyncio
import logging

from .database import DB
from .scraper import Scraper


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    db = DB()
    db.init_db()
    scraper = Scraper()
    asyncio.run(scraper.run(db))


if __name__ == "__main__":
    main()
