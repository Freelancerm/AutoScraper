from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScheduleConfig:
    tz: str
    scrape_time: str  # "HH:MM"
    dump_time: str  # "HH:MM"
    run_on_startup: bool


def _parse_hhmm(value: str) -> tuple[int, int]:
    # strict "HH:MM"
    if len(value) != 5 or value[2] != ":":
        raise ValueError(f"Invalid time format: {value} (expected HH:MM)")
    hh = int(value[:2])
    mm = int(value[3:])
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise ValueError(f"Invalid time: {value}")
    return hh, mm


class AppScheduler:
    """ Schedules the scraping and dumping tasks using APScheduler. Ensures that jobs do not overlap and handles timezones. """

    def __init__(
            self,
            cfg: ScheduleConfig,
            run_scrape: callable,
            run_dump: callable,
    ) -> None:
        self.cfg = cfg
        self.run_scrape = run_scrape
        self.run_dump = run_dump

        self.tz = ZoneInfo(cfg.tz)
        self.scheduler = AsyncIOScheduler(timezone=self.tz)

        # Locks to avoid overlapping runs
        self._lock = asyncio.Lock()

    async def _guarded(self, name: str, coro) -> None:
        """ Run the given coroutine with a lock to prevent overlapping executions. Logs start, finish, and exceptions. """
        if self._lock.locked():
            log.warning("%s skipped: job already running", name)
            return
        async with self._lock:
            log.info("%s started", name)
            try:
                await coro()
            except Exception as ex:
                log.exception(f"%s failed: {ex}", name)
            finally:
                log.info("%s finished", name)

    async def _scrape_job(self) -> None:
        await self._guarded("SCRAPE", self.run_scrape)

    async def _dump_job(self) -> None:
        await self._guarded("DUMP", self.run_dump)

    def start(self) -> None:
        """ Parse the configured times, set up the scheduled jobs, and start the scheduler. Jobs are set to coalesce and have a misfire grace time of 30"""
        sh, sm = _parse_hhmm(self.cfg.scrape_time)
        dh, dm = _parse_hhmm(self.cfg.dump_time)

        self.scheduler.add_job(
            self._scrape_job,
            CronTrigger(hour=sh, minute=sm, timezone=self.tz),
            id="scrape",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800,
        )
        self.scheduler.add_job(
            self._dump_job,
            CronTrigger(hour=dh, minute=dm, timezone=self.tz),
            id="dump",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800,
        )

        self.scheduler.start()
        log.info("Scheduler started (tz=%s, scrape=%s, dump=%s)",
                 self.cfg.tz, self.cfg.scrape_time, self.cfg.dump_time)

    async def run_forever(self) -> None:
        """ Run the scheduler indefinitely. If configured, also run the scrape job immediately on startup. """
        if self.cfg.run_on_startup:
            asyncio.create_task(self._scrape_job())

        while True:
            await asyncio.sleep(3600)
