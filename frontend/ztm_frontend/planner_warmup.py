"""Warm the serving process's day networks after atomic artifact publication, and keep today's live view current."""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from ztm_frontend import journey, live, live_times
from ztm_frontend.db import fetch_one, read_connection

if TYPE_CHECKING:
    from flask import Flask

LOGGER = logging.getLogger(__name__)
WARSAW = ZoneInfo("Europe/Warsaw")
CHECK_SECONDS = 30


class Warmer:
    """Check file metadata when idle; load one consistent snapshot when it changes."""

    def __init__(self, app: Flask) -> None:
        """Keep completion state local to this serving app's worker."""
        self.app = app
        self._completed: tuple[tuple[int, int, int, int], date] | None = None
        self._today: tuple[journey.Network, date] | None = None  # the last warm pass's network for its today

    def check(self, now: datetime) -> None:
        """Warm once per publication or Warsaw date; leave failed attempts retryable."""
        path = self.app.config["ZTM_PLANNER_PATH"]
        today = now.astimezone(WARSAW).date()
        try:
            stat = path.stat()
        except FileNotFoundError:
            self._completed = None
            return
        identity = (stat.st_dev, stat.st_ino, stat.st_mtime_ns, stat.st_size), today
        if identity == self._completed:
            return

        started = time.monotonic()
        # A Flask app context owns one read-only DuckDB connection for this entire
        # pass. A rename mid-warmup must not mix metadata, networks or live shapes.
        with self.app.app_context(), read_connection(path, fresh=True):
            metadata = fetch_one(path, "select build_id, first_date, last_date from planner_metadata limit 1")
            if metadata is None:
                raise RuntimeError("planner artifact has no build metadata")
            days = tuple(
                day
                for offset in range(journey.WARM_DAYS)
                if metadata["first_date"] <= (day := today + timedelta(days=offset)) <= metadata["last_date"]
            )
            journey.pin_days(path, str(metadata["build_id"]), days)
            LOGGER.info("Warming planner build %s for %s", metadata["build_id"], days)
            today_net = None
            for day in days:
                net = journey.network(path, day)
                journey.prepare(net)
                if day == today:
                    today_net = net
            if today_net is not None:
                current = live_times.view(path, today_net, today, now)
                if current is not None:
                    journey.prepare(current.net)
        self._completed = identity
        self._today = (today_net, today) if today_net is not None else None
        LOGGER.info("Warmed planner build %s for %s in %.2fs", metadata["build_id"], days, time.monotonic() - started)

    def refresh_live(self, now: datetime) -> None:
        """Patch and prepare today's network for the current feed object, so requests need not."""
        warm = self._today
        if warm is None or warm[1] != now.astimezone(WARSAW).date():
            return  # nothing warm yet, or the date rolled over and check() has yet to re-warm
        current = live_times.view(self.app.config["ZTM_PLANNER_PATH"], warm[0], warm[1], now)
        if current is not None:
            journey.prepare(current.net)

    def run_live(self, stop: threading.Event) -> None:
        """Refresh the live view once per feed object until shutdown, whether or not anyone is searching."""
        while not stop.is_set():
            try:
                self.refresh_live(datetime.now(WARSAW))
            except Exception:
                LOGGER.exception("Planner live refresh failed; retrying in %ss", live.FEED_TTL_S)
            stop.wait(live.FEED_TTL_S)

    def run(self, stop: threading.Event) -> None:
        """Warm immediately, then check at a fixed interval until shutdown."""
        while not stop.is_set():
            try:
                self.check(datetime.now(WARSAW))
            except Exception:
                LOGGER.exception("Planner cache warmup failed; retrying in %ss", CHECK_SECONDS)
            stop.wait(CHECK_SECONDS)


def start(app: Flask) -> None:
    """Start the warmup daemon and, with a live feed, the live refresher; no endpoint or Airflow dependency."""
    if "planner_warmup" in app.extensions:
        return
    if LOGGER.level == logging.NOTSET:
        LOGGER.setLevel(logging.INFO)
    stop = threading.Event()
    app.extensions["planner_warmup"] = stop
    warmer = Warmer(app)
    threading.Thread(target=warmer.run, args=(stop,), name="planner-warmup", daemon=True).start()
    if live.enabled():
        threading.Thread(target=warmer.run_live, args=(stop,), name="planner-live", daemon=True).start()
