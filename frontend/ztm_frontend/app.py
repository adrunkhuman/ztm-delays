from __future__ import annotations

import json
import os
from datetime import UTC
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytz
from flask import Flask, current_app, render_template, request, url_for

from ztm_frontend import db, queries

EARLY_DELAY_SECONDS = -60
LATE_DELAY_SECONDS = 180
LOW_ON_TIME_RATE = 0.6
WARSAW = pytz.timezone("Europe/Warsaw")

if TYPE_CHECKING:
    from datetime import datetime


def create_app() -> Flask:  # noqa: C901
    """Create the Flask app without opening the DuckDB artifact at import time."""
    app = Flask(__name__)
    db_path = Path(os.environ.get("ZTM_DUCKDB_PATH", "ztm/ztm.duckdb"))
    stylesheet_path = Path(app.static_folder or "") / "site.css"
    stylesheet_version = sha256(stylesheet_path.read_bytes()).hexdigest()[:12]

    app.config["ZTM_DUCKDB_PATH"] = db_path
    app.config["ZTM_DUCKDB_META_PATH"] = Path(f"{db_path}.meta.json")
    app.teardown_appcontext(db.close_request_connections)

    app.add_template_filter(_format_delay, "delay")
    app.add_template_filter(_format_integer, "integer")
    app.add_template_filter(_format_percent, "percent")
    app.add_template_filter(_format_time, "time")
    app.add_template_filter(_delay_class, "delay_class")
    app.add_template_filter(_percent_class, "percent_class")
    app.add_template_filter(lambda value: json.dumps(value, separators=(",", ":")), "to_json")

    @app.context_processor
    def inject_globals() -> dict[str, Any]:
        return {
            "meta": queries.get_export_metadata(current_app.config["ZTM_DUCKDB_PATH"]),
            "navigation_date": _selected_date_arg(),
            "scope_href": _scope_href,
            "stylesheet_version": stylesheet_version,
        }

    @app.url_defaults
    def preserve_window(endpoint: str, values: dict[str, Any]) -> None:
        if endpoint not in {"index", "lines", "stops", "schedule"} or "window" in values:
            return
        selected_window = queries.normalize_window(request.args.get("window"))
        if selected_window != "day":
            values["window"] = selected_window

    @app.get("/")
    def index() -> str:
        return render_template(
            "overview.html",
            **queries.get_overview(
                current_app.config["ZTM_DUCKDB_PATH"],
                _selected_date_arg(),
                request.args.get("window"),
            ),
        )

    @app.get("/lines/")
    @app.get("/lines/<line>")
    def lines(line: str | None = None) -> str:
        return render_template(
            "lines.html",
            **queries.get_lines(
                current_app.config["ZTM_DUCKDB_PATH"],
                line,
                _selected_mode(request.args.get("mode")),
                _selected_date_arg(),
                request.args.get("rank"),
                request.args.get("page"),
                request.args.get("window"),
            ),
        )

    @app.get("/stops/")
    @app.get("/stops/<stop_group_id>")
    @app.get("/stops/<stop_group_id>/<post>")
    def stops(stop_group_id: str | None = None, post: str | None = None) -> str:
        return render_template(
            "stops.html",
            **queries.get_stops(
                current_app.config["ZTM_DUCKDB_PATH"],
                stop_group_id,
                _selected_mode(request.args.get("mode")),
                request.args.get("q", ""),
                post or request.args.get("post"),
                _selected_date_arg(),
                request.args.get("view"),
                request.args.get("rank"),
                request.args.get("page"),
                request.args.get("picker_page"),
                request.args.get("window"),
            ),
        )

    @app.get("/schedule/")
    @app.get("/trips/")
    def schedule() -> str:
        return render_template(
            "schedule.html",
            **queries.get_schedule(
                current_app.config["ZTM_DUCKDB_PATH"],
                _selected_mode(request.args.get("mode")),
                request.args.get("line"),
                _selected_date_arg(),
                request.args.get("trip"),
                request.args.get("vehicle"),
                request.args.get("sort"),
                request.args.get("rank"),
                request.args.get("page"),
                request.args.get("window"),
            ),
        )

    @app.get("/trips/<trip_id>")
    def trip_detail(trip_id: str) -> str:
        return render_template(
            "trip_detail.html",
            **queries.get_trip_detail(
                current_app.config["ZTM_DUCKDB_PATH"],
                trip_id,
                _selected_date_arg(),
                request.args.get("vehicle"),
                request.args.get("window"),
                request.args.get("return_date"),
            ),
        )

    @app.get("/status")
    def status() -> str:
        return render_template("status.html", **queries.get_status(current_app.config["ZTM_DUCKDB_PATH"]))

    return app


def _format_delay(value: float | None) -> str:
    if value is None:
        return "n/a"

    rounded = round(value)
    if rounded > 0:
        return f"+{rounded}s"
    return f"{rounded}s"


def _selected_mode(value: str | None) -> str | None:
    if value in {"bus", "tram"}:
        return value
    return None


def _selected_date_arg() -> str | None:
    if request.headers.get("HX-Request") == "true":
        return request.args.get("date")
    return None


def _scope_href(window: str) -> str:
    values = dict(request.view_args or {})
    values.update(request.args.to_dict())
    values["window"] = queries.normalize_window(window)
    values.pop("page", None)
    return url_for(request.endpoint or "index", **values)


def _format_integer(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{int(value):,}".replace(",", " ")


def _format_percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{float(value) * 100:.0f}%"


def _format_time(value: datetime | None) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(WARSAW).strftime("%H:%M")


def _delay_class(value: float | None) -> str:
    if value is None:
        return "muted"
    if value <= EARLY_DELAY_SECONDS:
        return "early-text"
    if value >= LATE_DELAY_SECONDS:
        return "late-text"
    return "neutral-text"


def _percent_class(value: float | None) -> str:
    if value is None:
        return "muted"
    if value < LOW_ON_TIME_RATE:
        return "late-text"
    return "dim-text"


app = create_app()
