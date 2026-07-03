from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from flask import Flask, current_app, render_template, request

from ztm_frontend import queries

EARLY_DELAY_SECONDS = -60
LATE_DELAY_SECONDS = 180
LOW_ON_TIME_RATE = 0.6

if TYPE_CHECKING:
    from datetime import datetime


def create_app() -> Flask:
    """Create the Flask app without opening the DuckDB artifact at import time."""
    app = Flask(__name__)
    db_path = Path(os.environ.get("ZTM_DUCKDB_PATH", "ztm/ztm.duckdb"))

    app.config["ZTM_DUCKDB_PATH"] = db_path
    app.config["ZTM_DUCKDB_META_PATH"] = Path(f"{db_path}.meta.json")

    app.add_template_filter(_format_delay, "delay")
    app.add_template_filter(_format_integer, "integer")
    app.add_template_filter(_format_percent, "percent")
    app.add_template_filter(_format_time, "time")
    app.add_template_filter(_delay_class, "delay_class")
    app.add_template_filter(_percent_class, "percent_class")
    app.add_template_filter(lambda value: json.dumps(value, separators=(",", ":")), "to_json")

    @app.context_processor
    def inject_globals() -> dict[str, Any]:
        return {"meta": queries.get_export_metadata(current_app.config["ZTM_DUCKDB_PATH"])}

    @app.get("/")
    def index() -> str:
        return render_template(
            "overview.html",
            **queries.get_overview(current_app.config["ZTM_DUCKDB_PATH"], request.args.get("date")),
        )

    @app.get("/lines/")
    @app.get("/lines/<line>")
    def lines(line: str | None = None) -> str:
        mode = request.args.get("mode")
        if mode not in {"bus", "tram"}:
            mode = None
        return render_template(
            "lines.html",
            **queries.get_lines(current_app.config["ZTM_DUCKDB_PATH"], line, mode, request.args.get("date")),
        )

    @app.get("/stops/")
    @app.get("/stops/<stop_group_id>")
    @app.get("/stops/<stop_group_id>/<post>")
    def stops(stop_group_id: str | None = None, post: str | None = None) -> str:
        mode = request.args.get("mode")
        if mode not in {"bus", "tram"}:
            mode = None
        return render_template(
            "stops.html",
            **queries.get_stops(
                current_app.config["ZTM_DUCKDB_PATH"],
                stop_group_id,
                mode,
                request.args.get("q", ""),
                post or request.args.get("post"),
                request.args.get("date"),
            ),
        )

    @app.get("/schedule/")
    def schedule() -> str:
        mode = request.args.get("mode")
        if mode not in {"bus", "tram"}:
            mode = None
        return render_template(
            "schedule.html",
            **queries.get_schedule(
                current_app.config["ZTM_DUCKDB_PATH"],
                mode,
                request.args.get("line"),
                request.args.get("date"),
                request.args.get("trip"),
                request.args.get("vehicle"),
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
    return value.strftime("%H:%M")


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
