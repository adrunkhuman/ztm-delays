from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from flask import Flask, current_app, render_template, request

from ztm_frontend import queries


def create_app() -> Flask:
    """Create the Flask app without opening the DuckDB artifact at import time."""
    app = Flask(__name__)
    db_path = Path(os.environ.get("ZTM_DUCKDB_PATH", "ztm/ztm.duckdb"))

    app.config["ZTM_DUCKDB_PATH"] = db_path
    app.config["ZTM_DUCKDB_META_PATH"] = Path(f"{db_path}.meta.json")

    app.add_template_filter(_format_delay, "delay")
    app.add_template_filter(_format_integer, "integer")
    app.add_template_filter(_format_percent, "percent")

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
    def stops(stop_group_id: str | None = None) -> str:
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
                request.args.get("post"),
                request.args.get("date"),
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


app = create_app()
