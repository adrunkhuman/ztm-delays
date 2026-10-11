from __future__ import annotations

import json
import math
import os
import re
from datetime import UTC, datetime
from functools import lru_cache
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytz
from flask import (
    Flask,
    Response,
    abort,
    current_app,
    make_response,
    render_template,
    request,
    send_from_directory,
    url_for,
)

from ztm_frontend import db, live_status, planner, planner_text, queries, sqlite_geocoding
from ztm_frontend.sqlite_geocoding import GeocodingError

EARLY_DELAY_SECONDS = -60
LATE_DELAY_SECONDS = 180
LOW_ON_TIME_RATE = 0.6
WARSAW = pytz.timezone("Europe/Warsaw")
MAP_MONTH = re.compile(r"\d{4}-\d{2}")
MAP_SEGMENT_ID = re.compile(r"[0-9]{1,9}")
MAP_POPUP_MAX_IDS = 100
MAP_FILES = {"routes.geojson", "mini-bus.svg", "mini-tram.svg"}
MAP_VIEWS = {"mode": ("bus", "tram"), "period": ("wd", "we")}
MAP_POPUP_ROWS = 3
MAP_POPUP_CHIPS = 5
MAP_NEUTRAL_SECONDS = 10
# The planner form's query keys; links rebuilt from the request (the language switch) carry only these.
PLANNER_ARGS = ("date", "time", "from", "to", "q_from", "q_to", "from_lat", "from_lon", "to_lat", "to_lon")
PLANNER_LANG_COOKIE = "planner_lang"
PLANNER_LANG_COOKIE_SECONDS = 365 * 86_400


def create_app() -> Flask:  # noqa: C901
    """Create the Flask app without opening the DuckDB artifact at import time."""
    app = Flask(__name__)
    db_path = Path(os.environ.get("ZTM_DUCKDB_PATH", "ztm/ztm.duckdb"))
    # Content hashes in static URLs: Cloudflare and browsers cache static files for hours.
    static_versions = {
        path.name: sha256(path.read_bytes()).hexdigest()[:12]
        for path in Path(app.static_folder or "").iterdir()
        if path.suffix in {".css", ".js", ".json"}
    }

    def asset(filename: str) -> str:
        return url_for("static", filename=filename, v=static_versions.get(filename))

    app.config["ZTM_DUCKDB_PATH"] = db_path
    app.config["ZTM_DUCKDB_META_PATH"] = Path(f"{db_path}.meta.json")
    # Monthly route maps are published beside the DuckDB export, one directory per month.
    app.config["ZTM_MAPS_DIR"] = Path(os.environ.get("ZTM_MAPS_DIR", db_path.parent / "maps"))
    # The planner artifact is published separately, nightly, beside the export (see planner.py).
    app.config["ZTM_PLANNER_PATH"] = Path(
        os.environ.get("ZTM_PLANNER_PATH", db_path.parent / "planner" / "planner.duckdb")
    )
    app.config["ZTM_GEOCODING_DB"] = Path(path) if (path := os.environ.get("ZTM_GEOCODING_DB")) else None
    app.add_template_filter(planner.clock, "clock")
    app.teardown_appcontext(db.close_request_connections)
    _add_planner_routes(app)

    app.add_template_filter(_format_delay, "delay")
    app.add_template_filter(_format_integer, "integer")
    app.add_template_filter(_format_percent, "percent")
    app.add_template_filter(_format_time, "time")
    app.add_template_filter(_format_utc_timestamp, "utc_timestamp")
    app.add_template_filter(_delay_class, "delay_class")
    app.add_template_filter(_percent_class, "percent_class")
    app.add_template_filter(lambda value: json.dumps(value, separators=(",", ":")), "to_json")

    @app.context_processor
    def inject_globals() -> dict[str, Any]:
        requested_window = queries.normalize_window(request.args.get("window"))
        grouped_windows_available = queries.grouped_windows_available(current_app.config["ZTM_DUCKDB_PATH"])
        if requested_window != "day" and not grouped_windows_available:
            current_app.logger.warning(
                "Grouped window %s requested before serving artifact rebuild: %s",
                requested_window,
                current_app.config["ZTM_DUCKDB_PATH"],
            )
        return {
            "meta": queries.get_export_metadata(current_app.config["ZTM_DUCKDB_PATH"]),
            "navigation_date": _selected_date_arg(),
            "grouped_windows_available": grouped_windows_available,
            "scope_href": _scope_href,
            "asset": asset,
            "map_available": bool(_map_months()),
            "planner_available": current_app.config["ZTM_PLANNER_PATH"].is_file(),
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
            map_month=(map_month := next(reversed(_map_months()), None)),
            map_version=_map_month_data(map_month)[1] if map_month else None,
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

    @app.get("/map")
    def route_map() -> str:
        months = _map_months()
        if not months:
            abort(404)
        month = request.args.get("month")
        if month not in months:
            month = months[-1]
        index = months.index(month)
        data, version = _map_month_data(month)
        return render_template(
            "map.html",
            map_month=month,
            map_meta=data["meta"],
            map_version=version,
            map_view=_map_view(),
            previous_month=months[index - 1] if index > 0 else None,
            next_month=months[index + 1] if index + 1 < len(months) else None,
        )

    @app.get("/map/segments")
    def map_segments() -> str:
        return _render_map_popup()

    @app.get("/map/<month>/<name>")
    def map_file(month: str, name: str) -> Response:
        if month not in _map_months() or name not in MAP_FILES:
            abort(404)
        return send_from_directory(current_app.config["ZTM_MAPS_DIR"] / month, name, max_age=3600)

    _add_status_routes(app)

    return app


def _add_status_routes(app: Flask) -> None:
    @app.get("/status")
    def status() -> str:
        return render_template(
            "status.html",
            live=live_status.live_view(),
            history=live_status.history_view(),
            **queries.get_status(current_app.config["ZTM_DUCKDB_PATH"]),
        )

    @app.get("/status/live")
    def status_live() -> Response:
        """Live panel fragment, polled by the status page."""
        response = make_response(render_template("_status_live.html", live=live_status.live_view()))
        response.headers["Cache-Control"] = "no-store"
        return response


def _add_planner_routes(app: Flask) -> None:
    _add_planner_location_routes(app)
    app.add_template_filter(planner_text.plural, "plural")
    app.add_template_filter(planner_text.day_label, "day_label")

    @app.get("/planner")
    def planner_page() -> Response:
        return _render_planner("planner.html", remember_language=True)

    @app.get("/planner/results")
    def planner_results() -> Response:
        return _render_planner("_planner_results.html")

    @app.get("/planner/trip/<int(signed=True):trip_key>")
    def planner_trip(trip_key: int) -> str:
        day = planner.parse_date(request.args.get("date"))
        board, alight = request.args.get("board", type=int), request.args.get("alight", type=int)
        if day is None or board is None or alight is None:
            abort(404)
        stops = planner.trip_stops(_planner_path(), trip_key, board, alight, day, datetime.now(WARSAW))
        if not stops:
            abort(404)
        return render_template("_planner_stops.html", stops=stops, t=planner_text.TEXT[_planner_lang()])


def _add_planner_location_routes(app: Flask) -> None:
    @app.get("/planner/suggest/<field>")
    def planner_suggest(field: str) -> Response:
        if field not in {"from", "to"}:
            abort(404)
        query = request.args.get(f"q_{field}", "")
        groups = planner.suggest(_planner_path(), query)
        database = current_app.config["ZTM_GEOCODING_DB"]
        lang = _planner_lang()
        message = None
        if database is not None:
            try:
                groups += sqlite_geocoding.search(database, query)
            except GeocodingError:
                message = planner_text.TEXT[lang]["address_error"]
        html = render_template(
            "_planner_suggest.html", field=field, groups=groups, message=message, t=planner_text.TEXT[lang]
        )
        if message and groups:
            # The fragment renders a notice only for an empty list; retain both stops and the error.
            html += render_template(
                "_planner_suggest.html", field=field, groups=[], message=message, t=planner_text.TEXT[lang]
            )
        response = make_response(html)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/planner/reverse")
    def planner_reverse() -> Response:
        _planner_path()
        name = None
        try:
            lat, lon = float(request.args.get("lat", "")), float(request.args.get("lon", ""))
            database = current_app.config["ZTM_GEOCODING_DB"]
            candidate = sqlite_geocoding.reverse(database, lat, lon) if database is not None else None
            if candidate:
                name = candidate["name"]
                if candidate["distance_m"] > 20:  # noqa: PLR2004 - distinguish a nearby address from an exact label
                    name = str(planner_text.TEXT[_planner_lang()]["near"]).format(name=name)
        except (ValueError, OverflowError, GeocodingError):
            pass  # Coordinates remain a usable label if reverse lookup is unavailable.
        response = make_response({"name": name})
        response.headers["Cache-Control"] = "no-store"
        return response


def _render_planner(template: str, *, remember_language: bool = False) -> Response:
    """Render either response; only page navigation persists an explicit language choice."""
    now = datetime.now(WARSAW)
    lang = _planner_lang()
    args = {key: request.args[key] for key in PLANNER_ARGS if key in request.args}
    response = make_response(
        render_template(
            template,
            lang=lang,
            t=planner_text.TEXT[lang],
            args=args,
            **planner.get_page(_planner_path(), args, now.date(), now.hour * 3600 + now.minute * 60, now),
        )
    )
    if remember_language and request.args.get("lang") in planner_text.LANGS:
        response.set_cookie(PLANNER_LANG_COOKIE, lang, max_age=PLANNER_LANG_COOKIE_SECONDS, samesite="Lax")
    response.vary.update(("Accept-Language", "Cookie"))
    return response


def _planner_lang() -> str:
    """The switch's ``?lang=``, then the remembered choice, then the browser's preferred language."""
    for chosen in (request.args.get("lang"), request.cookies.get(PLANNER_LANG_COOKIE)):
        if chosen in planner_text.LANGS:
            return chosen
    return request.accept_languages.best_match(planner_text.LANGS, default=planner_text.DEFAULT_LANG)


def _planner_path() -> Path:
    path: Path = current_app.config["ZTM_PLANNER_PATH"]
    if not path.is_file():
        abort(404)
    return path


def _map_months() -> list[str]:
    """Published route-map months, oldest first; a month counts once its segments.json exists."""
    maps_dir: Path = current_app.config["ZTM_MAPS_DIR"]
    if not maps_dir.is_dir():
        return []
    return sorted(
        path.name
        for path in maps_dir.iterdir()
        if MAP_MONTH.fullmatch(path.name) and (path / "segments.json").is_file()
    )


def _map_view() -> dict[str, str]:
    """Map mode and period from the query string; unknown values fall back to the first option."""
    return {
        name: value if (value := request.args.get(name)) in values else values[0] for name, values in MAP_VIEWS.items()
    }


def _map_month_data(month: str) -> tuple[dict[str, Any], str]:
    """Parsed segments.json and a version that changes whenever the month is republished."""
    path = current_app.config["ZTM_MAPS_DIR"] / month / "segments.json"
    try:
        mtime_ns = path.stat().st_mtime_ns
    except FileNotFoundError:
        # A rebuild swaps the month directory in two renames; the month is briefly absent.
        abort(404)
    return _read_map_segments(path, mtime_ns), str(mtime_ns)


@lru_cache(maxsize=4)
def _read_map_segments(path: Path, _mtime_ns: int) -> dict[str, Any]:
    """Parsed segments.json; the mtime in the cache key picks up a republished month."""
    return json.loads(path.read_text())


def _render_map_popup() -> str:
    """One page of the segments under a map click, busiest first in the selected view."""
    month = request.args.get("month", "")
    if month not in _map_months():
        abort(404)
    view = _map_view()
    data, version = _map_month_data(month)
    # Feature ids are only stable within one build; ids from a page loaded before a rebuild would
    # silently describe other corridors.
    if request.args.get("v") != version:
        return render_template("_map_popup.html", stale=True)
    raw_ids = request.args.get("ids", "").split(",")
    ids = list(dict.fromkeys(int(value) for value in raw_ids if MAP_SEGMENT_ID.fullmatch(value)))[:MAP_POPUP_MAX_IDS]
    prefix = f"{view['period']}_{view['mode']}"
    segments = [(id_, data["segments"].get(str(id_), {})) for id_ in ids]
    paths = sorted(
        ((id_, segment) for id_, segment in segments if f"{prefix}_d" in segment),
        key=lambda path: -path[1][f"{prefix}_n"],
    )
    if not paths:
        abort(404)
    pages = math.ceil(len(paths) / MAP_POPUP_ROWS)
    page = min(max(request.args.get("page", 0, type=int), 0), pages - 1)
    start = page * MAP_POPUP_ROWS
    return render_template(
        "_map_popup.html",
        rows=[
            _map_popup_row(id_, segment, prefix, view["mode"]) for id_, segment in paths[start : start + MAP_POPUP_ROWS]
        ],
        anchor=data["meta"]["anchor"],
        mode=view["mode"],
        page=page,
        pages=pages,
        first=start + 1,
        last=min(start + MAP_POPUP_ROWS, len(paths)),
        total=len(paths),
        page_href=lambda target: url_for(
            "map_segments", month=month, v=version, **view, ids=",".join(map(str, ids)), page=target
        ),
    )


def _map_popup_row(id_: int, segment: dict[str, Any], prefix: str, mode: str) -> dict[str, Any]:
    delta = round(segment[f"{prefix}_d"])
    count = segment[f"{prefix}_n"]
    lines = segment.get(mode, [])
    return {
        "id": id_,
        "lines": lines[:MAP_POPUP_CHIPS],
        "more_lines": max(len(lines) - MAP_POPUP_CHIPS, 0),
        "delta": f"+{delta}s" if delta > 0 else f"{delta}s",
        "delta_class": "early-text"
        if delta <= -MAP_NEUTRAL_SECONDS
        else "late-text"
        if delta >= MAP_NEUTRAL_SECONDS
        else "neutral-text",
        "stops": segment["stops"],
        "aliases": segment["aliases"],
        "count": count,
        "recover": (recover := segment[f"{prefix}_r"] / count * 100),
        "gain": (gain := segment[f"{prefix}_g"] / count * 100),
        "steady": max(100 - recover - gain, 0),
    }


def _format_utc_timestamp(value: object) -> str:
    """Format export timestamps; naive warehouse timestamps are UTC."""
    if not value:
        return "unknown"
    try:
        timestamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except ValueError:
        return "unknown"
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


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
    return request.args.get("date")


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
