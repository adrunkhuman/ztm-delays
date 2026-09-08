"""Offline SQL/dbt boundary tests; no Airflow or Google SDK installation required."""

from __future__ import annotations

import datetime
import hashlib
import importlib.util
import json
import re
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import Mock

import duckdb
import pytest
from jinja2 import Environment, StrictUndefined

if TYPE_CHECKING:
    from collections.abc import Callable

DBT = Path(__file__).resolve().parents[2] / "dbt"


class TableRelation(str):
    __slots__ = ()
    type = "table"


class MacroReturn(Exception):
    def __init__(self, value: object) -> None:
        self.value = value


def _return(value: object) -> None:
    raise MacroReturn(value)


def _error(message: str) -> None:
    raise ValueError(message)


def _macros(**context: object) -> SimpleNamespace:
    env = Environment(undefined=StrictUndefined, extensions=["jinja2.ext.do"], autoescape=False)  # noqa: S701 - SQL, not HTML.
    defaults = {
        "execute": True,
        "flags": SimpleNamespace(WHICH="run", FULL_REFRESH=False),
        "var": lambda name, default=None: default,
        "ref": lambda name: name,
        "this": SimpleNamespace(database="project", schema="dataset", identifier="ledger"),
        "adapter": SimpleNamespace(get_relation=lambda **kwargs: TableRelation("int_schedule_fingerprint_daily")),
        "modules": SimpleNamespace(datetime=datetime, re=re),
        "exceptions": SimpleNamespace(raise_compiler_error=_error),
        "return": _return,
        "warsaw_scheduled_timestamp": lambda day, seconds: (
            f"timezone('Europe/Warsaw', cast({day} as timestamp) + {seconds} * interval '1 second')"
        ),
    }
    defaults.update(context)
    source = "\n".join(
        (DBT / "macros" / name).read_text()
        for name in (
            "schedule_ledger.sql",
            "schedule_fingerprints.sql",
            "gtfs_trip_schedule_history.sql",
        )
    )
    module = env.from_string(source).make_module(defaults)

    def wrap(name: str) -> Callable[..., object]:
        def call(*args: object) -> object:
            try:
                return getattr(module, name)(*args)
            except MacroReturn as returned:
                return returned.value

        return call

    return SimpleNamespace(**{name: wrap(name) for name in dir(module) if not name.startswith("_")})


def _variables(**values: object) -> Callable[..., object]:
    return lambda name, default=None: values.get(name, default)


def test_runtime_plan_checks_all_mapping_dates_and_ignores_successful_empty_dates() -> None:
    connection = duckdb.connect()
    connection.execute("create table int_gtfs_processing_snapshot(processing_date date, gtfs_snapshot_id varchar)")
    connection.execute(
        "create table int_schedule_fingerprint_daily(processing_date date, gtfs_snapshot_id varchar, is_date_marker boolean)"
    )
    connection.execute("""insert into int_gtfs_processing_snapshot values
        ('2026-01-01', 'republish'), ('2026-01-02', 'republish'), ('2026-01-03', 'empty'), ('2026-01-04', 'new')""")
    connection.execute("""insert into int_schedule_fingerprint_daily values
        ('2026-01-01', 'old', true), ('2026-01-02', 'old', true), ('2026-01-03', 'empty', true),
        ('2026-01-05', 'removed', true), ('2026-01-06', null, true)""")
    queries = []

    def query(sql: str) -> SimpleNamespace:
        queries.append(sql)
        return SimpleNamespace(rows=connection.execute(sql).fetchall())

    plan = _macros(run_query=query).schedule_ledger_plan()
    assert plan == [
        {"processing_date": "2026-01-01", "gtfs_snapshot_id": "republish"},
        {"processing_date": "2026-01-02", "gtfs_snapshot_id": "republish"},
        {"processing_date": "2026-01-04", "gtfs_snapshot_id": "new"},
        {"processing_date": "2026-01-05", "gtfs_snapshot_id": None},
    ]
    assert len(queries) == 1
    assert "stg_gtfs" not in queries[0]
    assert "limit 32" in queries[0]
    connection.close()


@pytest.mark.parametrize(
    ("variables", "existing", "message"),
    [
        ({}, None, "missing"),
        ({"schedule_ledger_max_dates": 367}, "ledger", "1..366"),
        (
            {
                "schedule_ledger_bootstrap": True,
                "schedule_ledger_start_date": "2026-01-01",
                "schedule_ledger_end_date": "2026-02-01",
            },
            None,
            "range",
        ),
        ({"schedule_ledger_plan": []}, "ledger", "compile-only"),
    ],
)
def test_runtime_plan_fails_before_any_query(variables: dict, existing: str | None, message: str) -> None:
    query = Mock(side_effect=AssertionError("Unexpected query"))
    macro = _macros(
        var=_variables(**variables),
        adapter=SimpleNamespace(get_relation=lambda **kwargs: None if existing is None else TableRelation(existing)),
        run_query=query,
    )
    with pytest.raises(ValueError, match=message):
        macro.schedule_ledger_plan()
    query.assert_not_called()


def test_bootstrap_is_explicit_bounded_and_full_refresh_is_rejected() -> None:
    query = Mock(return_value=SimpleNamespace(rows=[(datetime.date(2026, 1, 1), "snapshot")]))
    variables = _variables(
        schedule_ledger_bootstrap=True, schedule_ledger_start_date="2026-01-01", schedule_ledger_end_date="2026-01-02"
    )
    plan = _macros(
        var=variables, adapter=SimpleNamespace(get_relation=lambda **kwargs: None), run_query=query
    ).schedule_ledger_plan()
    assert plan == [{"processing_date": "2026-01-01", "gtfs_snapshot_id": "snapshot"}]
    assert "generate_date_array(date('2026-01-01'), date('2026-01-02'))" in query.call_args.args[0]
    with pytest.raises(ValueError, match="full-refresh"):
        _macros(flags=SimpleNamespace(WHICH="run", FULL_REFRESH=True)).schedule_ledger_plan()


def test_oversized_mapping_change_is_not_silently_truncated() -> None:
    query = Mock(return_value=SimpleNamespace(rows=[(datetime.date(2026, 1, 1), "s")] * 32))
    with pytest.raises(ValueError, match="Affected schedule dates exceed"):
        _macros(run_query=query).schedule_ledger_plan()


def test_explicit_repair_reexpands_same_snapshot_without_rebuilding_other_dates() -> None:
    query = Mock(return_value=SimpleNamespace(rows=[]))
    _macros(var=_variables(schedule_ledger_repair_dates=["2026-01-02"]), run_query=query).schedule_ledger_plan()
    sql = query.call_args.args[0]
    assert "union distinct" in sql
    assert "unnest([date('2026-01-02')])" in sql
    assert "left join int_gtfs_processing_snapshot" in sql


def test_offline_plan_requires_validated_pins_and_never_queries() -> None:
    flags = SimpleNamespace(WHICH="compile", FULL_REFRESH=False)
    query = Mock(side_effect=AssertionError("Unexpected query"))
    plan = [{"processing_date": "2026-01-01", "gtfs_snapshot_id": "snapshot-1"}]
    assert (
        _macros(flags=flags, var=_variables(schedule_ledger_plan=plan), run_query=query).schedule_ledger_plan() == plan
    )
    with pytest.raises(ValueError, match="requires schedule_ledger_plan"):
        _macros(flags=flags, run_query=query).schedule_ledger_plan()
    bad = [{"processing_date": "2026-01-01", "gtfs_snapshot_id": "bad' OR true"}]
    with pytest.raises(ValueError, match="Invalid pinned"):
        _macros(flags=flags, var=_variables(schedule_ledger_plan=bad)).schedule_ledger_plan()
    with pytest.raises(ValueError, match="Duplicate"):
        _macros(flags=flags, var=_variables(schedule_ledger_plan=plan * 2)).schedule_ledger_plan()
    query.assert_not_called()


def test_fingerprint_bytes_use_absolute_warsaw_order_then_end_then_signature() -> None:
    connection = duckdb.connect()
    # DuckDB MD5 already returns the same lowercase hex that BigQuery TO_HEX(MD5) produces.
    connection.execute("create macro to_hex(value) as value")
    connection.execute("""create table trips(processing_date date, gtfs_snapshot_id varchar, line varchar,
        direction_id bigint, schedule_day_type varchar, service_date date, trip_start_seconds bigint,
        trip_end_seconds bigint, trip_timetable_signature varchar)""")
    connection.execute("""insert into trips values
        ('2026-01-02', 's', '187', 0, 'weekday', '2026-01-02', 3600, 3800, 'D'),
        ('2026-01-02', 's', '187', 0, 'weekday', '2026-01-01', 90000, 90100, 'B'),
        ('2026-01-02', 's', '187', 0, 'weekday', '2026-01-02', 3600, 3700, 'A'),
        ('2026-01-02', 's', '187', 0, 'weekday', '2026-01-01', 3600, 3700, 'PRIOR')""")
    sql = _macros().schedule_fingerprints("trips").replace("'\\n'", "chr(10)")
    row = connection.execute(sql).fetchone()
    assert row[-2:] == (hashlib.md5(b"PRIOR\nA\nB\nD", usedforsecurity=False).hexdigest(), 4)
    connection.close()


def test_rendered_ledger_expands_history_and_marks_removed_dates() -> None:
    plan = [
        {"processing_date": "2026-01-02", "gtfs_snapshot_id": "pin"},
        {"processing_date": "2026-01-03", "gtfs_snapshot_id": None},
    ]
    macros = _macros()
    env = Environment(extensions=["jinja2.ext.do"], autoescape=False)  # noqa: S701 - SQL, not HTML.
    sql = env.from_string((DBT / "models/intermediate/int_schedule_fingerprint_daily.sql").read_text()).render(
        ref=lambda name: name,
        config=lambda **kwargs: "",
        tojson=json.dumps,
        schedule_ledger_plan=lambda: plan,
        gtfs_trip_schedule_history=macros.gtfs_trip_schedule_history,
        schedule_fingerprints=macros.schedule_fingerprints,
    )
    # Joins also exclude unrelated snapshots; only the raw scan bound is unobservable in results.
    for table in ("calendar_dates", "trips", "stop_times"):
        assert re.search(rf"from stg_gtfs__{table}\s+where gtfs_snapshot_id in \(\s*'pin'\s*\)", sql)
    # Local dialect spellings only, not a general BigQuery translator.
    sql = sql.replace("date_sub(processing_date, interval 1 day)", "cast(processing_date - interval '1 day' as date)")
    sql = sql.replace("safe_cast(", "try_cast(").replace("r'", "'")
    sql = sql.replace("extract(dayofweek from schedule_pattern_date)", "(extract(dayofweek from schedule_pattern_date) + 1)")
    sql = sql.replace("format('%06d:%s:%08d'", "printf('%06d:%s:%08d'").replace("'\\n'", "chr(10)")
    # BigQuery regexp_extract defaults to capture group 1; DuckDB defaults to the full match.
    sql = sql.replace("(?:^|:)(Pc|Pt|Sb|Nd)[A-Za-z]*$')", "(?:^|:)(Pc|Pt|Sb|Nd)[A-Za-z]*$', 1)")
    sql = sql.replace("^(\\d{4}-\\d{2}-\\d{2}):')", "^(\\d{4}-\\d{2}-\\d{2}):', 1)")
    with duckdb.connect() as connection:
        connection.execute("create macro to_hex(value) as value")
        connection.execute("""create table stg_gtfs__calendar_dates(
            service_id varchar, service_date date, gtfs_snapshot_id varchar);
            insert into stg_gtfs__calendar_dates values
            ('Pc', '2026-01-01', 'pin'), ('Pc', '2026-01-02', 'pin'),
            ('Pc', '2026-01-01', 'other'), ('Pc', '2026-01-02', 'other');
            create table stg_gtfs__trips(gtfs_snapshot_id varchar, line varchar, direction_id bigint,
                trip_id varchar, service_id varchar, trip_headsign varchar, shape_id varchar);
            insert into stg_gtfs__trips values
            ('pin', '187', 0, 'trip', 'Pc', 'headsign', 'shape'),
            ('other', '999', 0, 'trip', 'Pc', 'headsign', 'shape');
            create table stg_gtfs__stop_times(gtfs_snapshot_id varchar, trip_id varchar,
                stop_sequence bigint, stop_id varchar, arrival_time_seconds bigint, departure_time_seconds bigint);
            insert into stg_gtfs__stop_times values
            ('pin', 'trip', 2, 'B', 3700, 3700), ('pin', 'trip', 1, 'A', 3600, 3600),
            ('other', 'trip', 1, 'UNRELATED', 7200, 7200)""")
        rows = connection.execute(sql).fetchall()
    # The D-1 trip ends at 01:01:40 on D-1: it does not overlap D, but belongs in history.
    signature = "000001:A:00003600 | 000002:B:00003700"
    fingerprint = hashlib.md5(f"{signature}\n{signature}".encode(), usedforsecurity=False).hexdigest()
    assert sorted(rows, key=lambda row: (row[0], row[-1])) == [
        (datetime.date(2026, 1, 2), "pin", "187", 0, "weekday", fingerprint, 2, False),
        (datetime.date(2026, 1, 2), "pin", None, None, None, None, None, True),
        (datetime.date(2026, 1, 3), None, None, None, None, None, None, True),
    ]


def test_noop_ledger_has_no_raw_expansion_and_markers_are_excluded_from_versions() -> None:
    env = Environment(extensions=["jinja2.ext.do"], autoescape=False)  # noqa: S701 - SQL, not HTML.
    source = (DBT / "models/intermediate/int_schedule_fingerprint_daily.sql").read_text()
    config = Mock(return_value="")
    sql = env.from_string(source).render(
        ref=lambda name: name,
        schedule_ledger_plan=list,
        config=config,
        tojson=json.dumps,
    )
    # BigQuery rejects a WHERE clause on a SELECT without FROM, even when false.
    assert "from unnest(cast([] as array<int64>)) as empty_relation\nwhere false" in sql
    assert "trip_history as" not in sql
    assert "from stg_gtfs" not in sql
    assert config.call_args.kwargs["full_refresh"] is False
    assert config.call_args.kwargs["incremental_strategy"] == "insert_overwrite"
    assert "partitions" not in config.call_args.kwargs  # Inferred from markers in the temp table.
    versions = (DBT / "models/intermediate/int_schedule_version.sql").read_text()
    assert "where not is_date_marker" in versions
    assert "int_gtfs_trip_schedule_history" not in versions
    dim = (DBT / "models/marts/dim_schedule_version.sql").read_text()
    assert 'pre_hook="{{ schedule_ledger_assert_complete() }}"' in dim  # Deferred refs, not parse-time placeholders.


def test_estimator_dry_runs_source_instead_of_omitting_temp_processing(monkeypatch: pytest.MonkeyPatch) -> None:
    spec = importlib.util.spec_from_file_location("estimate_schedule", DBT / "tools/estimate_schedule.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setitem(
        sys.modules, "google.cloud", SimpleNamespace(bigquery=SimpleNamespace(QueryJobConfig=SimpleNamespace))
    )
    client = Mock()
    client.query.return_value.total_bytes_processed = 1024
    sql = '-- schedule_ledger_plan: [{"processing_date":"2026-01-01", "gtfs_snapshot_id":"s"}]\nselect * from raw_expansion'
    merge = module.replacement_query(sql, "`project.dataset.ledger`")
    assert "select * from raw_expansion" in merge
    assert "dest.processing_date in (date('2026-01-01'))" in merge
    assert "when not matched then insert" in merge
    assert module.dry_run(client, merge, "europe-north1")["bytes_processed"] == 1024
    config = client.query.call_args.kwargs["job_config"]
    assert config.dry_run is True
    assert config.use_query_cache is False
    client.query.return_value.result.assert_not_called()
    client.query.return_value.total_bytes_processed = None
    with pytest.raises(RuntimeError, match="no byte estimate"):
        module.dry_run(client, merge, "europe-north1")


def test_version_windows_ids_missing_lines_and_out_of_order_correction() -> None:
    connection = duckdb.connect()
    connection.execute("create macro to_hex(value) as value")
    connection.execute("""create table int_schedule_fingerprint_daily(
        processing_date date, gtfs_snapshot_id varchar, line varchar, direction_id bigint,
        schedule_day_type varchar, timetable_fingerprint varchar, scheduled_trip_count bigint,
        is_date_marker boolean)""")
    connection.execute("""insert into int_schedule_fingerprint_daily values
        ('2026-01-06', 's6', '187', 0, 'weekday', 'A', 3, false),
        ('2026-01-02', 's2', '187', 0, 'weekday', 'A', 2, false),
        ('2026-01-03', 'empty', null, null, null, null, null, true),
        ('2026-01-04', 's4', '187', 0, 'weekday', 'B', 2, false)""")
    # Adapt only dialect spellings; execute the production window/grouping SQL.
    env = Environment(autoescape=False)  # noqa: S701 - SQL, not HTML.
    sql = env.from_string((DBT / "models/intermediate/int_schedule_version.sql").read_text()).render(
        ref=lambda name: name
    )
    sql = re.sub(
        r"array_agg\(gtfs_snapshot_id order by (.*?) limit 1\)\[offset\(0\)\]",
        r"first(gtfs_snapshot_id order by \1)",
        sql,
    )
    sql = re.sub(
        r"date_sub\(\s*(lead\(valid_from_date\) over \([^\n]+\)),\s*interval 1 day\s*\)",
        r"cast(\1 - interval '1 day' as date)",
        sql,
    )
    sql = re.sub(
        r"to_json_string\(struct\((.*?)\)\)",
        lambda match: "to_json(struct_pack(" + re.sub(r"(\w+) as (\w+)", r"\2 := \1", match[1]) + "))",
        sql,
        flags=re.DOTALL,
    )

    def identifier(fingerprint: str, day: str) -> str:
        payload = json.dumps(
            {
                "line": "187",
                "direction_id": 0,
                "schedule_day_type": "weekday",
                "timetable_fingerprint": fingerprint,
                "valid_from_date": day,
            },
            separators=(",", ":"),
        )
        return hashlib.md5(payload.encode(), usedforsecurity=False).hexdigest()

    rows = sorted(connection.execute(sql).fetchall(), key=lambda row: row[5])
    assert [row[0] for row in rows] == [
        identifier("A", "2026-01-02"),
        identifier("B", "2026-01-04"),
        identifier("A", "2026-01-06"),
    ]
    assert [row[6] for row in rows] == [datetime.date(2026, 1, 3), datetime.date(2026, 1, 5), None]
    # An earlier correction rejoins the ranges, using the same original start-date ID.
    connection.execute(
        "update int_schedule_fingerprint_daily set timetable_fingerprint='A', gtfs_snapshot_id='corrected' where processing_date=date '2026-01-04'"
    )
    corrected = connection.execute(sql).fetchall()
    assert len(corrected) == 1
    assert corrected[0][0] == identifier("A", "2026-01-02")
    assert corrected[0][6:] == (None, "s2", "s6", datetime.date(2026, 1, 2), datetime.date(2026, 1, 6), 3)
    connection.close()
