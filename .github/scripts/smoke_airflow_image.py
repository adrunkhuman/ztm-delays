"""Credential-free image checks: no Airflow startup, database, or cloud calls."""

import importlib
import importlib.metadata
import os
import shlex
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch


def run(*args, **kwargs):
    subprocess.run(args, check=True, **kwargs)


def main():
    home = Path("/opt/airflow")
    for package, version in {
        "apache-airflow": "3.2.2",
        "dbt-core": "1.11.11",
        "dbt-bigquery": "1.11.3",
        "apache-airflow-providers-google": "22.0.0",
        "duckdb": "1.5.5",
        "uv": "0.11.16",
    }.items():
        assert importlib.metadata.version(package) == version, package
    for module in ("airflow", "google.cloud.bigquery", "google.cloud.storage", "duckdb", "pyarrow"):
        importlib.import_module(module)
    run("python", "-m", "pip", "check")
    dags = list((home / "dags").glob("*.py"))
    assert dags
    for dag in dags:
        compile(dag.read_text(), str(dag), "exec")
    for directory in ("dbt/target", "dbt/logs", "matcher-work", "planner-work", "serving", "logs", "auth"):
        probe = home / directory / ".image-smoke"
        probe.write_text("writable")
        probe.unlink()
    for directory in ("dags", "dbt/models", "matcher/src/ztm_matcher", "planner/src/ztm_planner"):
        for filename in (".env.image-smoke", "image-smoke.parquet"):
            assert not (home / directory / filename).exists(), (directory, filename)
    assert (home / "matcher/uv.lock").is_file()
    assert (home / "matcher/src/ztm_matcher/cli.py").is_file()
    assert (home / "planner/uv.lock").is_file()
    assert not (home / "planner/tests").exists()
    for asset in ("route_map_sql/segment_statistics.sql", "route_map_sql/pooled_routes.sql",
                  "route_map_assets/mini-background-bus.svg", "route_map_assets/mini-background-tram.svg"):
        assert (home / "dags" / asset).is_file(), asset
    sys.path.insert(0, str(home / "dags"))
    with patch.dict(os.environ, {
        "MATCHER_ENABLED": "true",
        "BIGQUERY_MATCHER_STAGING_DATASET": "image_smoke_stage",
        "BIGQUERY_MATCHER_INPUT_DATASET": "image_smoke_input",
    }):
        from ztm_matcher import MatcherConfig

        MatcherConfig.from_env().validate()
    # Importing is not enough: the scheduler also serializes each DAG and rejects some valid Python.
    from airflow.dag_processing.dagbag import DagBag
    from airflow.serialization.serialized_objects import DagSerialization

    bag = DagBag(str(home / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    assert bag.dags
    for dag in bag.dags.values():
        DagSerialization.to_dict(dag)
    run(*shlex.split(os.environ["MATCHER_COMMAND"]), "--help")
    run(
        str(home / "matcher-venv/bin/python"), "-c",
        "import sys, duckdb, numpy, pyarrow, pytz, tzdata, ztm_matcher; "
        "assert sys.prefix == '/opt/airflow/matcher-venv'; "
        "import importlib.util; assert importlib.util.find_spec('airflow') is None",
    )
    run("uv", "pip", "check", "--python", str(home / "matcher-venv/bin/python"))
    run(*shlex.split(os.environ["PLANNER_COMMAND"]), "--help")
    run(
        str(home / "planner-venv/bin/python"), "-c",
        "import sys, duckdb, lightgbm, numpy, osmium, pyarrow, scipy, ztm_planner.footpaths; "
        "assert sys.prefix == '/opt/airflow/planner-venv'; "
        "import importlib.util; assert importlib.util.find_spec('airflow') is None",
    )
    run("uv", "pip", "check", "--python", str(home / "planner-venv/bin/python"))
    run(
        "dbt", "--no-send-anonymous-usage-stats", "parse", "--no-partial-parse", "--profiles-dir", ".",
        cwd=home / "dbt",
    )


if __name__ == "__main__":
    main()
