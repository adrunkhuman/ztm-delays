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
    for directory in ("dbt/target", "dbt/logs", "matcher-work", "serving", "logs", "auth"):
        probe = home / directory / ".image-smoke"
        probe.write_text("writable")
        probe.unlink()
    for directory in ("dags", "dbt/models", "matcher/src/ztm_matcher"):
        for filename in (".env.image-smoke", "image-smoke.parquet"):
            assert not (home / directory / filename).exists(), (directory, filename)
    assert (home / "matcher/uv.lock").is_file()
    assert (home / "matcher/src/ztm_matcher/cli.py").is_file()
    sys.path.insert(0, str(home / "dags"))
    with patch.dict(os.environ, {
        "MATCHER_ENABLED": "true",
        "BIGQUERY_MATCHER_STAGING_DATASET": "image_smoke_stage",
        "BIGQUERY_MATCHER_INPUT_DATASET": "image_smoke_input",
    }):
        from ztm_matcher import MatcherConfig

        MatcherConfig.from_env().validate()
    run(*shlex.split(os.environ["MATCHER_COMMAND"]), "--help")
    run(
        str(home / "matcher-venv/bin/python"), "-c",
        "import sys, duckdb, numpy, pyarrow, pytz, tzdata, ztm_matcher; "
        "assert sys.prefix == '/opt/airflow/matcher-venv'; "
        "import importlib.util; assert importlib.util.find_spec('airflow') is None",
    )
    run("uv", "pip", "check", "--python", str(home / "matcher-venv/bin/python"))
    run(
        "dbt", "--no-send-anonymous-usage-stats", "parse", "--no-partial-parse", "--profiles-dir", ".",
        cwd=home / "dbt",
    )


if __name__ == "__main__":
    main()
