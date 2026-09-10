"""Offline CI and repository image contracts; no deployment access."""

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    ("job", "step_name", "packages"),
    [
        ("airflow", "Lint Airflow", ["ruff"]),
        ("airflow", "Test Airflow DAG boundaries", ["pytest", "tzdata", "duckdb", "pyarrow", "jinja2"]),
        ("dbt", "Install dbt", ["dbt-core", "dbt-bigquery"]),
        ("deployment-checks", "Test deployment", ["pyyaml", "pytest"]),
        ("deployment-checks", "Lint deployment tests", ["ruff"]),
        ("deployment-checks", "Compile pinned-date and no-change ledger plans offline", ["dbt-core", "dbt-bigquery"]),
    ],
)
def test_standalone_ci_dependencies_are_pinned(job, step_name, packages):
    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    step = next(step for step in ci["jobs"][job]["steps"] if step.get("name") == step_name)
    tokens = step["run"].split()
    for package in packages:
        versions = [token.removeprefix(f"{package}==") for token in tokens if token.startswith(f"{package}==")]
        assert versions and all(part.isdigit() for version in versions for part in version.split(".")), (
            step_name, package,
        )


def test_workflow_wiring():
    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    cloud_checks = ci["jobs"]["dbt"]
    assert cloud_checks["if"] == (
        "github.event_name == 'push' || "
        "(github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository)"
    )
    offline_checks = ci["jobs"]["deployment-checks"]
    assert "if" not in offline_checks
    commands = [step.get("run", "") for step in offline_checks["steps"]]
    compile_index = next(i for i, command in enumerate(commands) if "compile_schedule.py" in command)
    assert commands.index("python .github/scripts/check_raw_gps_contract.py") > compile_index
    assert "deploy-vps" not in ci["jobs"]
    assert not (ROOT / ".github/workflows/deploy-vps.yml").exists()
    assert not (ROOT / ".github/scripts/deploy_vps.sh").exists()
    image = ci["jobs"]["airflow-image"]
    assert "if" not in image
    build, smoke = [step["run"] for step in image["steps"] if "run" in step]
    assert "--platform linux/amd64 -f airflow/Dockerfile -t ztm-airflow:ci ." in build
    assert "--network none --entrypoint python" in smoke
    assert "smoke_airflow_image.py" in smoke
    assert "secrets." not in str(image)


def test_image_runtime_contract():
    dockerfile = (ROOT / "airflow/Dockerfile").read_text()
    assert 'CMD ["airflow", "standalone"]' in dockerfile
    for source, destination in (("airflow/dags", "dags"), ("dbt", "dbt"), ("matcher", "matcher")):
        assert f"COPY --chown=airflow:root {source}/ /opt/airflow/{destination}/" in dockerfile
    assert "uv sync --locked --no-dev --no-editable" in dockerfile
    assert "UV_PROJECT_ENVIRONMENT=/opt/airflow/matcher-venv" in dockerfile
    assert 'MATCHER_COMMAND="uv run --no-sync --project /opt/airflow/matcher ztm-matcher"' in dockerfile
    assert "uv==0.11.16" in dockerfile
    assert "VOLUME" not in dockerfile  # No anonymous volumes masking code or runtime storage.
    ignore = (ROOT / "airflow/Dockerfile.dockerignore").read_text().splitlines()
    rules = [line for line in ignore if line and not line.startswith("#")]
    assert rules[0] == "**"
    assert "!matcher/uv.lock" in rules
    assert "!dbt/profiles.yml" in rules
    assert not any(rule in rules for rule in ("!dbt/**", "!matcher/**", "!airflow/**"))
