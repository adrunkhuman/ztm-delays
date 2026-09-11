# Local checks

Python 3.13 and uv are used throughout. Components have separate environments; there is no root Python package or local full-stack deployment.

## Offline pipeline test

From the repository root:

```sh
uv sync --locked --project integration_tests
uv run --project integration_tests pytest -q integration_tests/test_offline_pipeline.py
```

A synthetic overnight trip passes through the real matcher, serving exporter, semantic validator, and frontend query layer. A small row adapter replaces dbt; this does not exercise BigQuery or the scheduler. No cloud credentials or live data are needed. Dependency installation needs network access; CI disables networking for the test itself.

## Component checks

Run from `poller/`, `matcher/`, or `frontend/`:

```sh
uv sync --locked
uv run ruff check .
uv run pytest
```

The matcher also runs `uv run ty check`. Airflow boundary tests and image checks are in its [README](../airflow/README.md#checks); [dbt](../dbt/README.md#checks-and-estimates) has an offline schedule compiler and credentialed warehouse checks.

[CI](../.github/workflows/ci.yml) additionally checks container startup, deployment configuration, and the shared raw GPS schema. It validates changes but does not deploy them.

Running the frontend requires a serving export. Running the poller contacts the city API. The full warehouse path requires GCP credentials and can incur charges; component READMEs distinguish these from offline checks.
