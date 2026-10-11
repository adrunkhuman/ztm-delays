# Local checks

Python 3.13 and uv are used throughout. Components have separate environments; there is no root Python package or local full-stack deployment. Frontend builds, including the pipeline test, also need Rust 1.98.0 and a C linker; Rust is not required in the deployed runtime.

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

## Routing engine

The frontend owns artifact loading and itinerary rendering. Its `ztm_frontend/routing.py` facade copies columns and endpoint metadata into the independent [Rust engine](../routing/README.md), then materializes only returned paths as Python labels. Rust searches with the GIL detached and no borrowed Python memory or Python callbacks. Immutable networks can be shared across requests; permission caches are synchronized across a query's windows, and a mutable window cannot run concurrently with itself.

| Source | Responsibility |
| --- | --- |
| `frontend/ztm_frontend/journey.py` | Network loading, endpoints, search windows, and itinerary rendering. |
| `frontend/ztm_frontend/routing.py` | Primitive engine boundary and Python label materialization. |
| `routing/src/` | Rust search, validation, native ownership, and PyO3 bindings. |
| `routing/python/ztm_routing/` | Importable engine package and typing stubs. |
| `frontend/tests/native/` | Boundary/concurrency checks and the unchanged test-only Python oracle. |

From the repository root, after installing the pinned toolchain:

```sh
rustup toolchain install 1.98.0 --profile minimal --component rustfmt --component clippy
cargo +1.98.0 fmt --manifest-path routing/Cargo.toml --all -- --check
cargo +1.98.0 clippy --manifest-path routing/Cargo.toml --locked --all-targets -- -D warnings
cargo +1.98.0 test --manifest-path routing/Cargo.toml --locked
RUSTUP_TOOLCHAIN=1.98.0 uv sync --locked --project frontend
uv run --project frontend pytest frontend/tests/native
# The image needs both frontend/ and routing/ in its context.
docker build -f frontend/Dockerfile -t ztm-frontend .
```

`ztm-routing` is a non-editable local dependency at `../routing`. Its package cache keys track `pyproject.toml`, `Cargo.toml`, `Cargo.lock`, Rust sources, and Python bindings/stubs; tracking only frontend metadata does not invalidate the dependency wheel. See [uv's cache rules](https://docs.astral.sh/uv/concepts/cache/#dependency-caching). After changing dependency metadata, update `frontend/uv.lock` with `uv lock --project frontend`; ordinary checks and image builds use the lock without updating it.

[CI](../.github/workflows/ci.yml) additionally checks Rust formatting, lint and tests, container startup, deployment configuration, and the shared raw GPS schema. It validates changes but does not deploy them.

Running the frontend requires a serving export. Running the poller contacts the city API. The full warehouse path requires GCP credentials and can incur charges; component READMEs distinguish these from offline checks.
