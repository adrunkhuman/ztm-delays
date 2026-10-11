FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

RUN apt-get update \
    && apt-get install --no-install-recommends -y g++ \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY setup.py MANIFEST.in _ztm_routing.pyi ./
COPY native ./native
COPY ztm_frontend ./ztm_frontend
RUN uv sync --frozen --no-dev --no-editable \
    && .venv/bin/python -c "import _ztm_routing; import ztm_frontend.journey"

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH" \
    ZTM_DUCKDB_PATH=/app/serving/ztm.duckdb

RUN apt-get update \
    && apt-get install --no-install-recommends -y libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Runtime is wheel-only: no source tree, build tools, or editable-install paths.
COPY --from=builder /app/.venv /app/.venv

USER 10001
EXPOSE 5000
CMD ["waitress-serve", "--host=0.0.0.0", "--port=5000", "--threads=4", "ztm_frontend.app:app"]
