# Build with: docker build -f frontend/Dockerfile -t ztm-frontend .
FROM rust:1.98.0-slim-bookworm AS rust-toolchain

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder
COPY --from=rust-toolchain /usr/local/cargo /usr/local/cargo
COPY --from=rust-toolchain /usr/local/rustup /usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH="/usr/local/cargo/bin:$PATH" \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN apt-get update \
    && apt-get install --no-install-recommends -y gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app/frontend
COPY routing /app/routing
COPY frontend/pyproject.toml frontend/uv.lock ./
COPY frontend/ztm_frontend ./ztm_frontend
RUN uv sync --locked --no-dev --no-editable \
    && /app/.venv/bin/python -c "import ztm_routing._native; import ztm_frontend.journey"

FROM python:3.13-slim-bookworm

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH" \
    ZTM_DUCKDB_PATH=/app/serving/ztm.duckdb

# DuckDB's wheel still needs the C++ runtime; this does not install a compiler.
RUN apt-get update \
    && apt-get install --no-install-recommends -y libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Installed wheels include templates/static assets. No sources, Rust, or build tools.
COPY --from=builder /app/.venv /app/.venv

USER 10001
EXPOSE 5000
CMD ["waitress-serve", "--host=0.0.0.0", "--port=5000", "--threads=4", "ztm_frontend.app:app"]
