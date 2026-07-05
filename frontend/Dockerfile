FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH" \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    ZTM_DUCKDB_PATH=/app/serving/ztm.duckdb

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY ztm_frontend ./ztm_frontend

USER 10001

EXPOSE 5000

CMD ["waitress-serve", "--host=0.0.0.0", "--port=5000", "ztm_frontend.app:app"]
