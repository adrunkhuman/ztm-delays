FROM apache/airflow:3.2.2@sha256:2b2755ecf24bcdfbef9c29295c3f756b2c46bddb85c5bec1f3fe835041e5ea86

# LightGBM (planner) needs the OpenMP runtime, which the base image lacks.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Keep pip from upgrading or downgrading Airflow while installing its extensions.
RUN pip install --no-cache-dir \
    apache-airflow==3.2.2 \
    dbt-core==1.11.11 \
    dbt-bigquery==1.11.3 \
    apache-airflow-providers-google==22.0.0 \
    duckdb==1.5.5 \
    uv==0.11.16

COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/
COPY --chown=airflow:root dbt/ /opt/airflow/dbt/
COPY --chown=airflow:root matcher/ /opt/airflow/matcher/
COPY --chown=airflow:root planner/ /opt/airflow/planner/

# Keep matcher dependencies separate from Airflow, with no runtime resolution/downloads.
ENV UV_PROJECT_ENVIRONMENT=/opt/airflow/matcher-venv \
    UV_PYTHON_DOWNLOADS=never \
    MATCHER_COMMAND="uv run --no-sync --project /opt/airflow/matcher ztm-matcher" \
    PLANNER_COMMAND="/opt/airflow/planner-venv/bin/ztm-planner"
# The planner gets its own environment too (LightGBM, its own DuckDB); its command is the venv's script.
RUN uv sync --locked --no-dev --no-editable --no-cache --python /usr/local/bin/python \
        --project /opt/airflow/matcher \
    && UV_PROJECT_ENVIRONMENT=/opt/airflow/planner-venv uv sync --locked --no-dev --no-editable --no-cache \
        --python /usr/local/bin/python --project /opt/airflow/planner \
    && mkdir -p /opt/airflow/dbt/target /opt/airflow/dbt/logs \
        /opt/airflow/matcher-work /opt/airflow/planner-work /opt/airflow/serving /opt/airflow/auth \
    && chmod -R g+rwX /opt/airflow/dbt /opt/airflow/matcher-work /opt/airflow/planner-work \
        /opt/airflow/serving /opt/airflow/auth

CMD ["airflow", "standalone"]
