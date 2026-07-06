#!/usr/bin/env bash
set -euo pipefail

repo_dir="${1:-/home/ubuntu/ztm-pipeline}"
branch="${2:-master}"
airflow_container_prefix="${3:-l11t1z4fvjlunhohvau5w8gc}"

if [[ ! -d "$repo_dir/.git" ]]; then
  echo "Repo not found: $repo_dir" >&2
  exit 1
fi

current_branch="$(git -C "$repo_dir" rev-parse --abbrev-ref HEAD)"
if [[ "$current_branch" != "$branch" ]]; then
  echo "Expected $repo_dir on branch $branch, got $current_branch" >&2
  exit 1
fi

tracked_changes="$(git -C "$repo_dir" status --porcelain --untracked-files=no)"
if [[ -n "$tracked_changes" ]]; then
  echo "Tracked worktree changes on VPS; refusing to deploy:" >&2
  printf '%s\n' "$tracked_changes" >&2
  exit 1
fi

git -C "$repo_dir" pull --ff-only origin "$branch"

airflow_container="$(sudo -n docker ps --format '{{.Names}}' | grep "^${airflow_container_prefix}-" | head -n1 || true)"
if [[ -z "$airflow_container" ]]; then
  echo "Airflow container not found with prefix: $airflow_container_prefix" >&2
  exit 1
fi

echo "Airflow container: $airflow_container"

sudo -n docker exec -i "$airflow_container" python <<'PY'
from pathlib import Path

for path in (
    Path("/opt/airflow/dags/ztm_airflow_common.py"),
    Path("/opt/airflow/dags/dag_gtfs_poll.py"),
    Path("/opt/airflow/dags/dag_gtfs_load.py"),
    Path("/opt/airflow/dags/dag_daily_gps.py"),
    Path("/opt/airflow/dags/dag_serving_export.py"),
):
    compile(path.read_text(), str(path), "exec")
PY

sudo -n docker exec "$airflow_container" airflow dags list --output table
sudo -n docker exec "$airflow_container" dbt parse --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
