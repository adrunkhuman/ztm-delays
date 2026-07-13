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

git -C "$repo_dir" pull --ff-only --quiet origin "$branch"

airflow_container="$(sudo -n docker ps --format '{{.Names}}' | grep "^${airflow_container_prefix}-" | head -n1 || true)"
if [[ -z "$airflow_container" ]]; then
  echo "Airflow container not found with prefix: $airflow_container_prefix" >&2
  exit 1
fi

echo "Airflow container found"

host_matcher_hash="$(sha256sum "$repo_dir/matcher/src/ztm_matcher/runtime.py" | cut -d' ' -f1)"
container_matcher_hash="$(sudo -n docker exec "$airflow_container" sha256sum /opt/airflow/matcher/src/ztm_matcher/runtime.py | cut -d' ' -f1)"
if [[ "$host_matcher_hash" != "$container_matcher_hash" ]]; then
  echo "Airflow matcher bind is stale; redeploy Airflow in Coolify and rerun this workflow" >&2
  exit 1
fi

sudo -n docker exec -i "$airflow_container" python <<'PY'
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, "/opt/airflow/dags")
from ztm_matcher import MatcherConfig

for path in (
    Path("/opt/airflow/dags/ztm_airflow_common.py"),
    Path("/opt/airflow/dags/dag_gtfs_poll.py"),
    Path("/opt/airflow/dags/dag_gtfs_load.py"),
    Path("/opt/airflow/dags/dag_daily_gps.py"),
    Path("/opt/airflow/dags/dag_serving_export.py"),
    Path("/opt/airflow/dags/ztm_matcher.py"),
):
    compile(path.read_text(), str(path), "exec")

matcher = MatcherConfig.from_env()
matcher.validate()
if not matcher.enabled:
    raise RuntimeError("Matcher requires MATCHER_ENABLED=true")
if not Path("/opt/airflow/matcher").is_dir():
    raise RuntimeError("Matcher bind is missing")
uv_environment_value = os.environ.get("UV_PROJECT_ENVIRONMENT", "")
if not uv_environment_value:
    raise RuntimeError("Matcher requires UV_PROJECT_ENVIRONMENT")
uv_environment = Path(uv_environment_value)
if not uv_environment.is_absolute() or uv_environment == Path("/opt/airflow/matcher") or Path("/opt/airflow/matcher") in uv_environment.parents:
    raise RuntimeError("UV_PROJECT_ENVIRONMENT must be an absolute writable path outside the matcher bind")
uv_environment.mkdir(parents=True, exist_ok=True)
if not os.access(uv_environment, os.W_OK):
    raise RuntimeError(f"UV_PROJECT_ENVIRONMENT is not writable: {uv_environment}")
matcher.workspace_root.mkdir(parents=True, exist_ok=True)
if not os.access(matcher.workspace_root, os.W_OK):
    raise RuntimeError(f"Matcher workspace is not writable: {matcher.workspace_root}")
subprocess.run([*matcher.command, "--help"], cwd=matcher.project_dir, check=True, stdout=subprocess.DEVNULL)
PY

expected_dags=(
  dag_gtfs_poll
  dag_gtfs_load
  dag_gps_raw_load
  dag_daily_gps
  dag_serving_export
)
dag_list="$(sudo -n docker exec "$airflow_container" airflow dags list --output plain)"
for dag_id in "${expected_dags[@]}"; do
  if ! grep -q "^${dag_id}[[:space:]]" <<<"$dag_list"; then
    echo "Missing Airflow DAG: $dag_id" >&2
    exit 1
  fi
done
echo "Airflow DAG smoke check passed"

sudo -n docker exec "$airflow_container" dbt parse --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
