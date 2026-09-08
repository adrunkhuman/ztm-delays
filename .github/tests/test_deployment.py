"""Offline deployment tests; git and sudo are substituted, never run live."""

import os
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SHA = "a" * 40


@pytest.mark.parametrize(
    ("scenario", "sha", "message", "branch"),
    [
        ("ok", "master", "full lowercase", "master"),
        ("ok", SHA, "branch master", "other"),
        ("branch", SHA, "got other", "master"),
        ("dirty", SHA, "Tracked worktree", "master"),
        ("wrong-sha", SHA, "does not resolve", "master"),
        ("off-master", SHA, "not on origin/master", "master"),
        ("stale", SHA, "non-fast-forward", "master"),
        ("equal", SHA, "Already at vetted SHA", "master"),
        ("ok", SHA, "Airflow container not found", "master"),
    ],
)
def test_remote_revision(tmp_path, scenario, sha, message, branch):
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    log = tmp_path / "commands"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = '''#!/bin/bash
printf '%s %s\\n' "${0##*/}" "$*" >> "$COMMAND_LOG"
if [[ "${0##*/}" == sudo ]]; then exit 0; fi
shift 2
case "$*" in
  "rev-parse --abbrev-ref HEAD") [[ "$SCENARIO" == branch ]] && echo other || echo master ;;
  "status --porcelain --untracked-files=no") [[ "$SCENARIO" != dirty ]] || echo ' M tracked' ;;
  "rev-parse --verify "*) [[ "$SCENARIO" == wrong-sha ]] && echo bad || echo "$SHA" ;;
  "merge-base --is-ancestor $SHA origin/master") [[ "$SCENARIO" != off-master ]] || exit 1 ;;
  "merge-base --is-ancestor HEAD $SHA") [[ "$SCENARIO" != stale ]] || exit 1 ;;
  "rev-parse HEAD") [[ "$SCENARIO" == equal ]] && echo "$SHA" || echo old ;;
esac
exit 0
'''
    for name in ("git", "sudo"):
        executable = bin_dir / name
        executable.write_text(fake)
        executable.chmod(0o755)
    result = subprocess.run(
        ["bash", str(ROOT / ".github/scripts/deploy_vps.sh"), str(repo), branch, "prefix", sha],
        env={**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
             "COMMAND_LOG": str(log), "SCENARIO": scenario, "SHA": SHA},
        capture_output=True, text=True, check=False,
    )
    assert result.returncode != 0  # Successful revision checks reach the absent mock container.
    assert message in result.stdout + result.stderr
    commands = log.read_text() if log.exists() else ""
    if sha != SHA or branch != "master":
        assert not commands
        return
    if scenario not in ("ok", "equal") or sha != SHA:
        assert "sudo" not in commands
        assert "merge --ff-only" not in commands
    if scenario in ("branch", "dirty"):
        assert "fetch" not in commands
    if scenario == "equal":
        assert "merge --ff-only" not in commands
    if scenario == "ok" and sha == SHA:
        assert commands.index("merge-base --is-ancestor HEAD") < commands.index(f"merge --ff-only {SHA}")
        assert commands.index(f"merge --ff-only {SHA}") < commands.index("sudo")
        assert "merge --ff-only origin/master" not in commands
        assert "pull " not in commands


def test_workflow_wiring():
    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    manual = yaml.safe_load((ROOT / ".github/workflows/deploy-vps.yml").read_text())
    auto = ci["jobs"]["deploy-vps"]
    assert auto["if"] == "github.event_name == 'push' && github.ref == 'refs/heads/master'"
    assert manual["jobs"]["deploy"]["if"] == "github.ref == 'refs/heads/master'"
    for job in (auto, manual["jobs"]["deploy"]):
        [deploy] = [step for step in job["steps"] if ".github/scripts/deploy_vps.sh" in step.get("run", "")]
        assert deploy["env"]["DEPLOY_SHA"] == "${{ github.sha }}"
        assert "'$DEPLOY_SHA'" in deploy["run"]
    assert set(auto["needs"]) == {"poller", "airflow", "matcher", "frontend", "dbt", "deployment-checks"}
    [compile_step] = [
        step["run"] for step in ci["jobs"]["deployment-checks"]["steps"]
        if "dbt/tools/compile_schedule.py" in step.get("run", "")
    ]
    assert "dbt-core==1.11.11" in compile_step
    assert "dbt-bigquery==1.11.3" in compile_step
    assert "dbt/tools/compile_schedule.py" in compile_step
