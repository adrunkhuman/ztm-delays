"""Credential-free poller image checks; do not start the poller or Tailscale."""

import importlib
import shutil
import subprocess
from pathlib import Path


def run(*args: str) -> None:
    subprocess.run(args, check=True)


def main() -> None:
    for module in ("google.cloud.storage", "pyarrow", "requests", "socks", "tzdata"):
        importlib.import_module(module)
    run("uv", "pip", "check", "--python", "/app/.venv/bin/python")

    for command in ("tailscale", "tailscaled"):
        assert shutil.which(command), command
    for script in (Path("/app/entrypoint.sh"), Path("/app/healthcheck.sh")):
        assert script.is_file() and script.stat().st_mode & 0o111, script
        run("sh", "-n", str(script))

    source = Path("/app/poller.py")
    assert source.is_file()
    compile(source.read_text(), str(source), "exec")


if __name__ == "__main__":
    main()
