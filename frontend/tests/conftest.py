from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path(__file__).resolve().parents[1]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


@pytest.fixture(autouse=True)
def _status_feature_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pages must not reach for GCS unless a test opts in with a bucket and a fake client."""
    from ztm_frontend import live_status  # noqa: PLC0415 - needs the path set above

    monkeypatch.delenv("ZTM_STATUS_GCS_BUCKET", raising=False)
    live_status.clear_cache()
