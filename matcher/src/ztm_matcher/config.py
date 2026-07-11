"""Run configuration."""

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

from ztm_matcher.errors import fail


@dataclass(frozen=True)
class RunConfig:
    """Explicit inputs, output, and resource limits for one processing day."""

    processing_date: date
    snapshot_id: str
    gps_root: Path
    gtfs_zip: Path
    output_dir: Path
    metrics_json: Path
    threads: int = 2
    memory_limit: str = "512MB"
    temp_limit: str = "20GB"
    max_vehicle_rows: int = 1_000_000
    allow_missing_hours: bool = False

    def as_manifest(self) -> dict[str, object]:
        """Return JSON-safe config values."""
        return {key: str(value) if isinstance(value, (date, Path)) else value for key, value in asdict(self).items()}


def parse_date(value: str) -> date:
    """Parse strict ISO date input."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise fail("invalid_data", f"processing date must be YYYY-MM-DD: {value}", 12) from exc
