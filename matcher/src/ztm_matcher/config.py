"""Run configuration."""

from dataclasses import asdict, dataclass
from datetime import date, timedelta
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
    memory_limit: str = "384MB"
    temp_limit: str = "20GB"
    max_vehicle_rows: int = 1_000_000
    allow_missing_hours: bool = False
    alignment_workers: int = 1
    diagnostic_line: str | None = None
    diagnostic_vehicle_number: str | None = None
    diagnostic_trip_id: str | None = None
    include_prior_gps: bool = True

    @property
    def input_dates(self) -> tuple[date, ...]:
        """Return the explicit Warsaw GPS partitions consumed by this run."""
        return (
            (self.processing_date - timedelta(days=1), self.processing_date)
            if self.include_prior_gps
            else (self.processing_date,)
        )

    def as_manifest(self) -> dict[str, object]:
        """Return JSON-safe config values."""
        return {
            key: str(value) if isinstance(value, (date, Path)) else value
            for key, value in asdict(self).items()
            if value is not None
        }


def parse_date(value: str) -> date:
    """Parse strict ISO date input."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise fail("invalid_data", f"processing date must be YYYY-MM-DD: {value}", 12) from exc
