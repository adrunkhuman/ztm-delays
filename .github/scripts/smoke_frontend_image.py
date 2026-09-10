"""Create a tiny serving fixture and check the image's default HTTP server."""

import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import duckdb

DB_PATH = Path("/app/serving/ztm.duckdb")


def create_fixture() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(DB_PATH)) as connection:
        connection.execute(
            """
            create table export_metadata as select
                'image-smoke' as export_id,
                'alpha-1' as export_version,
                'current_pipeline_provisional' as source_mode,
                timestamp '2026-07-14 03:12:00' as exported_at,
                1::ubigint as source_row_count,
                100::ubigint as duckdb_file_size_bytes;

            create table mart_pipeline_status as select
                date '2026-07-13' as service_date,
                'bus' as mode,
                1.0 as service_coverage_ratio,
                1.0 as completeness_ratio,
                10 as trips_complete,
                2 as trips_partial,
                1 as trips_broken;

            create table mart_pipeline_status_recent_summary as select
                'bus' as mode,
                1 as day_count,
                date '2026-07-13' as first_date,
                date '2026-07-13' as last_date,
                1.0 as completeness_ratio,
                1.0 as service_coverage_ratio,
                10 as trips_complete,
                1 as trips_broken,
                600 as expected_service_minutes,
                580 as observed_service_minutes,
                1.0 as health_ratio,
                'good' as health_label;
            """
        )
    DB_PATH.chmod(0o644)


def check_server() -> None:
    deadline = time.monotonic() + 15
    while True:
        try:
            with urlopen("http://127.0.0.1:5000/status", timeout=1) as response:  # noqa: S310
                body = response.read()
                assert response.status == 200
                assert b"service-minute coverage" in body
                assert b"2026-07-14 03:12:00 UTC" in body
                return
        except URLError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.25)


if __name__ == "__main__":
    {"create": create_fixture, "check": check_server}[sys.argv[1]]()
