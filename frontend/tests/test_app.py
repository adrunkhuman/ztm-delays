from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING

import duckdb

from ztm_frontend.app import create_app

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_status_page_renders_current_pipeline_contract(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table export_metadata as select
                'export-1' as export_id,
                'alpha-1' as export_version,
                'current_pipeline_provisional' as source_mode,
                timestamp '2026-07-14 03:12:00' as exported_at,
                1::ubigint as source_row_count,
                100::ubigint as duckdb_file_size_bytes;

            create table mart_pipeline_status as select
                date '2026-07-13' as service_date,
                'bus' as mode,
                1 as status_rank_desc,
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
    monkeypatch.setenv("ZTM_DUCKDB_PATH", str(db_path))

    response = create_app().test_client().get("/status")

    assert response.status_code == HTTPStatus.OK
    assert b"observed minutes" in response.data
    assert b"Bus partial" in response.data
    assert b"matched" not in response.data
