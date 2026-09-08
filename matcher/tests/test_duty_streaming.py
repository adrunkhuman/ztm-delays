from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_alignment import _course, _ping

import ztm_matcher.runtime as runtime
from ztm_matcher import ReconstructionRun, RunConfig
from ztm_matcher.alignment import extract_evidence, resolve_competing_ownership, settle_duty
from ztm_matcher.schemas import DUTY_EXECUTION_SCHEMA, TRAVERSAL_EVIDENCE_SCHEMA


@pytest.mark.parametrize("batch_size", [1, 3, 100])
@pytest.mark.parametrize("empty_evidence", [False, True])
def test_streamed_duties_match_per_duty_queries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, batch_size: int, empty_evidence: bool
) -> None:
    monkeypatch.setattr(runtime, "SEMANTICS_BATCH_ROWS", batch_size)
    output = tmp_path / "output"
    config = RunConfig(date(2026, 1, 15), "a", tmp_path, tmp_path / "gtfs.zip", output, output / "metrics.json")
    courses = [
        {**_course(trip, order), "service_date": day, "gtfs_snapshot_id": snapshot, "duty_chain_id": duty}
        for day in (date(2026, 1, 14), date(2026, 1, 15))
        for snapshot in ("a", "b")
        for duty in ("same", "without-evidence")
        for trip, order in (("second", 2), ("first-b", 1), ("first-a", 1))
    ]
    missing = {**courses[0], "duty_chain_id": "missing-terminals"}
    # Orphan inputs must not add duties; a schedule-only duty must still be yielded.
    orphan = {**courses[0], "duty_chain_id": "orphan"}
    schedule = courses + [missing, courses[0]]
    evidence = [
        item
        for course in courses + [missing, orphan]
        if course["duty_chain_id"] != "without-evidence" and not empty_evidence
        for item in extract_evidence(course, [_ping(0, 0), _ping(1, 0.01), _ping(2, 0.02)])
    ]
    with ReconstructionRun(config) as run:
        work = run._work()
        pq.write_table(pa.Table.from_pylist(list(reversed(schedule))), work / "duty_schedule.parquet")
        pq.write_table(pa.Table.from_pylist(list(reversed(courses + [orphan]))), work / ".terminal_courses.parquet")
        evidence_path = work / ".traversal_evidence.parquet"
        pq.write_table(pa.Table.from_pylist(list(reversed(evidence)), schema=TRAVERSAL_EVIDENCE_SCHEMA), evidence_path)
        connection = run._connection()
        keys = connection.execute(
            f"select distinct service_date, gtfs_snapshot_id, duty_chain_id "
            f"from read_parquet('{work / 'duty_schedule.parquet'}') order by 1, 2, 3"
        ).fetchall()
        expected = []
        for key in keys:
            pair = []
            for path, order in (
                (work / ".terminal_courses.parquet", "trip_order, trip_id"),
                (
                    evidence_path,
                    "trip_id, vehicle_type, vehicle_number, candidate_kind, origin_event_time, traversal_id",
                ),
            ):
                pair.append(
                    connection.execute(
                        f"select * from read_parquet('{path}') "
                        f"where service_date = ? and gtfs_snapshot_id = ? and duty_chain_id = ? order by {order}",
                        key,
                    )
                    .to_arrow_table()
                    .to_pylist()
                )
            expected.append(tuple(pair))

        queries = []

        class TracedConnection:
            def __init__(self, inner: Any) -> None:
                self.inner = inner

            def cursor(self) -> "TracedConnection":
                return TracedConnection(self.inner.cursor())

            def execute(self, sql: str) -> Any:
                queries.append(sql)
                return self.inner.execute(sql)

            def close(self) -> None:
                self.inner.close()

        monkeypatch.setattr(run, "_connection", lambda: TracedConnection(connection))
        actual = list(run._iter_duty_inputs(evidence_path))
        assert actual == expected
        assert len(actual) == len(keys) == 9
        assert any(not course_rows for course_rows, _ in actual)
        assert any(course_rows and not evidence_rows for course_rows, evidence_rows in actual)
        assert len(queries) == 3
        for name in ("duty_schedule.parquet", ".terminal_courses.parquet", ".traversal_evidence.parquet"):
            assert sum(name in query for query in queries) == 1
        assert list(run._iter_duty_inputs(evidence_path)) == actual

        def outcomes(groups: Any) -> pa.Table:
            return pa.Table.from_pylist(
                resolve_competing_ownership([row for c, e in groups for row in settle_duty(c, e)]),
                schema=DUTY_EXECUTION_SCHEMA,
            )

        assert outcomes(actual).equals(outcomes(expected))
