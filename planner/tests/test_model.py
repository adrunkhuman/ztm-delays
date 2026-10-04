from __future__ import annotations

from datetime import date

import duckdb
import numpy as np
import pytest

from ztm_planner import assemble, features, lookup, model
from ztm_planner.weather import WEATHER_FEATURES


@pytest.mark.parametrize("unseen_only", [False, True])
def test_matrix_keeps_missing_features_as_nan(unseen_only: bool) -> None:
    with duckdb.connect() as con:
        day = date(2026, 9, 7)
        features.register_holidays(con, day, day)
        con.execute(
            """create table raw as select * from (values
                (date '2026-09-07', 1::bigint, 'bus', '110', 0, '100101', '100201', 2, 1,
                 false, false, 500., 28800, 60, 100)
            ) t(service_date, trip_key, mode, line, direction_id, a_stop, b_stop, b_seq, pos,
                a_request, b_request, dist_m, a_sched_sod, sched_s, actual_s)"""
        )
        features.add_features(con, "raw", "observed")
        lookup.fit(con, "observed", "m_")
        features.build_daily(con, "observed", "daily")
        con.execute("create table line_map as select '110' as line, 0 as line_id")
        weather_cols = ", ".join(
            f"{'null' if name == 'wind' else '0'}::double as {name}" for name in WEATHER_FEATURES
        )
        con.execute(f"create table weather as select timestamp '2026-09-07 08:00:00' as wx_ts, {weather_cols}")
        # Unseen line/segment, missing distance and no matching weather hour or recent observations.
        con.execute(
            """create table scoring as select * replace (
                2::bigint as trip_key, 'new-line' as line, 'new-segment' as seg, null::double as dist_m,
                timestamp '2026-09-07 09:00:00' as wx_ts
            ) from observed"""
        )
        if not unseen_only:
            con.execute("insert into scoring select * from observed")
        assemble.model_rows(con, "scoring", "model_rows", "m_", "daily", day)
        # Exercise the row_id ordering even when the physical table is reversed.
        con.execute("create table reversed_rows as select * from model_rows order by row_id desc")
        actual = model.matrix(con, "reversed_rows")
        rows = con.execute(
            f"select {', '.join(model.FEATURES)} from model_rows order by row_id"
        ).fetchall()
        expected = np.array(
            [[np.nan if value is None else value for value in row] for row in rows], dtype=np.float32
        )
        assert actual.dtype == np.float32
        assert not np.ma.isMaskedArray(actual)
        np.testing.assert_array_equal(actual, expected)
        unseen_id = con.execute("select row_id from model_rows where line = 'new-line'").fetchone()
        assert unseen_id is not None
        unseen = actual[unseen_id[0] - 1]
        for name in ("line_id", "dist_m", "shift_seg", "shift_seghb", *WEATHER_FEATURES):
            assert np.isnan(unseen[model.FEATURES.index(name)])
        assert unseen[model.FEATURES.index("recent_n_seg")] == 0
        con.execute("create table empty_rows as select * from model_rows limit 0")
        assert model.matrix(con, "empty_rows").shape == (0, len(model.FEATURES))
