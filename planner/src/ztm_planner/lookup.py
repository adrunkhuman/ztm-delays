"""Hierarchical lookup of segment times: a base mean, then shrunken residual adjustments per level.

Level k's adjustment is the mean residual after levels < k, shrunk toward 0 by SHRINK pseudo-observations,
so sparse cells fall back to their parent. A new stop post inherits its stop-group pair (level 1).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from ztm_planner.settings import SHRINK

if TYPE_CHECKING:
    import duckdb

BASE = ("is_tram", "daytype", "hr", "sched_b")
LEVELS = (("grp", "daytype", "hr"), ("seg",), ("seg", "daytype", "hr"))
TABLES = ("base", "fallback", *(f"l{k}" for k in range(1, len(LEVELS) + 1)), "lr_seg", "lr_seghb")
TOP = f"p{len(LEVELS)}"


def fit(con: duckdb.DuckDBPyConnection, source: str, prefix: str) -> None:
    """Fit lookup tables ``<prefix>base``, ``<prefix>l1``..., from observed segments in ``source``."""
    con.execute(
        f"create or replace table {prefix}base as "
        f"select {', '.join(BASE)}, avg(actual_s) as p0 from {source} group by all"
    )
    con.execute(
        f"create or replace table {prefix}fallback as select is_tram, avg(actual_s) as fb from {source} group by all"
    )
    joins, pred = _base_join(prefix), "coalesce(b.p0, f.fb)"
    for k, keys in enumerate(LEVELS, 1):
        on = " and ".join(f"l{k}.{key} = s.{key}" for key in keys)
        con.execute(
            f"""
            create or replace table {prefix}l{k} as
            select {", ".join(f"s.{key}" for key in keys)},
                sum(s.actual_s - ({pred})) / (count(*) + {SHRINK}) as adj{k}, count(*) as n{k}
            from {source} s {joins} group by all
            """
        )
        joins += f" left join {prefix}l{k} l{k} on {on}"
        pred += f" + coalesce(l{k}.adj{k}, 0)"
    con.execute(
        f"create or replace table {prefix}lr_seg as select seg, avg(actual_s) as lr_seg from {source} group by all"
    )
    con.execute(
        f"create or replace table {prefix}lr_seghb as "
        f"select seg, hb, avg(actual_s) as lr_seghb from {source} group by all"
    )


def _base_join(prefix: str) -> str:
    on = " and ".join(f"b.{key} = s.{key}" for key in BASE)
    return f" left join {prefix}base b on {on} left join {prefix}fallback f on f.is_tram = s.is_tram"


def apply(con: duckdb.DuckDBPyConnection, source: str, target: str, prefix: str) -> None:
    """Create ``target``: ``source`` plus cumulative level predictions p0..pK, level counts and long-run means."""
    joins = _base_join(prefix)
    cols = ["coalesce(b.p0, f.fb) as p0"]
    pred = "coalesce(b.p0, f.fb)"
    for k, keys in enumerate(LEVELS, 1):
        joins += f" left join {prefix}l{k} l{k} on " + " and ".join(f"l{k}.{key} = s.{key}" for key in keys)
        pred += f" + coalesce(l{k}.adj{k}, 0)"
        cols += [f"{pred} as p{k}", f"coalesce(l{k}.n{k}, 0) as n{k}"]
    con.execute(
        f"""
        create or replace table {target} as
        select s.*, {", ".join(cols)}, ls.lr_seg, lh.lr_seghb
        from {source} s {joins}
        left join {prefix}lr_seg ls on ls.seg = s.seg
        left join {prefix}lr_seghb lh on lh.seg = s.seg and lh.hb = s.hb
        """
    )


def save(con: duckdb.DuckDBPyConnection, prefix: str, directory: Path) -> None:
    """Write the fitted tables to ``directory/lookup_<name>.parquet``."""
    directory.mkdir(parents=True, exist_ok=True)
    for name in TABLES:
        con.execute(f"copy {prefix}{name} to '{directory / f'lookup_{name}.parquet'}' (format parquet)")


def load(con: duckdb.DuckDBPyConnection, directory: Path, prefix: str) -> None:
    """Load tables written by ``save`` under ``prefix``."""
    for name in TABLES:
        path = Path(directory) / f"lookup_{name}.parquet"
        con.execute(f"create or replace table {prefix}{name} as select * from read_parquet('{path}')")
