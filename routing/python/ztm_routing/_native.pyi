from collections.abc import Sequence

type Incidence = tuple[int, int, Sequence[int], Sequence[int]]
type Envelope = tuple[Sequence[float], Sequence[float], Sequence[float]]
type Record = tuple[int, int, int, int, int, int, int, int]

class PreparedNet:
    def __init__(
        self,
        nstops: int,
        late_base: Sequence[int],
        expected: Sequence[int],
        depart: Sequence[int],
        range_ids: Sequence[int],
        trip_rows: Sequence[int],
        trip_pattern: Sequence[int],
        cumulative: Sequence[float],
        patterns: Sequence[Sequence[int]],
        pattern_alights: Sequence[Sequence[int]],
        pattern_prefix: Sequence[Sequence[float]],
        incidence: Sequence[Sequence[Incidence]],
        footpaths: Sequence[Sequence[tuple[int, int]]],
        envelopes: Sequence[Envelope],
    ) -> None: ...
    def payload_bytes(self) -> int: ...
    def late(self, board: int, alight: int) -> int: ...

class PreparedQuery:
    def __init__(self, network: PreparedNet, to_target: Sequence[float], permission_words: int) -> None: ...
    def peak_permission_words(self) -> int: ...

class NativeState:
    def __init__(
        self,
        query: PreparedQuery,
        origins: Sequence[int],
        access: Sequence[tuple[int, int]],
        origin_walks: Sequence[tuple[int, int, int]],
        targets: Sequence[tuple[int, int]],
        has_egress: bool,
        horizon_seconds: int,
    ) -> None: ...
    def run(self, after: int, boarding: Sequence[int] | None = None) -> list[list[Record]]: ...
    def invalidate(self) -> None: ...
    def stats(self) -> dict[str, int]: ...
