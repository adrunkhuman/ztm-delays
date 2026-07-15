from __future__ import annotations

import json
from pathlib import Path

from ztm_matcher.schemas import RAW_GPS_SCHEMA, RAW_GPS_SCHEMA_VERSION

RAW_GPS_CONTRACT = Path(__file__).resolve().parents[2] / "contracts" / "raw_gps_v1.json"


def test_raw_gps_arrow_schema_matches_repository_contract() -> None:
    contract = json.loads(RAW_GPS_CONTRACT.read_text(encoding="utf-8"))

    assert contract["version"] == RAW_GPS_SCHEMA_VERSION
    assert [(field.name, str(field.type), field.nullable) for field in RAW_GPS_SCHEMA] == [
        (field["name"], field["arrow_type"], field["nullable"]) for field in contract["fields"]
    ]
