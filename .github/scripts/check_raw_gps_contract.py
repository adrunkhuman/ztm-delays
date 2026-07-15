from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "contracts" / "raw_gps_v1.json"
MANIFEST_PATH = ROOT / "dbt" / "target" / "manifest.json"


def main() -> None:
    """Fail when dbt's raw GPS source differs from the repository contract."""
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    source = next(
        source
        for source in manifest["sources"].values()
        if source["source_name"] == "raw" and source["name"] == "raw_gps_pings"
    )
    actual = [(column["name"], column.get("data_type")) for column in source["columns"].values()]
    expected = [(field["name"], field["bigquery_type"]) for field in contract["fields"]]
    if actual != expected:
        raise SystemExit(f"dbt raw_gps_pings schema does not match {contract['version']}: {actual!r}")


if __name__ == "__main__":
    main()
