## BigQuery costs

- Use PHYSICAL billing for warehouse datasets.
- Prefer append-only loads and bounded partition replacement.
- Filter partitions and clustered snapshot IDs explicitly.
- Preserve recovery retention and historical correctness.
- Measure query scans and retained storage separately.
