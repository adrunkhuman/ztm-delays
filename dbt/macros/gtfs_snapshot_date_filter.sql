{#
    Partition filter for the snapshot-partitioned GTFS sources (raw_gtfs_trips, raw_gtfs_stop_times).
    Clustering on gtfs_snapshot_id alone does not bound BigQuery's pre-run estimate for MERGE, and
    maximum_bytes_billed is enforced against that estimate. Non-production IDs (unit-test fixtures)
    render no filter; the production tables require one, so a malformed ID fails instead of scanning.
#}
{% macro gtfs_snapshot_date_filter(snapshot_ids, column='gtfs_snapshot_date') %}
    {%- set ids = [snapshot_ids] if snapshot_ids is string else snapshot_ids -%}
    {%- set dates = [] -%}
    {%- set fixture_ids = [] -%}
    {%- for snapshot_id in ids -%}
        {%- if not modules.re.fullmatch('[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z_[0-9a-f]{12}', snapshot_id) -%}
            {%- do fixture_ids.append(snapshot_id) -%}
        {%- elif snapshot_id[:10] not in dates -%}
            {%- do dates.append(snapshot_id[:10]) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if fixture_ids -%}
        true
    {%- elif dates -%}
        {{ column }} in ({% for day in dates %}date '{{ day }}'{% if not loop.last %}, {% endif %}{% endfor %})
    {%- else -%}
        false
    {%- endif -%}
{% endmacro %}
