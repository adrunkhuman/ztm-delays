{#- GTFS seconds count from Warsaw noon minus 12 h: local midnight except on DST change days. Matches ztm_matcher.time. -#}
{% macro warsaw_scheduled_timestamp(service_date, gtfs_seconds) %}
    timestamp_add(
        timestamp_sub(timestamp(datetime({{ service_date }}, time '12:00:00'), 'Europe/Warsaw'), interval 12 hour),
        interval {{ gtfs_seconds }} second
    )
{% endmacro %}
