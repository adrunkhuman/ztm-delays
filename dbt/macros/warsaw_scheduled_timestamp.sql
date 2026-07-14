{% macro warsaw_scheduled_timestamp(service_date, gtfs_seconds) %}
    timestamp(
        datetime_add(datetime({{ service_date }}), interval {{ gtfs_seconds }} second),
        'Europe/Warsaw'
    )
{% endmacro %}
