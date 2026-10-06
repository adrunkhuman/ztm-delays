{% macro serving_route_pattern_rebuild_range() %}
    {% set processing_date = var('processing_date', '1970-01-01') %}
    {% set start = var('serving_rebuild_start_date', none) %}
    {% set end = var('serving_rebuild_end_date', none) %}
    {% if (start is none) != (end is none) %}
        {{ exceptions.raise_compiler_error('Supply both serving_rebuild_start_date and serving_rebuild_end_date.') }}
    {% endif %}
    {% if start is not none %}
        {% if modules.datetime.date.fromisoformat(start) > modules.datetime.date.fromisoformat(end) %}
            {{ exceptions.raise_compiler_error('serving_rebuild_start_date must not exceed serving_rebuild_end_date.') }}
        {% endif %}
        {% if is_incremental() %}
            {{ exceptions.raise_compiler_error('Bulk route-pattern rebuild requires --full-refresh for the selected route-pattern models.') }}
        {% endif %}
    {% endif %}
    {{ return({'start': start or processing_date, 'end': end or processing_date}) }}
{% endmacro %}

{% macro serving_route_pattern_window_ctes() %}
    {% set scope = serving_route_pattern_rebuild_range() %}
    {% set lookback_days = var('serving_window_lookback_days', 420) %}
    windows as (
        select *
        from {{ ref('dim_serving_window_date') }}
        where source_end_date between date('{{ scope.start }}') and date('{{ scope.end }}')
    ),
    patterns as (
        select *
        from {{ ref('int_serving_trip_route_pattern') }}
        where service_date between date_sub(date('{{ scope.start }}'), interval {{ lookback_days }} day)
            and date('{{ scope.end }}')
    ),
    eligible as (
        select windows.window_type, windows.window_key, windows.source_end_date, patterns.*
        from patterns
        inner join windows on patterns.service_date = windows.service_date
        where patterns.service_date between date_sub(windows.source_end_date, interval {{ lookback_days }} day)
            and windows.source_end_date
          and (
              windows.window_type in ('day', 'month')
              or exists (
                  select 1
                  from {{ ref('dim_schedule_version') }} as anchor_version
                  where anchor_version.schedule_version_id = patterns.schedule_version_id
                    and windows.source_end_date between anchor_version.valid_from_date
                        and coalesce(anchor_version.valid_to_date, date '9999-12-31')
              )
          )
    )
{% endmacro %}
