{% macro serving_window_service_date_filter(column, start_date, end_date) %}
    {#- Joining dim_serving_window_date does not prune partitions; literal dates read at execution time do. -#}
    {#- Offline compiles (--no-introspect) cannot query; no filter keeps the result, unpruned. -#}
    {% if not execute or not flags.INTROSPECT %}
        {{ return('true') }}
    {% endif %}
    {% set dates_query %}
        select string_agg(distinct format("date '%t'", service_date), ', ' order by format("date '%t'", service_date))
        from {{ ref('dim_serving_window_date') }}
        where source_end_date between date('{{ start_date }}') and date('{{ end_date }}')
          and {{ serving_window_type_filter('window_type') }}
    {% endset %}
    {% set dates = run_query(dates_query).columns[0].values()[0] %}
    {{ return(column ~ ' in (' ~ (dates or 'cast(null as date)') ~ ')') }}
{% endmacro %}

{% macro serving_window_types() %}
    {#- Unset means every window type. The nightly prior-date pass sets day,month: its late overnight trips are
        visible there, while rolling windows ending yesterday are superseded by today's. -#}
    {% set value = var('serving_window_types', none) %}
    {% if value is none %}
        {{ return(none) }}
    {% endif %}
    {% set types = [] %}
    {% for item in value.split(',') %}
        {% if item.strip() %}
            {% do types.append(item.strip()) %}
        {% endif %}
    {% endfor %}
    {% for item in types %}
        {% if item not in ['day', 'month', 'weekdays', 'weekend'] %}
            {{ exceptions.raise_compiler_error('Unknown serving window type: ' ~ item) }}
        {% endif %}
    {% endfor %}
    {% if not types %}
        {{ exceptions.raise_compiler_error('serving_window_types must name at least one window type.') }}
    {% endif %}
    {% if flags.FULL_REFRESH %}
        {{ exceptions.raise_compiler_error('serving_window_types rebuilds part of a partition and cannot run with --full-refresh.') }}
    {% endif %}
    {{ return(types) }}
{% endmacro %}

{% macro serving_window_type_filter(column) %}
    {% set types = serving_window_types() %}
    {{ return('true' if types is none else column ~ " in ('" ~ types | join("', '") ~ "')") }}
{% endmacro %}

{% macro serving_window_retained_rows(processing_date) %}
    {#- insert_overwrite replaces the whole partition, so a partial rebuild carries the other window types over. -#}
    {% set types = serving_window_types() %}
    {% if types is not none and is_incremental() %}
union all corresponding
select *
from {{ this }}
where source_end_date = date('{{ processing_date }}')
  and not {{ serving_window_type_filter('window_type') }}
    {% endif %}
{% endmacro %}

{% macro serving_window_anchor_version(version_key, anchor_date) %}
    {#- Whether the arrival's schedule version is in force on the window's end date. -#}
    exists (
        select 1
        from {{ ref('dim_schedule_version') }} as anchor_version
        where farm_fingerprint(anchor_version.schedule_version_id) = {{ version_key }}
          and date('{{ anchor_date }}') between anchor_version.valid_from_date
              and coalesce(anchor_version.valid_to_date, date '9999-12-31')
    )
{% endmacro %}
