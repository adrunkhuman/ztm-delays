{% macro serving_window_service_date_filter(column, start_date, end_date) %}
    {#- Joining dim_serving_window_date does not prune partitions; literal dates read at execution time do. -#}
    {% if not execute %}
        {{ return('true') }}
    {% endif %}
    {% set dates_query %}
        select string_agg(distinct format("date '%t'", service_date), ', ' order by format("date '%t'", service_date))
        from {{ ref('dim_serving_window_date') }}
        where source_end_date between date('{{ start_date }}') and date('{{ end_date }}')
    {% endset %}
    {% set dates = run_query(dates_query).columns[0].values()[0] %}
    {{ return(column ~ ' in (' ~ (dates or 'cast(null as date)') ~ ')') }}
{% endmacro %}
