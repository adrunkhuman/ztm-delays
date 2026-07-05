{% macro period_partition_filter(column_name='period_start_date') -%}
    {% set period_partition_dates = var("period_partition_dates", "") %}
    {% if period_partition_dates %}
        {{ column_name }} in (
            {%- for partition_date in period_partition_dates.split('|') -%}
                date('{{ partition_date }}'){% if not loop.last %}, {% endif %}
            {%- endfor -%}
        )
    {% else %}
        {{ column_name }} between date('{{ var("period_source_start_date", var("aggregation_start_date", var("processing_date"))) }}')
            and date('{{ var("processing_date") }}')
    {% endif %}
{%- endmacro %}
