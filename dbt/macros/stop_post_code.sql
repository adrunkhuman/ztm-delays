{% macro stop_post_code(stop_id_expression) -%}
    case
        when regexp_contains(cast({{ stop_id_expression }} as string), r'^.{4}[0-9]{2}$')
            then substr(cast({{ stop_id_expression }} as string), 5, 2)
        when regexp_contains(cast({{ stop_id_expression }} as string), r':[^:]+$')
            then regexp_extract(cast({{ stop_id_expression }} as string), r':([^:]+)$')
        else coalesce(nullif(substr(cast({{ stop_id_expression }} as string), 5), ''), cast({{ stop_id_expression }} as string))
    end
{%- endmacro %}
