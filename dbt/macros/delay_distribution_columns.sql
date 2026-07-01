{% macro delay_distribution_columns(delay_column='delay_seconds') %}
    count(*) as n,
    avg({{ delay_column }}) as mean_delay_seconds,
    approx_quantiles({{ delay_column }}, 100)[offset(10)] as p10_delay_seconds,
    approx_quantiles({{ delay_column }}, 100)[offset(50)] as median_delay_seconds,
    approx_quantiles({{ delay_column }}, 100)[offset(50)] as p50_delay_seconds,
    approx_quantiles({{ delay_column }}, 100)[offset(90)] as p90_delay_seconds,
    stddev_samp({{ delay_column }}) as stddev_delay_seconds,
    safe_divide(countif({{ delay_column }} between -60 and 180), count(*)) as on_time_rate,
    [
        struct(
            'early_over_5m' as bucket_label,
            cast(null as int64) as min_delay_seconds,
            -301 as max_delay_seconds,
            countif({{ delay_column }} < -300) as n
        ),
        struct(
            'early_1_to_5m' as bucket_label,
            -300 as min_delay_seconds,
            -61 as max_delay_seconds,
            countif({{ delay_column }} between -300 and -61) as n
        ),
        struct(
            'on_time' as bucket_label,
            -60 as min_delay_seconds,
            180 as max_delay_seconds,
            countif({{ delay_column }} between -60 and 180) as n
        ),
        struct(
            'late_3_to_5m' as bucket_label,
            181 as min_delay_seconds,
            300 as max_delay_seconds,
            countif({{ delay_column }} between 181 and 300) as n
        ),
        struct(
            'late_5_to_10m' as bucket_label,
            301 as min_delay_seconds,
            600 as max_delay_seconds,
            countif({{ delay_column }} between 301 and 600) as n
        ),
        struct(
            'late_10_to_20m' as bucket_label,
            601 as min_delay_seconds,
            1200 as max_delay_seconds,
            countif({{ delay_column }} between 601 and 1200) as n
        ),
        struct(
            'late_over_20m' as bucket_label,
            1201 as min_delay_seconds,
            cast(null as int64) as max_delay_seconds,
            countif({{ delay_column }} > 1200) as n
        )
    ] as delay_histogram
{% endmacro %}
