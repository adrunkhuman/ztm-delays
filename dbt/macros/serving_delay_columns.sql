{% macro serving_delay_count_columns(delay_column='delay_seconds') %}
    count(*) as arrival_count,
    avg({{ delay_column }}) as mean_delay_seconds,
    countif({{ delay_column }} < -60) as early_count,
    countif({{ delay_column }} between -60 and 180) as on_time_count,
    countif({{ delay_column }} > 180) as late_count,
    safe_divide(countif({{ delay_column }} < -60), count(*)) as early_rate,
    safe_divide(countif({{ delay_column }} between -60 and 180), count(*)) as on_time_rate,
    safe_divide(countif({{ delay_column }} > 180), count(*)) as late_rate,
    [
        struct('early_over_5m' as bucket_label, cast(null as int64) as min_delay_seconds, -301 as max_delay_seconds, countif({{ delay_column }} < -300) as n),
        struct('early_2_to_5m' as bucket_label, -300 as min_delay_seconds, -121 as max_delay_seconds, countif({{ delay_column }} between -300 and -121) as n),
        struct('early_1_to_2m' as bucket_label, -120 as min_delay_seconds, -61 as max_delay_seconds, countif({{ delay_column }} between -120 and -61) as n),
        struct('on_time_early_30_60s' as bucket_label, -60 as min_delay_seconds, -31 as max_delay_seconds, countif({{ delay_column }} between -60 and -31) as n),
        struct('on_time_early_0_30s' as bucket_label, -30 as min_delay_seconds, -1 as max_delay_seconds, countif({{ delay_column }} between -30 and -1) as n),
        struct('on_time_late_0_30s' as bucket_label, 0 as min_delay_seconds, 30 as max_delay_seconds, countif({{ delay_column }} between 0 and 30) as n),
        struct('on_time_late_30_60s' as bucket_label, 31 as min_delay_seconds, 60 as max_delay_seconds, countif({{ delay_column }} between 31 and 60) as n),
        struct('on_time_late_1_to_3m' as bucket_label, 61 as min_delay_seconds, 180 as max_delay_seconds, countif({{ delay_column }} between 61 and 180) as n),
        struct('late_3_to_5m' as bucket_label, 181 as min_delay_seconds, 300 as max_delay_seconds, countif({{ delay_column }} between 181 and 300) as n),
        struct('late_5_to_10m' as bucket_label, 301 as min_delay_seconds, 600 as max_delay_seconds, countif({{ delay_column }} between 301 and 600) as n),
        struct('late_10_to_20m' as bucket_label, 601 as min_delay_seconds, 1200 as max_delay_seconds, countif({{ delay_column }} between 601 and 1200) as n),
        struct('late_over_20m' as bucket_label, 1201 as min_delay_seconds, cast(null as int64) as max_delay_seconds, countif({{ delay_column }} > 1200) as n)
    ] as delay_histogram
{% endmacro %}
