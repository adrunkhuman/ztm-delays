{% macro schedule_snapshot_literals(snapshot_ids) %}
    {% for snapshot_id in snapshot_ids %}'{{ snapshot_id }}'{% if not loop.last %}, {% endif %}{% else %}cast(null as string){% endfor %}
{% endmacro %}

{% macro schedule_ledger_difference(ledger_relation) %}
    with recorded as (
        select processing_date, gtfs_snapshot_id
        from {{ ledger_relation }}
        where is_date_marker and gtfs_snapshot_id is not null
    )
    select coalesce(mapping.processing_date, recorded.processing_date) as processing_date,
        mapping.gtfs_snapshot_id
    from {{ ref('int_gtfs_processing_snapshot') }} as mapping
    full outer join recorded using (processing_date)
    where mapping.gtfs_snapshot_id is distinct from recorded.gtfs_snapshot_id
{% endmacro %}

{% macro schedule_ledger_plan() %}
    {% if not execute %}{{ return([]) }}{% endif %}
    {% if flags.FULL_REFRESH %}
        {{ exceptions.raise_compiler_error('Schedule ledger forbids --full-refresh; use bounded bootstrap batches.') }}
    {% endif %}
    {% set max_dates = var('schedule_ledger_max_dates', 31) | int %}
    {% if max_dates < 1 or max_dates > 366 %}
        {{ exceptions.raise_compiler_error('schedule_ledger_max_dates must be 1..366.') }}
    {% endif %}
    {% set explicit_plan = var('schedule_ledger_plan', none) %}
    {% if flags.WHICH not in ['run', 'build'] %}
        {% if explicit_plan is none %}
            {{ exceptions.raise_compiler_error('Offline ledger compile requires schedule_ledger_plan: [{processing_date: YYYY-MM-DD, gtfs_snapshot_id: ID}]. No warehouse queries were issued by the planner.') }}
        {% endif %}
        {% set plan = explicit_plan %}
    {% else %}
        {% if explicit_plan is not none %}
            {{ exceptions.raise_compiler_error('schedule_ledger_plan is compile-only; runtime always reconciles the current mapping.') }}
        {% endif %}
        {% set existing = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) %}
        {% if existing is not none and existing.type != 'table' %}
            {{ exceptions.raise_compiler_error('Schedule ledger must be a table; refusing automatic replacement of an existing view.') }}
        {% endif %}
        {% set bootstrap = var('schedule_ledger_bootstrap', false) %}
        {% if existing is none and not bootstrap %}
            {{ exceptions.raise_compiler_error('Schedule ledger missing: explicit bounded schedule_ledger_bootstrap required before normal runs.') }}
        {% endif %}
        {% if bootstrap %}
            {% set start = var('schedule_ledger_start_date', '') %}
            {% set end = var('schedule_ledger_end_date', '') %}
            {% set start_date = modules.datetime.date.fromisoformat(start) %}
            {% set end_date = modules.datetime.date.fromisoformat(end) %}
            {% if (end_date - start_date).days < 0 or (end_date - start_date).days >= max_dates %}
                {{ exceptions.raise_compiler_error('Bootstrap range must fit schedule_ledger_max_dates (inclusive).') }}
            {% endif %}
            {% set plan_sql %}
                select requested as processing_date, mapping.gtfs_snapshot_id
                from unnest(generate_date_array(date('{{ start }}'), date('{{ end }}'))) as requested
                left join {{ ref('int_gtfs_processing_snapshot') }} as mapping on requested = mapping.processing_date
            {% endset %}
        {% else %}
            {% set repair_dates = var('schedule_ledger_repair_dates', []) %}
            {% for day in repair_dates %}
                {% set validated = modules.datetime.date.fromisoformat(day) %}
            {% endfor %}
            {% set plan_sql %}
                {{ schedule_ledger_difference(existing) }}
                {% if repair_dates %}
                union distinct
                select requested as processing_date, mapping.gtfs_snapshot_id
                from unnest([{% for day in repair_dates %}date('{{ day }}'){% if not loop.last %}, {% endif %}{% endfor %}]) as requested
                left join {{ ref('int_gtfs_processing_snapshot') }} as mapping on requested = mapping.processing_date
                {% endif %}
            {% endset %}
        {% endif %}
        {% set result = run_query('select * from (' ~ plan_sql ~ ') order by processing_date limit ' ~ (max_dates + 1)) %}
        {% set plan = [] %}
        {% for row in result.rows %}
            {% do plan.append({'processing_date': row[0] | string, 'gtfs_snapshot_id': row[1]}) %}
        {% endfor %}
    {% endif %}
    {% if plan | length > max_dates %}
        {{ exceptions.raise_compiler_error('Affected schedule dates exceed schedule_ledger_max_dates; nothing expanded. Use reviewed bounded bootstrap batches, then reconcile again.') }}
    {% endif %}
    {% set seen = [] %}
    {% for item in plan %}
        {% set validated = modules.datetime.date.fromisoformat(item['processing_date']) %}
        {% if item['processing_date'] in seen %}
            {{ exceptions.raise_compiler_error('Duplicate processing date in schedule_ledger_plan.') }}
        {% endif %}
        {% do seen.append(item['processing_date']) %}
        {% if item['gtfs_snapshot_id'] is not none and not modules.re.fullmatch('[A-Za-z0-9_:+.\\-]+', item['gtfs_snapshot_id']) %}
            {{ exceptions.raise_compiler_error('Invalid pinned schedule snapshot ID.') }}
        {% endif %}
    {% endfor %}
    {{ return(plan) }}
{% endmacro %}

{% macro schedule_ledger_assert_complete() %}
    assert not exists (
        {{ schedule_ledger_difference(ref('int_schedule_fingerprint_daily')) }}
    ) as 'Schedule ledger is incomplete or stale: reconcile all affected dates before publishing versions';
{% endmacro %}
