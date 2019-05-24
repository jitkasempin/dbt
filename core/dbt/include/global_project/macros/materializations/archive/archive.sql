{#
    Add new columns to the table if applicable
#}
{% macro create_columns(relation, columns) %}
  {{ adapter_macro('create_columns', relation, columns) }}
{% endmacro %}

{% macro default__create_columns(relation, columns) %}
  {% for column in columns %}
    {% call statement() %}
      alter table {{ relation }} add column "{{ column.name }}" {{ column.data_type }};
    {% endcall %}
  {% endfor %}
{% endmacro %}


{% macro archive_staging_table_sql(strategy, source_sql, target_relation, source_columns) -%}

    with archive_query as (

        {{ source_sql }}

    ),

    source_data as (

        select *,
            {{ strategy.scd_id }} as dbt_scd_id,
            {{ strategy.unique_key }} as dbt_pk,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from

        from archive_query
    ),

    archived_data as (

        select *,
            {{ strategy.unique_key }} as dbt_pk

        from {{ target_relation }}

    ),

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to

        from source_data
        left outer join archived_data on archived_data.dbt_pk = source_data.dbt_pk
        where archived_data.dbt_pk is null
           or (
                archived_data.dbt_pk is not null
            and archived_data.dbt_valid_to is null
            and (
                {{ strategy.row_changed }}
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            source_data.dbt_updated_at as dbt_valid_to

        from source_data
        join archived_data on archived_data.dbt_pk = source_data.dbt_pk
        where archived_data.dbt_valid_to is null
          and (
            {{ strategy.row_changed }}
          )
    )

    select * from insertions
    union all
    select * from updates

{%- endmacro %}


{% macro build_archive_table(strategy, sql) %}

    select *,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq

{% endmacro %}


{% macro get_or_create_relation(database, schema, identifier, type) %}
  {%- set target_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}

  {% if target_relation %}
    {% do return([true, target_relation]) %}
  {% endif %}

  {%- set new_relation = api.Relation.create(
      database=database,
      schema=schema,
      identifier=identifier,
      type=type
  ) -%}
  {% do return([false, new_relation]) %}
{% endmacro %}


{% materialization archive, default %}
  {%- set config = model['config'] -%}

  {%- set target_database = config.get('target_database') -%}
  {%- set target_schema = config.get('target_schema') -%}
  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}

  {% if not adapter.check_schema_exists(target_database, target_schema) %}
    {% do create_schema(target_database, target_schema) %}
  {% endif %}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=target_database,
          schema=target_schema,
          identifier=target_table,
          type='table') -%}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "archived_data", "source_data", config) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_archive_table(strategy, model['injected_sql']) %}
      {% call statement('main') -%}
          {{ create_table_as(False, target_relation, build_sql) }}
      {% endcall %}

  {% else %}

      {{ adapter.valid_archive_target(target_relation) }}

      {%- set tmp_relation = make_temp_relation(target_relation) %}
      {% set merge_sql = archive_staging_table_sql(strategy, sql, target_relation) %}

      {% call statement('build_archive_staging_relation') %}
        {{ create_table_as(True, tmp_relation, merge_sql) }}
      {% endcall %}

      {% set source_columns = adapter.get_columns_in_relation(tmp_relation) %}

      {% do adapter.expand_target_column_types(from_relation=tmp_relation,
                                               to_relation=target_relation) %}

      {% set excluded_cols = ['dbt_change_type', 'dbt_pk'] %}
      {% set missing_columns = adapter.get_missing_columns(tmp_relation, target_relation)
                               | rejectattr("name", "in", excluded_cols)
                               | rejectattr("name", "in", excluded_cols | upper)
                               | list %}

      {% set dest_columns = source_columns
                            | rejectattr("name", "in", excluded_cols)
                            | rejectattr("name", "in", excluded_cols | upper)
                            | list %}

      {% do create_columns(target_relation, missing_columns) %}

      {% call statement('main') %}
          {{ archive_merge_sql(
                target = target_relation,
                source = tmp_relation,
                update_cols = [api.Column.create('dbt_valid_to', 'timestamp')],
                insert_cols = dest_columns
             )
          }}
      {% endcall %}

  {% endif %}

  {{ adapter.commit() }}
{% endmaterialization %}
