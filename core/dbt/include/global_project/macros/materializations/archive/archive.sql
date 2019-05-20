{#
    Create SCD Hash SQL fields cross-db
#}

{% macro archive_hash_arguments(args) %}
  {{ adapter_macro('archive_hash_arguments', args) }}
{% endmacro %}

{% macro default__archive_hash_arguments(args) %}
    md5({% for arg in args %}coalesce(cast({{ arg }} as varchar ), '') {% if not loop.last %} || '|' || {% endif %}{% endfor %})
{% endmacro %}

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

{% macro archive_get_time() -%}
  {{ adapter_macro('archive_get_time') }}
{%- endmacro %}

{% macro default__archive_get_time() -%}
  {{ current_timestamp() }}
{%- endmacro %}

{#-- TODO : This doesn't belong here #}
{% macro snowflake__archive_get_time() -%}
  to_timestamp_ntz({{ current_timestamp() }})
{%- endmacro %}

{% macro timestamp_strategy(archived_rel, current_rel, config) %}

    {% set updated_at = config['updated_at'] %}
    {% set row_changed_expr -%}
        ({{ archived_rel }}.{{ updated_at }} < {{ current_rel }}.{{ updated_at }})
    {%- endset %}

    -- TODO : Use real macro here....
    {% set primary_key = config['unique_key'] %}
    {% set scd_id_expr %}
        md5({{ primary_key }} || {{ updated_at }}::text)
    {% endset %}


    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr
    }) %}
{% endmacro %}

{% macro check_col_strategy(archived_rel, current_rel, config) %}
    {% set check_cols = config['check_cols'] %}

    {# TODO 
    {% if check_cols == 'all' %}
    {% set check_cols = source_columns | map(attribute='name') | list %}
    #}
    {% set updated_at = archive_get_time() %}

    {% set row_changed_expr -%}
        (
        {% for col in check_cols %}
            {{ archived_rel }}.{{ col }} != {{ current_rel }}.{{ col }}
            {%- if not loop.last %} or {% endif %}
        {% endfor %}
        )
    {%- endset %}

    {% set primary_key = config['unique_key'] %}
    {% set scd_id_expr %}
        {# TODO : USE REAL HASH #}
        md5(cast({{ primary_key }} as text) || cast({{ 'name' }} as text))
    {% endset %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr
    }) %}
{% endmacro %}


{% macro archive_update_sql(strategy, source_sql, target_relation, source_columns) -%}

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

        select * from {{ target_relation }}

    ),

    updates as (

        select
            archived_data.dbt_pk,
            archived_data.dbt_scd_id,
            source_data.dbt_updated_at as dbt_valid_to

        from source_data
        join archived_data on archived_data.dbt_pk = source_data.dbt_pk
        where archived_data.dbt_valid_to is null
          and (
            {{ strategy.row_changed }}
          )
    )

    select * from updates

{%- endmacro %}

{% macro archive_insert_sql(strategy, source_sql, target_relation, source_columns) -%}

    with archive_query as (

        {{ source_sql }}

    ),

    source_data as (

        select *,
            {{ strategy.scd_id }} as dbt_scd_id,
            {{ strategy.unique_key }} as dbt_pk,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to

        from archive_query
    ),

    archived_data as (

        select * from {{ target_relation }}

    ),

    insertions as (

        select
            source_data.*

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
    )

    select * from insertions

{% endmacro %}


{% macro build_archive_table(strategy, sql) %}

    select *,
        {{ strategy.unique_key }} as dbt_pk,
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

  {% set information_schema = api.Relation.create(database=target_database).information_schema() %}
  {% if not check_schema_exists(information_schema, target_schema) %}
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

  {% if strategy_name == 'timestamp' %}
      {% set strategy_macro = timestamp_strategy %}
  {% elif strategy_name == 'check' %}
      {% set strategy_macro = check_col_strategy  %}
  {% else %}
      {{ exceptions.raise_compiler_error('Got invalid strategy "{}"'.format(strategy_name)) }}
  {% endif %}

  {% set strategy = strategy_macro("archived_data", "source_data", config) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_archive_table(strategy, model['injected_sql']) %}
      {% call statement('main') -%}
          {{ create_table_as(False, target_relation, build_sql) }}
      {% endcall %}

  {% else %}

      {{ adapter.valid_archive_target(target_relation) }}

      {# TODO : Be smarter about database/schema names for temp tables! #}
      {%- set tmp_relation = api.Relation.create(
            identifier=target_table ~ "__dbt_tmp") -%}

      {% set insert_sql = archive_insert_sql(strategy, model['injected_sql'], target_relation) %}

      {% call statement('gen_updates') %}
        {{ create_table_as(True, tmp_relation, insert_sql) }}
      {% endcall %}

      {% call statement('gen_updates') %}
        {% set update_sql = archive_update_sql(strategy, model['injected_sql'], target_relation) %}
        insert into {{ tmp_relation }} (dbt_scd_id, dbt_valid_to)
        select
            dbt_scd_id,
            dbt_valid_to

        from (
            {{ update_sql }}
        ) as sbq
      {% endcall %}

      {%- set source_columns = adapter.get_columns_in_relation(tmp_relation) -%}
      {# TODO : Make this take a relation #}
      {{ adapter.expand_target_column_types(temp_table=target_table ~ "__dbt_tmp",
                                            to_relation=target_relation) }}

      {% set missing_columns = adapter.get_missing_columns(tmp_relation, target_relation) %}
      {{ create_columns(target_relation, missing_columns) }}

      {% set merge_on = 'DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id' %}
      {% set merge_when = [
        {
            "type": "matched",
            "action": "update",
            "columns": [
                api.Column.create('dbt_valid_to', 'timestamp')
            ]
        },
        {
            "type": "not matched",
            "action": "insert",
            "columns": source_columns
        }
      ] %}

      {% call statement('main') %}
        {{ explicit_get_merge_sql(target_relation, tmp_relation, merge_on, merge_when) }}
      {% endcall %}

  {% endif %}

  {{ adapter.commit() }}
{% endmaterialization %}
