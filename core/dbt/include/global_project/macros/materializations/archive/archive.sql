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
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr
    }) %}
{% endmacro %}

{% macro check_col_strategy(archived_rel, current_rel, config) %}
    {% set check_cols = config['check_cols'] %}

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
        md5({{ primary_key }} || {{ updated_at }})
    {% endset %}

    {% do return({
        "updated_at": archive_get_time(),
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr
    }) %}
{% endmacro %}


{% macro archive_select(source_sql, target_relation, source_columns, strategy) -%}

    {% set timestamp_column = api.Column.create('_', 'timestamp') %}

    with current_data as (

        select
            *,
            {{ updated_at }} as dbt_updated_at,
            {{ unique_key }} as dbt_pk,
            {{ updated_at }} as dbt_valid_from,
            {{ timestamp_column.literal('null') }} as tmp_valid_to
        from source
    ),

    archived_data as (

        select
            *,
            {{ updated_at }} as dbt_updated_at,
            {{ unique_key }} as dbt_pk,
            dbt_valid_from,
            dbt_valid_to as tmp_valid_to
        from {{ target_relation }}

    ),

    insertions as (

        select
            current_data.*,
            {{ timestamp_column.literal('null') }} as dbt_valid_to
        from current_data
        left outer join archived_data
          on archived_data.dbt_pk = current_data.dbt_pk
        where
          archived_data.dbt_pk is null
          or (
                archived_data.dbt_pk is not null
            and archived_data.dbt_updated_at < current_data.dbt_updated_at
            and archived_data.tmp_valid_to is null
        )
    ),

    updates as (

        select
            archived_data.*,
            current_data.dbt_updated_at as dbt_valid_to
        from current_data
        left outer join archived_data
          on archived_data.dbt_pk = current_data.dbt_pk
        where archived_data.dbt_pk is not null
          and archived_data.dbt_updated_at < current_data.dbt_updated_at
          and archived_data.tmp_valid_to is null
    )

    select
        {% for col in source_columns -%}
            {{ col.name }},
        {% endfor %}
        dbt_updated_at,
        dbt_pk,
        dbt_valid_from,
        dbt_valid_to,
        {{ scd_hash }} as dbt_scd_id

    from (
        select * from updates
        union all
        select * from insertions
    )

{%- endmacro %}


{% macro build_archive_table(strategy, sql) %}

    {% set updated_at = strategy['updated_at'] %}
    {% set scd_id = strategy['scd_id'] %}

    select *,
        {{ updated_at }} as dbt_valid_from,
        nullif({{ updated_at }}, {{ updated_at }}) as dbt_valid_to,
        {{ scd_id }} as dbt_scd_id,
        {{ updated_at }} as dbt_updated_at
    from (
        {{ sql }}
    ) sbq

{% endmacro %}

{% macro create_temporary_table(sql, relation) %}
  {{ return(adapter_macro('create_temporary_table', sql, relation)) }}
{% endmacro %}

{% macro default__create_temporary_table(sql, relation) %}
    {% call statement() %}
        {{ create_table_as(True, relation, sql) }}
    {% endcall %}
    {{ return(relation) }}
{% endmacro %}


{% materialization archive, default %}
  {%- set config = model['config'] -%}

  {%- set target_database = config.get('target_database') -%}
  {%- set target_schema = config.get('target_schema') -%}
  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}

  {% set information_schema = api.Relation.create(
    database=target_database,
    schema=target_schema,
    identifier=target_table).information_schema() %}

  {% if not check_schema_exists(information_schema, target_schema) %}
    {{ create_schema(target_database, target_schema) }}
  {% endif %}

  -- TODO : Can we clean this up?
  {%- set target_relation = adapter.get_relation(
      database=target_database,
      schema=target_schema,
      identifier=target_table) -%}

  {% set archive_exists = target_relation is not none %}

  {%- if target_relation is none -%}
    {%- set target_relation = api.Relation.create(
        database=target_database,
        schema=target_schema,
        identifier=target_table) -%}
  {%- elif not target_relation.is_table -%}
    {{ exceptions.relation_wrong_type(target_relation, 'table') }}
  {%- endif -%}

  -- TODO : Switch on this
  {% set strategy = timestamp_strategy("archived_data", "current_data", config) %}

  {% if not archive_exists %}

      {% set build_sql = build_archive_table(strategy, model['injected_sql']) %}
      {% call statement('main') -%}
          {{ create_table_as(False, target_relation, build_sql) }}
      {% endcall %}

  {% else %}
      {# TODO : Use source columns?? #}
      {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}

      {%- set source_sql = archive_select(model['injected_sql'], target_relation, dest_columns, strategy) %}

      {%- set tmp_relation = api.Relation.create(
            database=target_database,
            schema=target_schema,
            identifier=target_table ~ "__dbt_tmp") -%}

      {% call statement('main') %}
        {{ create_temporary_table(source_sql, tmp_relation) }}
        {{ get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns) }}
      {% endcall %}

  {% endif %}


{# -- TODO
    {% set missing_columns = adapter.get_missing_columns(source_info_model, target_relation) %}
    {{ create_columns(target_relation, missing_columns) }}
    {% if check_cols == 'all' %}
    {% set check_cols = source_columns | map(attribute='name') | list %}
    {% endif %}
    {{ exceptions.raise_compiler_error('Got invalid strategy "{}"'.format(strategy)) }}
    {{ adapter.expand_target_column_types(temp_table=tmp_identifier, to_relation=target_relation) }}
    {{ adapter.valid_archive_target(target_relation) }}
#}


  {{ adapter.commit() }}
{% endmaterialization %}
