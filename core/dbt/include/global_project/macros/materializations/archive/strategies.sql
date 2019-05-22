{#
    Dispatch strategies by name, optionally qualified to a package
#}
{% macro strategy_dispatch(name) -%}
{% set original_name = name %}
  {% if '.' in name %}
    {% set package_name, name = name.split(".", 1) %}
  {% else %}
    {% set package_name = none %}
  {% endif %}

  {% if package_name is none %}
    {% set package_context = context %}
  {% elif package_name in context %}
    {% set package_context = context[package_name] %}
  {% else %}
    {% set error_msg %}
        Could not find package '{{package_name}}', called with '{{original_name}}'
    {% endset %}
    {{ exceptions.raise_compiler_error(error_msg | trim) }}
  {% endif %}

  {%- set search_name = 'archive_' ~ name ~ '_strategy' -%}

  {% if search_name not in package_context %}
    {% set error_msg %}
        The specified strategy macro '{{name}}' was not found in package '{{ package_name }}'
    {% endset %}
    {{ exceptions.raise_compiler_error(error_msg | trim) }}
  {% endif %}
  {{ return(package_context[search_name]) }}
{%- endmacro %}


{#
    Create SCD Hash SQL fields cross-db
#}
{% macro archive_hash_arguments(args) %}
  {{ adapter_macro('archive_hash_arguments', args) }}
{% endmacro %}


{% macro default__archive_hash_arguments(args) %}
    md5({% for arg in args %}
        coalesce(cast({{ arg }} as varchar ), '') {% if not loop.last %} || '|' || {% endif %}
    {% endfor %})
{% endmacro %}


{#
    Get the current time cross-db
#}
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


{#
    Core strategy definitions
#}
{% macro archive_timestamp_strategy(archived_rel, current_rel, config) %}
    {% set primary_key = config['unique_key'] %}
    {% set updated_at = config['updated_at'] %}

    {% set row_changed_expr -%}
        ({{ archived_rel }}.{{ updated_at }} < {{ current_rel }}.{{ updated_at }})
    {%- endset %}

    {% set scd_id_expr = archive_hash_arguments([primary_key, updated_at]) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr
    }) %}
{% endmacro %}


{% macro archive_check_strategy(archived_rel, current_rel, config) %}
    {% set primary_key = config['unique_key'] %}
    {% set check_cols = config['check_cols'] %}
    {% set updated_at = archive_get_time() %}

    {% set row_changed_expr -%}
        (
        {% for col in check_cols %}
            {{ archived_rel }}.{{ col }} != {{ current_rel }}.{{ col }}
            {%- if not loop.last %} or {% endif %}
        {% endfor %}
        )
    {%- endset %}

    {% set scd_id_expr = archive_hash_arguments(check_cols) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr
    }) %}
{% endmacro %}
