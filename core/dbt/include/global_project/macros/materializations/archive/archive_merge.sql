
{% macro archive_merge_sql(target, source, update_cols, insert_cols) -%}
  {{ adapter_macro('archive_merge_sql', target, source, update_cols, insert_cols) }}
{%- endmacro %}


{% macro default__archive_merge_sql(target, source, update_cols, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols| map(attribute="name") | join(', ') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id
      and DBT_INTERNAL_DEST.dbt_valid_to is null

    when matched and dbt_change_type = 'update'
    then update set {% for column in update_cols -%}
        {{ column.name }} = DBT_INTERNAL_SOURCE.{{ column.name }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    when not matched and dbt_change_type = 'insert'
    then insert
        ({{ insert_cols_csv }})
    values
        ({{ insert_cols_csv }});
{% endmacro %}


