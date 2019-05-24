
{% macro archive_merge_sql(target, source, insert_cols) -%}
  {{ adapter_macro('archive_merge_sql', target, source, insert_cols) }}
{%- endmacro %}


{% macro default__archive_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols| map(attribute="name") | join(', ') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_pk = DBT_INTERNAL_DEST.dbt_pk

    when matched and DBT_INTERNAL_DEST.dbt_valid_to is null
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_from

    when not matched
        then insert ({{ insert_cols_csv }})
        values ({{ insert_cols_csv }})
    ;
{% endmacro %}


