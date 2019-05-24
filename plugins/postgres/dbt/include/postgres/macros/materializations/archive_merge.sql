
{% macro postgres__archive_merge_sql(target, source, update_cols, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | map(attribute="name") | join(', ') -%}

    update {{ target }}
    set {% for column in update_cols -%}
        {{ column.name }} = DBT_INTERNAL_SOURCE.{{ column.name }}
    {% endfor %}
    from {{ source }} as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id = {{ target }}.dbt_scd_id
      and DBT_INTERNAL_SOURCE.change_type = 'update'
      and {{ target }}.dbt_valid_to is null;

    insert into {{ target }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
        DBT_INTERNAL_SOURCE.{{ column.name }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ source }} as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type = 'insert';
{% endmacro %}
