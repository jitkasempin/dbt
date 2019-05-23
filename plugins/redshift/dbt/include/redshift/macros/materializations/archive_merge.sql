
{% macro redshift__archive_merge_sql(target, source, update_cols, insert_cols) -%}
    {{ postgres__archive_merge_sql(target, source, update_cols, insert_cols) }}
{% endmacro %}
