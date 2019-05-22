
{% macro explicit_get_merge_sql(target, source, merge_on, merge_when) -%}
  {{ adapter_macro('explicit_get_merge_sql', target, source, merge_on, merge_when) }}
{%- endmacro %}


{#
    merge_on: <string>
        "DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id"

    merge_when: list<dict>:
        [
            {
                "type": "matched",
                "action": "update",
                "columns": ["column_1", ....]
            },
            {
                "type": "not matched",
                "action": "insert",
                "columns": ["column_1", ....]
            },
            ...
        ]
#}

{% macro default__explicit_get_merge_sql(target, source, merge_on, merge_when) -%}
    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on {{ merge_on }}

    {% for clause in merge_when %}
        {% if clause.type == 'matched' and clause.action == 'update' %}
            when matched {{ clause.predicate }}
            then update set {% for column in clause.columns -%}
                {{ column.name }} = DBT_INTERNAL_SOURCE.{{ column.name }}
                {%- if not loop.last %}, {%- endif %}
            {%- endfor %}
        {% elif clause.type == 'matched' and clause.action == 'delete' %}
            when matched {{ clause.predicate }} then delete
        {% elif clause.type == 'not matched' and clause.action == 'insert' %}
            {%- set cols_csv = clause.columns | map(attribute="name") | join(', ') -%}
            when not matched {{ clause.predicate }}
            then insert
                ({{ cols_csv }})
            values
                ({{ cols_csv }})
        {% else %}
            {% do exceptions.raise_compiler_error("The specified merge clause for " ~ target ~ " is not supported:\n" ~ clause) %}
        {% endif %}
    {% endfor %}
{% endmacro %}


{% macro postgres__explicit_get_merge_sql(target, source, merge_on, merge_when) -%}
    {% for clause in merge_when %}
        {%- set cols_csv = clause.columns | map(attribute="name") | join(', ') -%}

        {% if clause.type == 'matched' and clause.action == 'update' %}
            update {{ target }} as DBT_INTERNAL_DEST
            set {% for column in clause.columns -%}
                {{ column.name }} = DBT_INTERNAL_SOURCE.{{ column.name }}
            {% endfor %}
            from {{ source }} as DBT_INTERNAL_SOURCE
            where {{ merge_on }} {{ clause.predicate }};
        {% elif clause.type == 'matched' and clause.action == 'delete' %}
            delete from {{ target }} as DBT_INTERNAL_DEST
            using {{ source }} as DBT_INTERNAL_SOURCE
            where {{ merge_on }} {{ clause.predicate }};
        {% elif clause.type == 'not matched' and clause.action == 'insert' %}
            insert into {{ target }} ({{ cols_csv }})
            select {% for column in clause.columns -%}
                DBT_INTERNAL_SOURCE.{{ column.name }} {%- if not loop.last %}, {%- endif %}
            {%- endfor %}
            from {{ source }} as DBT_INTERNAL_SOURCE
            where not exists(
                select 1
                from {{ target }} as DBT_INTERNAL_DEST
                where {{ merge_on }}
            ) {{ clause.predicate }}
            ;
        {% else %}
            {% do exceptions.raise_compiler_error("The specified merge clause for " ~ target ~ " is not supported:\n" ~ clause) %}
        {% endif %}
    {% endfor %}
{% endmacro %}
