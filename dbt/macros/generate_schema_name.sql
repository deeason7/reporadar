{#
    Use a model's configured schema as its schema, rather than as a suffix.

    The default behaviour prefixes the target's schema, which would publish the
    marts as `main_marts` — `main` being the in-memory query engine's own schema,
    a name that means nothing in the database the dashboard connects to and that
    would appear in every panel's SQL. Since the marts are written into an
    attached database, the target schema is not a namespace worth inheriting.

    Models with no configured schema still fall back to the target's, so this only
    changes the case it is written for.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
