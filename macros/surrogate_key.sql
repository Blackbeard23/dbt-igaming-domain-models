{% macro surrogate_key(cols) -%}
    md5(concat({{ cols | join(", ") }}))
{%- endmacro %}
