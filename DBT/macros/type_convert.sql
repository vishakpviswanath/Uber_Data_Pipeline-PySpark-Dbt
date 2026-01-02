{% macro type_convert(column, trg_type) %}

    {% if trg_type == 'date' %}
        to_date({{column}}, 'yyyy-MM-dd')
    {% elif trg_type == 'timestamp' %}
        to_timestamp({{column}}, 'yyyy-MM-dd HH:mm:ss')
    {% elif trg_type == 'int' %}
        cast({{ column }} as int)
    {% elif trg_type == 'float' %}
        cast({{ column }} as decimal(9,2))
    {% elif trg_type == 'decimal' %}
        cast({{ column }} as decimal(9,6))
    {% else %}
        {{ exceptions.raise_compiler_error(
            "Invalid target type: " ~ trg_type
        ) }}
    {% endif %}
    
{% endmacro %}