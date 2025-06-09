{% macro calculate_percentage(column1, column2) %}
    CASE
        WHEN {{ column2 }} = 0 THEN NULL
        ELSE round(CAST({{ column1 }} AS FLOAT) / CAST({{ column2 }} AS FLOAT), 4)
    END
{% endmacro %}