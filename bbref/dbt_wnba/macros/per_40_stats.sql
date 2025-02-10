{% macro per_40_stats(metric_column, minutes_column) %}
    CASE
        WHEN {{ minutes_column }} = 0 THEN 0
        ELSE round(CAST({{ metric_column }} AS FLOAT) / CAST({{ minutes_column }} AS FLOAT) * 40, 3)
    END
{% endmacro %}