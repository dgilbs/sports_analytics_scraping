{% macro per_36_stats(metric_column, minutes_column) %}
    CASE
        WHEN {{ minutes_column }} = 0 THEN 0
        ELSE round(CAST({{ metric_column }} AS NUMERIC) / CAST({{ minutes_column }} AS NUMERIC) * 36, 3)
    END
{% endmacro %}