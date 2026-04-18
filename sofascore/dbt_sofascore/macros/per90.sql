{% macro per90(stat, minutes, decimal_places=2) %}
    case
        when {{ minutes }} = 0 or {{ minutes }} is null
        then null
        else round(
            cast({{ stat }} as decimal) / cast({{ minutes }} as decimal) * 90.0,
            {{ decimal_places }}
        )
    end
{% endmacro %}
