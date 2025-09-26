{% macro safe_divide_round(numerator, denominator, decimal_places=4) %}
    case 
        when {{ denominator }} = 0 or {{ denominator }} is null 
        then null 
        else round(
            cast({{ numerator }} as decimal) / cast({{ denominator }} as decimal), 
            {{ decimal_places }}
        )
    end
{% endmacro %}