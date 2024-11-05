-- macros/test_daily_metric_consistency.sql
{% test daily_metric_consistency(model, date_column, metric_column) %}

WITH daily_changes AS (
    SELECT 
        {{ date_column }},
        {{ metric_column }},
        LAG({{ metric_column }}) OVER (ORDER BY {{ date_column }}) as prev_value,
        ({{ metric_column }} - LAG({{ metric_column }}) OVER (ORDER BY {{ date_column }})) / 
        NULLIF(LAG({{ metric_column }}) OVER (ORDER BY {{ date_column }}), 0) * 100 as pct_change
    FROM {{ model }}
)

SELECT *
FROM daily_changes
WHERE ABS(pct_change) > 200  -- Flag suspicious changes (more than 200%)

{% endtest %}