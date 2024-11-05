-- tests/assert_transfer_amounts_match.sql
WITH raw_daily AS (
    SELECT 
        DATE(loaded_at) as date,
        SUM(amount_usd) as total_amount
    FROM {{ source('raw', 'RAW_TRANSFERS') }}
    WHERE amount_usd > 0
    GROUP BY 1
),

transformed_daily AS (
    SELECT 
        data_date as date,
        total_amount_usd as total_amount
    FROM {{ ref('daily_transfers') }}
)

SELECT 
    r.date,
    r.total_amount as raw_amount,
    t.total_amount as transformed_amount,
    ABS(r.total_amount - t.total_amount) as difference
FROM raw_daily r
LEFT JOIN transformed_daily t ON r.date = t.date
WHERE ABS(r.total_amount - t.total_amount) > 1.0  -- More tolerant threshold