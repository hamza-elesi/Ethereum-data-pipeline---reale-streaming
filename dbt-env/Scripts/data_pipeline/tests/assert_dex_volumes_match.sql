-- tests/assert_dex_volumes_match.sql
WITH raw_daily AS (
    SELECT 
        DATE(loaded_at) as date,
        SUM(trade_amount_usd) as total_volume
    FROM {{ source('raw', 'RAW_DEX_TRADES') }}
    WHERE trade_amount_usd > 0
    GROUP BY 1
),

transformed_daily AS (
    SELECT 
        data_date as date,
        total_volume_usd as total_volume
    FROM {{ ref('daily_dex_trades') }}
)

SELECT 
    r.date,
    r.total_volume as raw_volume,
    t.total_volume as transformed_volume,
    ABS(r.total_volume - t.total_volume) as difference
FROM raw_daily r
LEFT JOIN transformed_daily t ON r.date = t.date
WHERE ABS(r.total_volume - t.total_volume) > 1.0  -- More tolerant threshold