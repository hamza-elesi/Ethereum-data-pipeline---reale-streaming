-- models/staging/stg_dex_trades.sql
{{ config(
    materialized='view',
    schema='DBT_SCHEMA_STAGING'
) }}

SELECT 
    COALESCE(exchange_name, 'UNKNOWN') as exchange_name,
    NULLIF(trade_amount_usd, 0) as trade_amount_usd,
    loaded_at,
    DATE(loaded_at) as data_date
FROM {{ source('raw', 'RAW_DEX_TRADES') }}
WHERE trade_amount_usd > 0