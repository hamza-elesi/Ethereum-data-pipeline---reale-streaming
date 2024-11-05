-- models/marts/DEX_TRADES/daily_dex_trades.sql
{{ config(
    materialized='table',
    schema='DBT_SCHEMA_DEX_TRADES',
    unique_key=['data_date', 'exchange_name']
) }}

SELECT 
    data_date,
    exchange_name,
    COUNT(*) as number_of_trades,
    SUM(trade_amount_usd) as total_volume_usd,
    AVG(trade_amount_usd) as avg_trade_size_usd,
    MIN(trade_amount_usd) as min_trade_usd,
    MAX(trade_amount_usd) as max_trade_usd
FROM {{ ref('stg_dex_trades') }}
GROUP BY 1, 2