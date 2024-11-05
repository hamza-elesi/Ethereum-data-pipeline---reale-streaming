-- models/staging/stg_transfers.sql
{{ config(
    materialized='view',
    schema='DBT_SCHEMA_STAGING'
) }}

SELECT 
    COALESCE(token_symbol, 'UNKNOWN') as token_symbol,
    token_address,
    NULLIF(transaction_count, 0) as transaction_count,
    NULLIF(unique_senders, 0) as unique_senders,
    NULLIF(unique_receivers, 0) as unique_receivers,
    NULLIF(amount, 0) as token_amount,
    NULLIF(amount_usd, 0) as amount_usd,
    median_amount,
    max_amount,
    loaded_at,
    DATE(loaded_at) as data_date
FROM {{ source('raw', 'RAW_TRANSFERS') }}
WHERE amount_usd > 0