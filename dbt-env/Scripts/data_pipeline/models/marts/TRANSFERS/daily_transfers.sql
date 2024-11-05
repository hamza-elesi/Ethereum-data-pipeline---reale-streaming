-- models/marts/TRANSFERS/daily_transfers.sql
{{ config(
    materialized='table',
    schema='DBT_SCHEMA_TRANSFERS',
    unique_key=['data_date', 'token_symbol', 'token_address']
) }}

SELECT 
    data_date,
    token_symbol,
    token_address,
    COUNT(*) as num_transfers,
    SUM(transaction_count) as total_transactions,
    SUM(unique_senders) as total_unique_senders,
    SUM(unique_receivers) as total_unique_receivers,
    SUM(token_amount) as total_token_amount,
    SUM(amount_usd) as total_amount_usd,
    AVG(amount_usd) as avg_transfer_size_usd,
    MAX(max_amount) as largest_transfer
FROM {{ ref('stg_transfers') }}
GROUP BY 1, 2, 3