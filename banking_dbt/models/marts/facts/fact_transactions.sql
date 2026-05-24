{# {{ config(materialized='incremental', unique_key='transaction_id') }}

SELECT
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_type,
    t.transaction_time,
    CURRENT_TIMESTAMP AS load_timestamp
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('stg_accounts') }} a
    ON t.account_id = a.account_id #}

{{ config(materialized='incremental', unique_key='transaction_id') }}

WITH base_data AS (
    SELECT
        t.transaction_id,
        t.account_id,
        a.customer_id,
        t.amount,
        t.related_account_id,
        t.status,
        t.transaction_type,
        t.transaction_time,
        CURRENT_TIMESTAMP AS load_timestamp
    FROM {{ ref('stg_transactions') }} t
    LEFT JOIN {{ ref('stg_accounts') }} a
        ON t.account_id = a.account_id
)

SELECT * 
FROM base_data
-- Giữ lại 1 dòng duy nhất cho mỗi transaction_id, ưu tiên dòng có thời gian mới nhất
QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) = 1