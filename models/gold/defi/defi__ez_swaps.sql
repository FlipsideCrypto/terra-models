{{ config(
    materialized = "view",
    tags = ['core'],
    enabled = false
) }}

WITH swap AS (

    SELECT
        *
    FROM
        {{ ref("silver__dex_swaps") }}
)
SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_succeeded,
    trader,
    from_amount,
    from_currency,
    from_decimal,
    to_amount,
    to_currency,
    TO_DECIMAL,
    pool_id
FROM
    swap
