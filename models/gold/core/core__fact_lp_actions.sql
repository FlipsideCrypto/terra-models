{{ config(
    materialized = 'view',
    secure = true
) }}

WITH lp_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__lp_actions') }}
)

select 
    block_id,
    block_timestamp,
    action_id,
    tx_id,
    tx_succeeded,
    blockchain,
    chain_id,
    pool_address,
    liquidity_provider_address,
    action,
    amount,
    currency,
    decimals
from
    lp_actions