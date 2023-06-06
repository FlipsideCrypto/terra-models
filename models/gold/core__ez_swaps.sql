{{ config(materialized="view", secure=true) }}

with swap as (select * from {{ ref("silver__dex_swaps") }})

select
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
    to_decimal,
    pool_id

from swap
