{{ config(
    materialized = 'view',
    secure = true,
    enabled = false
) }}

WITH nft_mints AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_mints') }}
)
SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    tx_succeeded,
    contract_address,
    mint_price,
    minter,
    token_id,
    currency,
    decimals,
    mint_id
FROM
    nft_mints
