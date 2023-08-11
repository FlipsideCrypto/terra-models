{{ config(
    materialized = 'view',
    secure = 'true',
    tags = ['classic'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'TERRASWAP',
    'PURPOSE': 'DEX' }} }
) }}

SELECT
  'terra' as blockchain,
  chain_id,
  block_id,
  block_timestamp,
  contract_address,
  total_share / pow(10,6) AS total_share,
  token_0_currency,
  token_0_amount / pow(10,6) AS token_0_amount,
  token_1_currency,
  token_1_amount / pow(10,6) AS token_1_amount
FROM
  {{ ref('silver_classic__terraswap_pool_reserves') }}

