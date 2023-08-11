{{ config(
    materialized = 'view',
    tags = ['classic']
) }}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  proposer_address,
  tx_count
FROM
  {{ ref('silver_classic__blocks') }}
