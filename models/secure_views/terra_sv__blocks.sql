{{ config(
  materialized = 'view',
  secure = 'true',
  tags = ['snowflake', 'classic', 'blocks', 'terra', 'secure_views'],
) }}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  proposer_address
FROM
  {{ ref('silver_classic__blocks') }}
