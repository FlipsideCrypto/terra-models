{{ config(
    materialized = 'view',
    tags = ['classic']
) }}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  tx_status,
  tx_module,
  tx_type,
  tx_memo,
  msg_index,
  msg_module,
  msg_type,
  msg_value
FROM
  {{ ref('silver_classic__msgs') }}
