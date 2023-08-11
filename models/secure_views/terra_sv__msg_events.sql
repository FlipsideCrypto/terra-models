{{ config(
  materialized = 'view',
  secure = 'true',
  tags = ['snowflake', 'classic', 'msg_events', 'terra', 'secure_views']
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
  msg_index,
  msg_module,
  msg_type,
  event_index,
  event_type,
  event_attributes
FROM
  {{ ref('silver_classic__msg_events') }}
