{{ config(
  materialized = 'view',
  tags = ['core']
) }}

WITH messages AS (

  SELECT
    *
  FROM
    {{ ref('silver__msgs') }}
)
SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  tx_succeeded,
  msg_group,
  msg_index,
  msg_type,
  msg,
  COALESCE (
    msgs_id,
    {{ dbt_utils.generate_surrogate_key(
      ['message_id']
    ) }}
  ) AS fact_msgs_id,
  COALESCE(
    inserted_timestamp,
    '2000-01-01'
  ) AS inserted_timestamp,
  COALESCE(
    modified_timestamp,
    '2000-01-01'
  ) AS modified_timestamp
FROM
  messages
