{{ config(
  materialized = 'view',
  tags = ['core']
) }}

WITH msg_attributes AS (

  SELECT
    *
  FROM
    {{ ref('silver__msg_attributes') }}
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
  attribute_key,
  attribute_value,
  attribute_index,
  COALESCE (
    msg_attributes_id,
    {{ dbt_utils.generate_surrogate_key(
      ['message_id']
    ) }}
  ) AS fact_msg_attributes_id,
  COALESCE(
    inserted_timestamp,
    '2000-01-01'
  ) AS inserted_timestamp,
  COALESCE(
    modified_timestamp,
    '2000-01-01'
  ) AS modified_timestamp
FROM
  msg_attributes
