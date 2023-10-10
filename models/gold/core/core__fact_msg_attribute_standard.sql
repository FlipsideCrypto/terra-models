{{ config(
  materialized = 'view',
  secure = true
) }}

WITH msg_attributes AS (

  SELECT
    *
  FROM
    {{ ref('silver__msg_attributes_2') }}
)
SELECT
  block_id,
  block_timestamp,
  tx_id,
  tx_succeeded,
  msg_group,
  msg_sub_group,
  msg_index,
  msg_type,
  attribute_index,
  attribute_key,
  attribute_value
FROM
  msg_attributes
