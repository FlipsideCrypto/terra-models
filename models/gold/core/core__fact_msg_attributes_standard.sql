{{ config(
  materialized = 'view',
  tags = ['core']
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
  attribute_value,
  COALESCE (
    msg_attributes_2_id,
    {{ dbt_utils.generate_surrogate_key(
      ['_unique_key']
    ) }}
  ) AS fact_msg_attributes_standard_id,
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
