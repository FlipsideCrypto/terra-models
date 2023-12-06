{{ config(
   materialized = "view",
   tags = ['core']
) }}

WITH staking AS (

   SELECT
      *
   FROM
      {{ ref("silver__staking") }}
)
SELECT
   NULL AS blockchain,
   block_id,
   block_timestamp,
   tx_id,
   tx_succeeded,
   NULL AS chain_id,
   NULL AS message_index,
   action,
   delegator_address,
   amount,
   validator_address,
   validator_src_address,
   validator_label.label AS validator_label,
   validator_src_label.label AS validator_src_label,
   staking_id,
   COALESCE (
      stakings_id,
      {{ dbt_utils.generate_surrogate_key(
         ['staking_id']
      ) }}
   ) AS ez_staking_id,
   COALESCE(
      staking.inserted_timestamp,
      '2000-01-01'
   ) AS inserted_timestamp,
   COALESCE(
      staking.modified_timestamp,
      '2000-01-01'
   ) AS modified_timestamp
FROM
   staking
   LEFT OUTER JOIN {{ ref('core__dim_address_labels') }}
   validator_label
   ON validator_label.address = staking.validator_address
   LEFT OUTER JOIN {{ ref('core__dim_address_labels') }}
   validator_src_label
   ON validator_src_label.address = staking.validator_src_address
