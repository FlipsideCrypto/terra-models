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
   staking_id
FROM
   staking
   LEFT OUTER JOIN {{ ref('core__dim_address_labels') }}
   validator_label
   ON validator_label.address = staking.validator_address
   LEFT OUTER JOIN {{ ref('core__dim_address_labels') }}
   validator_src_label
   ON validator_src_label.address = staking.validator_src_address
