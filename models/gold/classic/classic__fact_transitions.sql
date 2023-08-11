{{ config(
    materialized = 'view',
    tags = ['classic']
) }}

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  transition_type,
  INDEX,
  event,
  event_attributes
FROM
  {{ ref('silver_classic__transitions') }}
