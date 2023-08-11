{{ config(
    materialized = 'view',
    tags = ['classic']
) }}

SELECT
  chain_id AS blockchain,
  block_timestamp,
  block_number,
  tax_rate
FROM
  {{ ref(
    'silver_classic__tax_rate'
  ) }}
WHERE
  1 = 1