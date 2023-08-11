{{ config(
    materialized = 'view',
    secure = 'true',
    tags = ['classic']
) }}

SELECT
  block_number AS block_id,
  block_timestamp,
  blockchain,
  address,
  voting_power
FROM
  {{ source(
    'bronze',
    'classic_validator_voting_power'
  ) }}

qualify(ROW_NUMBER() over (PARTITION BY block_id, address
ORDER BY
  block_timestamp DESC)) = 1
