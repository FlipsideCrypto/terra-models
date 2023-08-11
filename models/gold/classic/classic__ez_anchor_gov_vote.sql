{{ config(
    materialized = 'view',
    secure = 'true',
    tags = ['classic'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'ANCHOR' }} }
) }}

SELECT
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value :sender :: STRING AS voter,
  msg_value :execute_msg :cast_vote :poll_id AS poll_id,
  msg_value :execute_msg :cast_vote :vote :: STRING AS vote,
  msg_value :execute_msg :cast_vote :amount / pow(
    10,
    6
  ) AS balance,
  msg_value :contract :: STRING AS contract_address,
  l.address_name AS contract_label
FROM
  {{ ref('silver_classic__msgs') }}
  m
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
  ON msg_value :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
WHERE
  msg_value :contract :: STRING = 'terra1f32xyep306hhcxxxf7mlyh0ucggc00rm2s9da5' -- ANC Governance
  AND msg_value :execute_msg :cast_vote IS NOT NULL
  AND tx_status = 'SUCCEEDED'
