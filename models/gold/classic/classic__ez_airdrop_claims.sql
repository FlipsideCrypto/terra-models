{{ config(
    materialized = 'view',
    secure = 'true',
    tags = ['classic'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'AIRDROP' }} }
) }}

SELECT
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value :execute_msg :claim :stage :: STRING AS airdrop_id,
  msg_value :sender :: STRING AS claimer,
  msg_value :execute_msg :claim :amount / pow(10,6) AS amount,
  msg_value :contract :: STRING AS contract_address,
  l.address_name AS contract_label
FROM {{ ref('silver_classic__msgs') }} m
  
LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
  ON msg_value :contract :: STRING = l.address 
  AND l.blockchain = 'terra' 
  AND l.creator = 'flipside'

WHERE msg_value :execute_msg :claim :amount IS NOT NULL
  AND tx_status = 'SUCCEEDED'
