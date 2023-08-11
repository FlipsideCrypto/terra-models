{{ config(
    materialized = 'view',
    tags = ['classic'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} }
) }}

WITH balances AS (

  SELECT
    DATE,
    address,
    balance
  FROM {{ ref('silver_classic__daily_balances') }}
  WHERE balance_type = 'staked'
    AND address IN(
      SELECT
        DISTINCT msg_value :voter :: STRING
      FROM
        {{ ref('silver_classic__msgs') }}
      WHERE
        msg_module = 'gov'
        AND msg_type = 'gov/MsgVote'
        AND tx_status = 'SUCCEEDED'

)
),
voting_power AS (

SELECT 
  block_timestamp,
  address,
  voting_power,
  row_number() OVER (PARTITION BY date_trunc('day', block_timestamp), address ORDER BY block_timestamp DESC) as rn
FROM {{ source(
    'bronze',
    'classic_validator_voting_power'
    ) }}

WHERE 1=1
),

validator_address AS (

SELECT 
  date_trunc('day', block_timestamp) as date,
  address,
  delegator_address,
  voting_power
FROM voting_power

LEFT OUTER JOIN {{ ref('classic__dim_validator_labels') }}
  ON address = vp_address

WHERE rn = 1

)

SELECT
  A.blockchain,
  chain_id,
  tx_status,
  block_id,
  block_timestamp,
  tx_id,
  msg_type,
  REGEXP_REPLACE(msg_value :voter,'\"','') AS voter,
  voter_labels.l1_label AS voter_label_type,
  voter_labels.l2_label AS voter_label_subtype,
  voter_labels.project_name AS voter_address_label,
  voter_labels.address AS voter_address_name,
  REGEXP_REPLACE(msg_value :proposal_id,'\"','') AS proposal_id,
  REGEXP_REPLACE(msg_value :option,'\"','') AS "OPTION",
  CASE 
    WHEN delegator_address IS NOT NULL THEN voting_power 
    ELSE b.balance
  END AS voting_power
FROM {{ ref('silver_classic__msgs') }} A
  
LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS voter_labels
  ON msg_value :voter = voter_labels.address
  AND voter_labels.blockchain = 'terra'
  AND voter_labels.creator = 'flipside'
  
LEFT OUTER JOIN balances b
  ON DATE(A.block_timestamp) = DATE(b.date)
  AND msg_value :voter :: STRING = b.address

LEFT OUTER JOIN validator_address va
  ON va.date = date_trunc('day', A.block_timestamp)
  AND msg_value:voter::STRING = va.delegator_address

WHERE msg_module = 'gov'
  AND msg_type = 'gov/MsgVote'
  AND tx_status = 'SUCCEEDED'

