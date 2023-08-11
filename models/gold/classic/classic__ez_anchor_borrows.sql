{{ config(
    materialized = 'view',
    secure = 'true',
    tags = ['classic'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'ANCHOR' }} }
) }}

WITH prices AS (

  SELECT
    DATE_TRUNC(
      'hour',
      block_timestamp
    ) AS HOUR,
    currency,
    symbol,
    AVG(price_usd) AS price
  FROM
    {{ ref('classic__dim_oracle_prices') }}
  WHERE
    1 = 1
GROUP BY
  1,
  2,
  3
)

SELECT DISTINCT * 
FROM (
SELECT
  a.blockchain,
  a.chain_id,
  a.block_id,
  a.block_timestamp,
  a.tx_id,
  action_log :borrower::STRING AS sender,
  action_log :borrow_amount / POW(10,6) AS amount,
  amount * price AS amount_usd,
  'uusd' AS currency,
  action_contract_address AS contract_address,
  l.address_name AS contract_label,
  CASE WHEN
  msg_value :execute_msg :process_anchor_message IS NOT NULL
  THEN 'Wormhole'
  ELSE 'Terra'
  END AS source
FROM
  {{ ref('silver_classic__event_actions') }} a
  LEFT JOIN {{ ref('silver_classic__msgs') }} m
  ON a.tx_id = m.tx_id AND a.msg_index = m.msg_index
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    a.block_timestamp
  ) = o.hour
  AND 'uusd' = o.currency
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
  ON action_contract_address = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
WHERE
  action_method = 'borrow_stable' -- Anchor Borrow
  AND action_contract_address = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s' -- Anchor Market Contract
  
UNION

SELECT
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value :sender :: STRING AS sender,
  msg_value :execute_msg :borrow_stable :borrow_amount / pow(
    10,
    6
  ) AS amount,
  amount * price AS amount_usd,
  'uusd' AS currency,
  msg_value :contract :: STRING AS contract_address,
  l.address_name AS contract_label,
  'Terra' AS source
FROM
  {{ ref('silver_classic__msgs') }}
  m
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    block_timestamp
  ) = o.hour
  AND 'uusd' = o.currency
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
  ON msg_value :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
WHERE
  msg_value :execute_msg :borrow_stable IS NOT NULL -- Anchor Borrow
  AND msg_value :contract :: STRING = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s' -- Anchor Market Contract
  AND tx_status = 'SUCCEEDED'
)