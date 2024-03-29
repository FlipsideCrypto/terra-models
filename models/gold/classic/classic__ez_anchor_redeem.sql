{{ config(
    materialized = 'view',
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
),

redeem_events AS (
  SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  tx_status,
  msg_index,
  event_attributes
  FROM {{ ref('silver_classic__msg_events') }}
  WHERE tx_id in (select tx_id from {{ ref('silver_classic__msgs') }} WHERE msg_value:contract::string = 'terra1hzh9vpxhsk8253se0vv5jj6etdvxu3nv8z07zu')
  AND event_type = 'from_contract'
  AND event_attributes:redeem_amount is not null
  AND tx_status = 'SUCCEEDED'
),

wormhole_txs AS (
SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  action_log:redeem_amount::FLOAT / pow(
    10,
    6
  ) AS amount,
  action_contract_address AS contract_address,
  'Wormhole' AS source
FROM {{ ref('silver_classic__event_actions') }} a
WHERE action_method = 'redeem_stable'
AND tx_id IN (SELECT tx_id FROM {{ ref('silver_classic__msgs') }} WHERE msg_value :execute_msg :process_anchor_message IS NOT NULL)

),

other_txs AS (
SELECT
  a.blockchain,
  a.chain_id,
  a.block_id,
  a.block_timestamp,
  a.tx_id,
  m.msg_index,
  msg_value :sender::STRING AS sender,
  action_log:redeem_amount::FLOAT / pow(
    10,
    6
  ) AS amount,
  action_contract_address AS contract_address
FROM {{ ref('silver_classic__event_actions') }} a
LEFT JOIN {{ ref('silver_classic__msgs') }} m
ON a.tx_id = m.tx_id AND a.msg_index = m.msg_index
WHERE action_method = 'redeem_stable'
AND msg_value :execute_msg :process_anchor_message IS NULL
AND a.tx_id NOT IN (SELECT DISTINCT tx_id FROM redeem_events)
AND m.msg_index IS NOT NULL
)

SELECT DISTINCT * FROM (
  SELECT
  e.blockchain,
  e.chain_id,
  e.block_id,
  e.block_timestamp,
  e.tx_id,
  msg_value :sender :: STRING AS sender,
  event_attributes:redeem_amount::FLOAT / pow(
    10,
    6
  ) AS amount,
  amount * price AS amount_usd,
  msg_value :contract :: STRING AS currency,
  COALESCE(msg_value :execute_msg :send :contract :: STRING, '') AS contract_address,
  COALESCE(l.address_name, '') AS contract_label,
  'Terra' AS source
FROM redeem_events e
JOIN {{ ref('silver_classic__msgs') }} m
ON e.tx_id = m.tx_id
AND e.msg_index = m.msg_index
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
  ON msg_value :execute_msg :send :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  LEFT OUTER JOIN prices r
  ON DATE_TRUNC(
    'hour',
    e.block_timestamp
  ) = HOUR
  AND msg_value :contract :: STRING = r.currency
WHERE e.tx_status = 'SUCCEEDED'

UNION

SELECT
  w.blockchain,
  w.chain_id,
  w.block_id,
  w.block_timestamp,
  w.tx_id,
  action_log :from::STRING AS sender,
  amount,
  amount * price AS amount_usd,
  action_contract_address AS currency,
  contract_address,
  COALESCE(l.address_name, '') AS contract_label,
  source
FROM wormhole_txs w
LEFT JOIN {{ ref('silver_classic__event_actions') }} e
ON w.tx_id = e.tx_id
LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
  ON w.contract_address = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  LEFT OUTER JOIN prices r
  ON DATE_TRUNC(
    'hour',
    w.block_timestamp
  ) = HOUR
  AND action_contract_address = r.currency
WHERE action_method = 'send'
AND action_log :to::STRING = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s' --Anchor market

UNION

SELECT
  o.blockchain,
  o.chain_id,
  o.block_id,
  o.block_timestamp,
  o.tx_id,
  COALESCE(o.sender, action_log :from::STRING) AS sender,
  amount,
  amount * price AS amount_usd,
  action_contract_address AS currency,
  contract_address,
  COALESCE(l.address_name, '') AS contract_label,
  'Terra' AS source
FROM other_txs o
LEFT JOIN {{ ref('silver_classic__event_actions') }} e
ON o.tx_id = e.tx_id
LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
  ON contract_address = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  LEFT OUTER JOIN prices r
  ON DATE_TRUNC(
    'hour',
    o.block_timestamp
  ) = HOUR
  AND action_contract_address = r.currency
WHERE action_method = 'send'
AND action_log :to::STRING = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s' --Anchor market
)
