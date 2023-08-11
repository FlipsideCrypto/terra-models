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
),
msgs AS (
  SELECT
    m.blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    msg_index,
    msg_value :sender :: STRING AS sender,
    msg_value :coins [0] :amount / pow(
      10,
      6
    ) AS deposit_amount,
    deposit_amount * price AS deposit_amount_usd,
    msg_value :coins [0] :denom :: STRING AS deposit_currency,
    msg_value :contract :: STRING AS contract_address,
    l.address_name AS contract_label
  FROM
    {{ ref('silver_classic__msgs') }}
    m
    LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
    ON msg_value :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
    LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      block_timestamp
    ) = o.hour
    AND msg_value :coins [0] :denom :: STRING = o.currency
  WHERE
    msg_value :execute_msg :deposit_stable IS NOT NULL
    AND msg_value :contract :: STRING = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s'
    AND tx_status = 'SUCCEEDED'
),
events AS (
  SELECT
    tx_id,
    msg_index,
    event_attributes :mint_amount / pow(
      10,
      6
    ) AS mint_amount,
    mint_amount * price AS mint_amount_usd,
    event_attributes :"1_contract_address" :: STRING AS mint_currency
  FROM
    {{ ref('silver_classic__msg_events') }}
    LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      block_timestamp
    ) = o.hour
    AND event_attributes :"1_contract_address" :: STRING = o.currency
  WHERE
    event_type = 'from_contract'
    AND tx_id IN(
      SELECT
        tx_id
      FROM
        msgs
    )
    AND event_attributes :"0_action" :: STRING = 'deposit_stable'
    AND tx_status = 'SUCCEEDED'
),

deposits AS (
SELECT
  a.blockchain,
  a.chain_id,
  a.block_id,
  a.block_timestamp,
  a.tx_id,
  a.msg_index,
  action_index,
  action_log :depositor :: STRING AS sender,
  action_log :deposit_amount / pow(
    10,
    6
  ) AS deposit_amount,
  action_log :mint_amount / pow(
    10,
    6
  ) AS mint_amount,
  coalesce(msg_value :coins [0] :denom :: STRING, 'uusd') AS deposit_currency,
  action_contract_address AS contract_address,
  CASE WHEN
  msg_value :execute_msg :process_anchor_message IS NOT NULL
  THEN 'Wormhole'
  ELSE 'Terra'
  END AS source
FROM {{ ref('silver_classic__event_actions') }} a
LEFT JOIN {{ ref('silver_classic__msgs') }} m
  ON a.tx_id = m.tx_id AND a.msg_index = m.msg_index
WHERE action_method = 'deposit_stable'
)

SELECT DISTINCT * 
FROM (
SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  sender,
  deposit_amount,
  deposit_amount_usd,
  deposit_currency,
  mint_amount,
  mint_amount_usd,
  mint_currency,
  contract_address,
  contract_label,
  'Terra' AS source
FROM
  msgs m
  JOIN events e
  ON m.tx_id = e.tx_id

UNION

SELECT
  d.blockchain,
  d.chain_id,
  d.block_id,
  d.block_timestamp,
  d.tx_id,
  sender,
  deposit_amount,
  deposit_amount * o.price AS deposit_amount_usd,
  deposit_currency,
  mint_amount,
  mint_amount * p.price AS mint_amount_usd,
  action_contract_address AS mint_currency,
  contract_address,
  l.address_name AS contract_label,
  source
FROM
  deposits d
  LEFT JOIN {{ ref('silver_classic__event_actions') }} m
  ON d.tx_id = m.tx_id 
  AND d.msg_index = m.msg_index
  AND sender = action_log :to::STRING
LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
    ON contract_address = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      d.block_timestamp
    ) = o.hour
    AND deposit_currency = o.currency
LEFT OUTER JOIN prices p
    ON DATE_TRUNC(
      'hour',
      d.block_timestamp
    ) = p.hour
    AND action_contract_address = p.currency
WHERE action_method = 'mint'
AND action_contract_address = 'terra1hzh9vpxhsk8253se0vv5jj6etdvxu3nv8z07zu' --aUST
)