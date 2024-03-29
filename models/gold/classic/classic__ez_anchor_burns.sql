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

col_4_burns AS (
SELECT
  m.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value :sender :: STRING AS sender,
  msg_value :execute_msg :send :amount / pow(
    10,
    6
  ) AS amount,
  amount * price AS amount_usd,
  msg_value :contract :: STRING AS currency,
  msg_value :execute_msg :send :contract :: STRING AS contract_address,
  COALESCE(l.address_name, '') AS contract_label
FROM
  {{ ref('silver_classic__msgs') }}
  m
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    block_timestamp
  ) = o.hour
  AND msg_value :contract :: STRING = o.currency
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
  ON msg_value :execute_msg :send :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
WHERE
  msg_value :execute_msg :send :msg :unbond IS NOT NULL
  AND tx_status = 'SUCCEEDED'
  AND chain_id = 'columbus-4'
),

msgs AS (
SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  msg_value :sender :: STRING AS sender,
  msg_value :contract :: STRING AS currency,
  msg_value :execute_msg :send :contract :: STRING AS contract_address
FROM
   {{ ref('silver_classic__msgs') }}
WHERE
  msg_value :execute_msg :send :contract :: STRING = 'terra1mtwph2juhj0rvjz7dy92gvl6xvukaxu8rfv8ts' -- Anchor bLUNA Hub
  AND tx_status = 'SUCCEEDED'
  AND chain_id = 'columbus-5'
),

col_5_burns AS (
SELECT
 m.blockchain,
 m.chain_id,
 m.block_id,
 m.block_timestamp,
 m.tx_id,
 sender,
 event_attributes :burnt_amount / pow(
    10,
    6
  ) AS amount,
 amount * price AS amount_usd,
 m.currency,
 contract_address,
 COALESCE(l.address_name, '') AS contract_label
FROM msgs m
LEFT JOIN {{ ref('silver_classic__msg_events') }} e
ON m.tx_id = e.tx_id
LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    m.block_timestamp
  ) = o.hour
  AND m.currency = o.currency
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
  ON contract_address = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
WHERE event_type = 'from_contract'
AND event_attributes :burnt_amount IS NOT NULL
)

SELECT
blockchain,
chain_id,
block_id,
block_timestamp,
tx_id,
sender,
amount,
amount_usd,
currency,
contract_address,
contract_label
FROM col_4_burns

UNION ALL

SELECT
blockchain,
chain_id,
block_id,
block_timestamp,
tx_id,
sender,
amount,
amount_usd,
currency,
contract_address,
contract_label
FROM col_5_burns
