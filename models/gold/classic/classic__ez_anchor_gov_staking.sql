{{ config(
    materialized = 'view',
    secure = 'true',
    tags = ['classic'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'ANCHOR',
    'PURPOSE': 'STAKING' }} }
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
stake_msgs AS (
  SELECT
    t.blockchain,
    chain_id,
    block_id,
    block_timestamp,
    tx_id,
    msg_value :sender :: STRING AS sender,
    msg_value :execute_msg :send :amount / pow(
      10,
      6
    ) AS amount,
    amount * o.price AS amount_usd,
    msg_value :contract :: STRING AS currency,
    msg_value :execute_msg :send :contract :: STRING AS contract_address,
    l.address_name AS contract_label
  FROM
    {{ ref('silver_classic__msgs') }}
    t
    LEFT OUTER JOIN prices o
    ON DATE_TRUNC(
      'hour',
      t.block_timestamp
    ) = o.hour
    AND msg_value :contract :: STRING = o.currency
    LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
    ON msg_value :execute_msg :send :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
  WHERE
    (
      msg_value :execute_msg :send :msg :stake_voting_tokens IS NOT NULL
      AND msg_value :execute_msg :send :contract :: STRING = 'terra1f32xyep306hhcxxxf7mlyh0ucggc00rm2s9da5'
      AND tx_status = 'SUCCEEDED'
    )
    OR 
    (
      msg_value:execute_msg:send:msg::string = 'eyJzdGFrZV92b3RpbmdfdG9rZW5zIjp7fX0='
      AND tx_status = 'SUCCEEDED'
    )
),
stake_events AS (
  SELECT
    tx_id,
    msg_index,
    event_attributes :share AS shares
  FROM
    {{ ref('silver_classic__msg_events') }}
  WHERE
    tx_id IN(
      SELECT
        DISTINCT tx_id
      FROM
        stake_msgs
    )
    AND event_type = 'from_contract'
    AND event_attributes :share IS NOT NULL
) -- Staking
SELECT
  blockchain,
  chain_id,
  block_id,
  block_timestamp,
  m.tx_id,
  msg_index,
  'stake' AS event_type,
  sender,
  amount,
  amount_usd,
  currency,
  shares,
  contract_address,
  contract_label
FROM
  stake_msgs m
  JOIN stake_events e
  ON m.tx_id = e.tx_id
UNION
  -- Unstaking
SELECT
  t.blockchain,
  chain_id,
  block_id,
  block_timestamp,
  tx_id,
  null AS msg_index,
  'unstake' AS event_type,
  msg_value :sender :: STRING AS sender,
  msg_value :execute_msg :withdraw_voting_tokens :amount / pow(
    10,
    6
  ) AS amount,
  amount * o.price AS amount_usd,
  'terra14z56l0fp2lsf86zy3hty2z47ezkhnthtr9yq76' AS currency,
  NULL AS shares,
  msg_value :contract :: STRING AS contract_address,
  l.address_name AS contract_label
FROM
  {{ ref('silver_classic__msgs') }}
  t
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    t.block_timestamp
  ) = o.hour
  AND 'terra14z56l0fp2lsf86zy3hty2z47ezkhnthtr9yq76' = o.currency
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
  ON msg_value :contract :: STRING = l.address AND l.blockchain = 'terra' AND l.creator = 'flipside'
WHERE
  msg_value :execute_msg :withdraw_voting_tokens IS NOT NULL
  AND msg_value :contract :: STRING = 'terra1f32xyep306hhcxxxf7mlyh0ucggc00rm2s9da5'
  AND tx_status = 'SUCCEEDED'


