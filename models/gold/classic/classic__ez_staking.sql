{{ config(
    materialized = 'view',
    secure = 'true',
    tags = ['classic'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }} }
) }}

WITH prices AS (

  SELECT
    DATE_TRUNC(
      'hour',
      block_timestamp
    ) AS HOUR,
    currency,
    symbol,
    AVG(price_usd) AS price_usd
  FROM
    {{ ref('classic__dim_oracle_prices') }}
  WHERE
    1 = 1
GROUP BY
  1,
  2,
  3
),
delegate AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    msg_index,
    'delegate' AS action,
    msg_value :delegator_address :: STRING AS delegator_address,
    msg_value :validator_address :: STRING AS validator_address,
    REGEXP_REPLACE(msg_value :amount :amount / pow(10, 6), '\"', '') AS amount,
    msg_value :amount :denom :: STRING AS currency
  FROM
    {{ ref('silver_classic__msgs') }}
  WHERE
    msg_module = 'staking'
    AND msg_type = 'staking/MsgDelegate'
    AND tx_status = 'SUCCEEDED'

),
undelegate AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    msg_index,
    'undelegate' AS action,
    msg_value :delegator_address :: STRING AS delegator_address,
    msg_value :validator_address :: STRING AS validator_address,
    REGEXP_REPLACE(msg_value :amount :amount / pow(10, 6), '\"', '') AS amount,
    msg_value :amount :denom :: STRING AS currency
  FROM
    {{ ref('silver_classic__msgs') }}
  WHERE
    msg_module = 'staking'
    AND msg_type = 'staking/MsgUndelegate'
    AND tx_status = 'SUCCEEDED'
),
redelegate AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    msg_index,
    'redelegate' AS action,
    msg_value :delegator_address :: STRING AS delegator_address,
    msg_value :validator_dst_address :: STRING AS validator_address,
    REGEXP_REPLACE(msg_value :amount :amount / pow(10, 6), '\"', '') AS amount,
    msg_value :amount :denom :: STRING AS currency
  FROM
    {{ ref('silver_classic__msgs') }}
  WHERE
    msg_module = 'staking'
    AND msg_type = 'staking/MsgBeginRedelegate'
    AND tx_status = 'SUCCEEDED'
)
SELECT
  A.blockchain,
  A.chain_id,
  A.tx_status,
  A.block_id,
  A.block_timestamp,
  A.msg_index,
  A.tx_id,
  A.action,
  A.delegator_address,
  delegator_labels.l1_label AS delegator_label_type,
  delegator_labels.l2_label AS delegator_label_subtype,
  delegator_labels.project_name AS delegator_address_label,
  delegator_labels.address AS delegator_address_name,
  A.validator_address,
  validator_labels.l1_label AS validator_label_type,
  validator_labels.l2_label AS validator_label_subtype,
  validator_labels.project_name AS validator_address_label,
  validator_labels.address AS validator_address_name,
  A.amount :: FLOAT AS event_amount,
  price_usd,
  A.amount * price_usd AS event_amount_usd,
  p.symbol AS currency
FROM
  (
    SELECT
      *
    FROM
      delegate
    UNION ALL
    SELECT
      *
    FROM
      undelegate
    UNION ALL
    SELECT
      *
    FROM
      redelegate
  ) A
  LEFT OUTER JOIN prices p
  ON p.currency = A.currency
  AND p.hour = DATE_TRUNC(
    'hour',
    A.block_timestamp
  )
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }}
  delegator_labels
  ON A.delegator_address = delegator_labels.address
  AND delegator_labels.blockchain = 'terra'
  AND delegator_labels.creator = 'flipside'
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }}
  validator_labels
  ON A.validator_address = validator_labels.address
  AND validator_labels.blockchain = 'terra'
  AND validator_labels.creator = 'flipside'

