{{ config(
    materialized = 'view',
    tags = ['classic']
) }}

WITH withdraw_delegator_rewards AS (

  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    msg_index,
    event_attributes_amount_index,
    'withdraw_delegator_rewards' AS action,
    event_rewards_amount AS amount,
    event_rewards_currency AS currency,
    validator_address AS validator,
    delegator_address AS delegator
  FROM
    {{ ref('silver_classic__withdraw_delegator_rewards') }}
),
withdraw_validator_commission AS (
  SELECT
    blockchain,
    chain_id,
    tx_status,
    block_id,
    block_timestamp,
    tx_id,
    msg_index,
    event_attributes_amount_index,
    'withdraw_validator_commission' AS action,
    amount,
    currency,
    validator_address AS validator,
    delegator_address AS delegator
  FROM
    {{ ref('silver_classic__withdraw_validator_commission') }}
),
prices AS (
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
  GROUP BY
    1,
    2,
    3
)
SELECT
  A.blockchain,
  A.chain_id,
  A.tx_status,
  A.block_id,
  A.block_timestamp,
  A.tx_id,
  A.msg_index,
  A.event_attributes_amount_index,
  A.action,
  A.validator AS validator,
  validator_labels.l1_label AS validator_label_type,
  validator_labels.l2_label AS validator_label_subtype,
  validator_labels.project_name AS validator_address_label,
  validator_labels.address_name AS validator_address_name,
  A.delegator,
  delegator_labels.l1_label AS delegator_label_type,
  delegator_labels.l2_label AS delegator_label_subtype,
  delegator_labels.project_name AS delegator_address_label,
  delegator_labels.address_name AS delegator_address_name,
  A.amount AS event_amount,
  price_usd,
  A.amount * price_usd AS event_amount_usd,
  p.symbol AS currency
FROM
  (
    SELECT
      *
    FROM
      withdraw_delegator_rewards
    UNION ALL
    SELECT
      *
    FROM
      withdraw_validator_commission
  ) A
  LEFT OUTER JOIN prices p
  ON p.currency = A.currency
  AND p.hour = DATE_TRUNC(
    'hour',
    A.block_timestamp
  )
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }}
  delegator_labels
  ON A.delegator = delegator_labels.address
  AND delegator_labels.blockchain = 'terra'
  AND delegator_labels.creator = 'flipside'
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }}
  validator_labels
  ON A.validator = validator_labels.address
  AND validator_labels.blockchain = 'terra'
  AND validator_labels.creator = 'flipside'

