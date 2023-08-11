{{ config(
    materialized = 'view',
    secure = 'true',
    tags = ['classic'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} }
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
  GROUP BY
    1,
    2,
    3
),
proposal_id AS (
  SELECT
    tx_id,
    event_attributes :proposal_id AS proposal_id
  FROM
    {{ ref('silver_classic__msg_events') }}
  WHERE
    msg_module = 'gov'
    AND msg_type = 'gov/MsgSubmitProposal'
    AND tx_status = 'SUCCEEDED'
    AND event_type = 'proposal_deposit'
)
SELECT
  t.blockchain,
  t.chain_id,
  t.tx_status,
  t.block_id,
  t.block_timestamp,
  t.tx_id,
  t.msg_type,
  REGEXP_REPLACE(
    msg_value :proposer,
    '\"',
    ''
  ) AS proposer,
  proposer_labels.l1_label AS proposer_label_type,
  proposer_labels.l2_label AS proposer_label_subtype,
  proposer_labels.project_name AS proposer_address_label,
  proposer_labels.address AS proposer_address_name,
  p.proposal_id,
  COALESCE(
    msg_value :content :type :: STRING,
    msg_value :"content"."@type" :: STRING -- columbus-5
  ) AS proposal_type,
  COALESCE(
    msg_value :content :value :description :: STRING,
    msg_value :content :description :: STRING --columbus-5
  ) AS description,
  COALESCE(
    msg_value :content :value :title :: STRING,
    msg_value :content :title :: STRING --columbus-5
  ) AS title,
  msg_value :initial_deposit [0] :amount / pow(
    10,
    6
  ) AS deposit_amount,
  deposit_amount * o.price_usd AS deposit_amount_usd,
  msg_value :initial_deposit [0] :denom :: STRING AS deposit_currency
FROM
  {{ ref('silver_classic__msgs') }}
  t
  LEFT OUTER JOIN prices o
  ON DATE_TRUNC(
    'hour',
    t.block_timestamp
  ) = o.hour
  AND REGEXP_REPLACE(
    t.msg_value :initial_deposit [0] :denom,
    '\"',
    ''
  ) = o.currency
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS proposer_labels
  ON REGEXP_REPLACE(
    t.msg_value :proposer,
    '\"',
    ''
  ) = proposer_labels.address
  AND proposer_labels.blockchain = 'terra'
  AND proposer_labels.creator = 'flipside'
  LEFT OUTER JOIN proposal_id p
  ON t.tx_id = p.tx_id
WHERE
  msg_module = 'gov'
  AND msg_type = 'gov/MsgSubmitProposal'
  AND tx_status = 'SUCCEEDED'