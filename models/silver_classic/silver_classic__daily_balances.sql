{{ config(
  materialized = 'incremental',
  unique_key = 'date',
  incremental_strategy = 'delete+insert',
  cluster_by = ['date'],
  tags = ['snowflake', 'classic', 'silver_classic__daily_balances']
) }}

WITH base_balances AS (
SELECT
  block_timestamp, address, currency, balance_type, blockchain, balance
FROM
  {{ ref('silver_classic__balances') }}
),

address_ranges AS (
  SELECT
    address,
    currency,
    balance_type,
    'terra' AS blockchain,
    MIN(
      block_timestamp :: DATE
    ) AS min_block_date,
    MAX(
      CURRENT_TIMESTAMP :: DATE
    ) AS max_block_date
  FROM
    base_balances
  GROUP BY
    1,
    2,
    3,
    4
),

cte_my_date AS (
  SELECT
    DATE_HOUR :: DATE AS DATE
  FROM
    {{ source(
      'crosschain_core',
      'dim_date_hours'
    ) }}
  GROUP BY
    1
),

all_dates AS (
  SELECT
    C.date,
    A.address,
    A.currency,
    A.balance_type,
    A.blockchain
  FROM
    cte_my_date C
    LEFT JOIN address_ranges A
    ON C.date BETWEEN A.min_block_date
    AND A.max_block_date
  WHERE
    A.address IS NOT NULL
),

terra_balances AS (
  SELECT
    address,
    currency,
    block_timestamp,
    balance_type,
    'terra' AS blockchain,
    balance
  FROM
    base_balances qualify(ROW_NUMBER() over(PARTITION BY address, currency, block_timestamp :: DATE, balance_type
  ORDER BY
    block_timestamp DESC)) = 1
),

balance_tmp AS (
  SELECT
    d.date,
    d.address,
    d.currency,
    CASE
      WHEN d.currency IN (
        'LUNA',
        'UST',
        'KRT',
        'MNT',
        'SDT',
        'krw',
        'mnt',
        'sdr'
      ) THEN b.balance
      ELSE b.balance / pow(
        10,
        6
      )
    END AS balance,
    d.balance_type,
    d.blockchain
  FROM
    all_dates d
    LEFT JOIN terra_balances b
    ON d.date = b.block_timestamp :: DATE
    AND d.address = b.address
    AND d.currency = b.currency
    AND d.balance_type = b.balance_type
    AND d.blockchain = b.blockchain
)

SELECT
  DATE,
  address,
  currency,
  balance_type,
  blockchain,
  TRUE AS is_native,
  LAST_VALUE(
    balance ignore nulls
  ) over(
    PARTITION BY address,
    currency,
    balance_type,
    blockchain
    ORDER BY
      DATE ASC rows unbounded preceding
  ) AS balance
FROM
  balance_tmp