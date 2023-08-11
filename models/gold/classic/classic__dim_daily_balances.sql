{{ config(
    materialized = 'view',
    secure = 'true',
    tags = ['classic'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BALANCES' }} }
) }}

WITH prices AS (

  SELECT
    p.symbol,
    l.address_name,
    DATE_TRUNC(
      'day',
      block_timestamp
    ) AS DAY,
    AVG(price_usd) AS price
  FROM
    {{ ref('classic__dim_oracle_prices') }} p
  LEFT JOIN {{ ref('classic__dim_labels') }} l
  on p.symbol = l.address
  where 1=1
  GROUP BY
    p.symbol,
    DAY,
    l.address_name
)

SELECT
  DATE,
  b.address,
  address_labels.l1_label AS address_label_type,
  address_labels.l2_label AS address_label_subtype,
  address_labels.project_name AS address_label,
  address_labels.address_name AS address_name,
  balance,
  balance * p.price AS balance_usd,
  b.balance_type,
  b.is_native,
  currency
FROM
  {{ ref('silver_classic__daily_balances') }} b
  LEFT OUTER JOIN prices p
  ON p.symbol = case when b.currency = 'USD' then 'UST'
            when len(b.currency) = 3 then upper(concat(substring(b.currency,1,2),'T'))
            when substring(b.currency,1,1) = 'U' then upper(concat(substring(b.currency,2,2),'T'))
            else b.currency end
  AND p.day = b.date
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS address_labels
  ON b.address = address_labels.address AND address_labels.blockchain = 'terra' AND address_labels.creator = 'flipside'
WHERE
  lower(b.currency) not like 'ibc%'
  and b.currency <> 'UNOK'

UNION

SELECT
  DATE,
  b.address,
  address_labels.l1_label AS address_label_type,
  address_labels.l2_label AS address_label_subtype,
  address_labels.project_name AS address_label,
  address_labels.address_name AS address_name,
  balance,
  CASE
  WHEN p.price * balance IS NULL
  THEN (last_value(p.price ignore nulls) over (partition by currency order by DATE asc rows between unbounded preceding and current row)) * balance
  ELSE balance * p.price
  END AS balance_usd,
  b.balance_type,
  b.is_native,
  currency
FROM {{ ref('silver_classic__block_synthetic_balances') }} b
  
LEFT OUTER JOIN prices p
  ON p.symbol = currency
  AND p.day = b.date
  
LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS address_labels
  ON b.address = address_labels.address 
  AND address_labels.blockchain = 'terra' 
  AND address_labels.creator = 'flipside'