{{ config(
  materialized = 'table',
  tags = ['classic']
) }}

WITH prices AS (

  SELECT
    DATE_TRUNC(
      'hour',
      recorded_at
    ) AS block_timestamp,
    symbol AS currency,
    AVG(price) AS price
  FROM
    {{ source(
      'shared',
      'legacy_prices'
    ) }}
  WHERE
    asset_id = '4172'
    AND provider IS NOT NULL
  GROUP BY
    1,
    2
),
other_prices AS (
  SELECT
    DATE_TRUNC(
      'hour',
      recorded_at
    ) AS block_timestamp,
    symbol AS currency,
    AVG(price) AS price
  FROM
    {{ source(
      'shared',
      'legacy_prices'
    ) }}
  WHERE
    asset_id IN(
      '7857',
      '8857'
    )
  GROUP BY
    1,
    2
),
luna_rate AS (
  SELECT
    blockchain,
    chain_id,
    block_timestamp,
    block_id,
    REGEXP_REPLACE(
      event_attributes :denom :: STRING,
      '\"',
      ''
    ) AS currency,
    event_attributes :exchange_rate :: FLOAT AS exchange_rate
  FROM
    {{ ref('silver_classic__transitions') }}
  WHERE
    event = 'exchange_rate_update'
),
polymine AS (
  SELECT
    'terra' AS blockchain,
    DATE_TRUNC(
      'minute',
      recorded_at
    ) AS block_timestamp,
    'terra1kcthelkax4j9x8d3ny6sdag0qmxxynl3qtcrpy' AS currency,
    symbol,
    AVG(
      p.price
    ) AS price,
    'coinmarketcap' AS source
  FROM
    {{ source(
      'shared',
      'legacy_prices'
    ) }}
    p
  WHERE
    asset_id IN(
      'pylon-protocol',
      '10767'
    )
  GROUP BY
    1,
    2,
    3,
    4,
    6
),
feed_prices AS (
  SELECT
    m.blockchain,
    DATE_TRUNC(
      'hour',
      m.block_timestamp
    ) AS block_timestamp,
    f.value [0] :: STRING AS currency,
    AVG(
      f.value [1] :: FLOAT
    ) AS price_usd
  FROM
    {{ ref('silver_classic__msgs') }}
    m,
    LATERAL FLATTEN (
      input => msg_value :execute_msg :feed_price :prices
    ) f
  WHERE
    msg_value :execute_msg :feed_price IS NOT NULL
    AND tx_status = 'SUCCEEDED'
  GROUP BY
    1,
    2,
    3
)
SELECT
  blockchain,
  l.block_timestamp,
  l.currency,
  CASE
    WHEN l.currency = 'usgd' THEN 'SGT'
    WHEN l.currency = 'uusd' THEN 'UST'
    WHEN l.currency = 'ukrw' THEN 'KRT'
    WHEN l.currency = 'unok' THEN 'NOT'
    WHEN l.currency = 'ucny' THEN 'CNT'
    WHEN l.currency = 'uinr' THEN 'INT'
    WHEN l.currency = 'ueur' THEN 'EUT'
    WHEN l.currency = 'udkk' THEN 'DKT'
    WHEN l.currency = 'uhkd' THEN 'HKT'
    WHEN l.currency = 'usek' THEN 'SET'
    WHEN l.currency = 'uthb' THEN 'THT'
    WHEN l.currency = 'umnt' THEN 'MNT'
    WHEN l.currency = 'ucad' THEN 'CAT'
    WHEN l.currency = 'ugbp' THEN 'GBT'
    WHEN l.currency = 'ujpy' THEN 'JPT'
    WHEN l.currency = 'usdr' THEN 'SDT'
    WHEN l.currency = 'uchf' THEN 'CHT'
    WHEN l.currency = 'uaud' THEN 'AUT'
    WHEN l.currency = 'uidr' THEN 'IDT'
    WHEN l.currency = 'uphp' THEN 'PHT'
    WHEN l.currency = 'utwd' THEN 'TWT'
    WHEN l.currency = 'umyr' THEN 'MYT'
    ELSE l.currency
  END AS symbol,
  exchange_rate AS luna_exchange_rate,
  CASE
    WHEN (
      price / exchange_rate
    ) IS NULL THEN (LAST_VALUE(price ignore nulls) over (PARTITION BY symbol
    ORDER BY
      l.block_timestamp ASC rows BETWEEN unbounded preceding
      AND CURRENT ROW)) / exchange_rate
      ELSE price / exchange_rate
  END AS price_usd,
  'oracle' AS source
FROM
  luna_rate l
  LEFT OUTER JOIN prices p
  ON DATE_TRUNC(
    'hour',
    l.block_timestamp
  ) = p.block_timestamp
UNION
SELECT
  'terra' AS blockchain,
  block_timestamp,
  'uluna' AS currency,
  'LUNA' AS symbol,
  1 AS luna_exchange_rate,
  price AS price_usd,
  'coinmarketcap' AS source
FROM
  prices
UNION
SELECT
  'terra' AS blockchain,
  o.block_timestamp,
  CASE
    WHEN o.currency = 'MIR' THEN 'terra15gwkyepfc6xgca5t5zefzwy42uts8l2m4g40k6'
    WHEN o.currency = 'ANC' THEN 'terra14z56l0fp2lsf86zy3hty2z47ezkhnthtr9yq76'
    ELSE NULL
  END AS currency,
  o.currency AS symbol,
  x.price / o.price AS luna_exchange_rate,
  o.price AS price_usd,
  'coinmarketcap' AS source
FROM
  other_prices o
  LEFT OUTER JOIN prices x
  ON DATE_TRUNC(
    'hour',
    o.block_timestamp
  ) = x.block_timestamp
UNION
SELECT
  ee.blockchain,
  ee.block_timestamp,
  ee.currency,
  l.address_name AS symbol,
  CASE
    WHEN ee.currency = 'terra1z3e2e4jpk4n0xzzwlkgcfvc95pc5ldq0xcny58' THEN pp.price / (ee.price_usd * pow(10, 2))
    ELSE pp.price / ee.price_usd
  END AS luna_exchange_rate,
  CASE
    WHEN ee.currency = 'terra1z3e2e4jpk4n0xzzwlkgcfvc95pc5ldq0xcny58' THEN ee.price_usd * pow(
      10,
      2
    )
    ELSE ee.price_usd
  END AS price_usd,
  'oracle' AS source
FROM
  feed_prices ee
  LEFT OUTER JOIN {{ ref('silver_classic__address_labels') }} AS l
  ON ee.currency = l.address
  AND l.blockchain = 'terra'
  AND l.creator = 'flipside'
  LEFT OUTER JOIN prices pp
  ON DATE_TRUNC(
    'hour',
    ee.block_timestamp
  ) = pp.block_timestamp
UNION
SELECT
  p.blockchain,
  p.block_timestamp,
  p.currency,
  p.symbol,
  l.price / p.price AS luna_exchange_rate,
  p.price AS price_usd,
  p.source
FROM
  polymine p
  LEFT OUTER JOIN prices l
  ON DATE_TRUNC(
    'hour',
    p.block_timestamp
  ) = l.block_timestamp
