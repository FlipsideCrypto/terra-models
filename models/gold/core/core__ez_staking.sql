{{ config(
   materialized = "view",
   secure = true
) }}

SELECT
   *
FROM
   {{ ref("defi__ez_staking") }}
