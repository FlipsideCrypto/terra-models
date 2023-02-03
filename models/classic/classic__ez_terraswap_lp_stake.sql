{{ config(
    materialized = 'view',
    secure = 'true',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'TERRASWAP',
    'PURPOSE': 'DEX' }} }
) }}

SELECT
    *
FROM
    {{ source(
        'terraswap',
        'lp_stake'
    ) }}
