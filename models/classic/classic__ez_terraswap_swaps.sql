{{ config(
    materialized = 'view',
    secure = 'true',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'TERRASWAP',
    'PURPOSE': 'DEX, SWAPS' }} }
) }}

SELECT
    *
FROM
    {{ source(
        'terraswap',
        'swaps'
    ) }}
