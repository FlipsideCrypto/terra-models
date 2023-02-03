{{ config(
    materialized = 'view',
    secure = 'true',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'ASTROPORT',
    'PURPOSE': 'DEX' }} }
) }}

SELECT
    *
FROM
    {{ source(
        'astroport',
        'pool_reserves'
    ) }}
