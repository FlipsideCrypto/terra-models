{{ config(
    materialized = 'view',
    secure = 'true',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'ASTROPORT',
    'PURPOSE': 'DEX, SWAPS' }} }
) }}

SELECT
    *
FROM
    {{ source(
        'astroport',
        'swaps'
    ) }}
