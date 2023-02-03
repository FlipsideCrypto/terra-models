{{ config(
    materialized = 'view',
    secure = 'true',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, SWAPS' }} }
) }}

SELECT
    *
FROM
    {{ source(
        'terra',
        'swaps'
    ) }}
