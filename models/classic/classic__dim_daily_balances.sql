{{ config(
    materialized = 'view',
    secure = 'true',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BALANCES' }} }
) }}

SELECT
    *
FROM
    {{ source(
        'terra',
        'daily_balances'
    ) }}
