{{ config(
    materialized = 'view',
    secure = 'true',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }} }
) }}

SELECT
    *
FROM
    {{ source(
        'terra',
        'staking'
    ) }}
