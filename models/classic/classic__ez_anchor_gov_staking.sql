{{ config(
    materialized = 'view',
    secure = 'true',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'ANCHOR',
    'PURPOSE': 'STAKING' }} }
) }}

SELECT
    *
FROM
    {{ source(
        'anchor',
        'gov_staking'
    ) }}
