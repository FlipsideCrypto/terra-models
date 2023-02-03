{{ config(
    materialized = 'view',
    secure = 'true',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'AIRDROP' }} }
) }}

SELECT
    *
FROM
    {{ source(
        'terra',
        'airdrop_claims'
    ) }}
