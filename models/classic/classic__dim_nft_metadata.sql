{{ config(
    materialized = 'view',
    secure = 'true',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} }
) }}

SELECT
    *
FROM
    {{ source(
        'terra',
        'nft_metadata'
    ) }}
