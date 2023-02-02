{{ config(
    materialized = 'view',
    secure = 'true',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} }
) }}

SELECT
    *
FROM
    {{ source(
        'terra',
        'gov_submit_proposal'
    ) }}
