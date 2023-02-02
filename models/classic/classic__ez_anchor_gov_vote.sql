{{ config(
    materialized = 'view',
    secure = 'true',
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'ANCHOR' }} }
) }}

SELECT
    *
FROM
    {{ source(
        'anchor',
        'gov_vote'
    ) }}
